//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include <chrono>

#include "common/Percentile.h"
#include "core/Manager.h"

namespace star {


template <class Workload>
class StarManager : public star::Manager {
public:
  using base_type = star::Manager;
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;

  StarManager(std::size_t coordinator_id, std::size_t id,
              const Context &context, 
              std::atomic<bool> &stopFlag, 
              DatabaseType& db)
      : base_type(coordinator_id, id, context, stopFlag),
        db(db),
        c_partitioner(std::make_unique<StarCPartitioner>(
            coordinator_id, context.coordinator_num)) {

    batch_size = context.batch_size;
    recorder_status.store(static_cast<int32_t>(ExecutorStatus::STOP));
    transmit_status.store(static_cast<int32_t>(ExecutorStatus::STOP));
  }

  ExecutorStatus merge_value_to_signal(uint32_t value, ExecutorStatus signal) {
    // the value is put into the most significant 24 bits
    uint32_t offset = 8;
    return static_cast<ExecutorStatus>((value << offset) |
                                       static_cast<uint32_t>(signal));
  }

  std::tuple<uint32_t, ExecutorStatus> split_signal(ExecutorStatus signal) {
    // the value is put into the most significant 24 bits
    uint32_t offset = 8, mask = 0xff;
    uint32_t value = static_cast<uint32_t>(signal);
    // return value and ``real" signal
    return std::make_tuple(value >> offset,
                           static_cast<ExecutorStatus>(value & mask));
  }

  void update_batch_size(uint64_t running_time) {
    // running_time in microseconds
    // context.group_time in ms
    batch_size = batch_size * (context.group_time * 1000) / running_time;

    if (batch_size % 10 != 0) {
      batch_size += (10 - batch_size % 10);
    }
  }

  void signal_worker(ExecutorStatus status) {

    // only the coordinator node calls this function
    DCHECK(coordinator_id == 0);
    std::tuple<uint32_t, ExecutorStatus> split = split_signal(status);
    set_worker_status(std::get<1>(split));

    // signal to everyone
    for (auto i = 0u; i < context.coordinator_num; i++) {
      if (i == coordinator_id) {
        continue;
      }
      ControlMessageFactory::new_signal_message(*messages[i],
                                                static_cast<uint32_t>(status));
    }
    flush_messages();
  }

  bool wait4_ack_time() {

    std::chrono::steady_clock::time_point start;

    // only coordinator waits for ack
    DCHECK(coordinator_id == 0);

    std::size_t n_coordinators = context.coordinator_num;

    for (auto i = 0u; i < n_coordinators - 1; i++) {

      ack_in_queue.wait_till_non_empty();

      std::unique_ptr<Message> message(ack_in_queue.front());
      bool ok = ack_in_queue.pop();
      CHECK(ok);

      CHECK(message->get_message_count() == 1);

      MessagePiece messagePiece = *(message->begin());
      auto type = static_cast<ControlMessage>(messagePiece.get_message_type());
      CHECK(type == ControlMessage::ACK);
    }

    return true;
  }

  void set_record_worker_status(ExecutorStatus status) {
    recorder_status.store(static_cast<uint32_t>(status));
  }
  
  void set_record_worker_transmit_status(ExecutorStatus status) {
    transmit_status.store(static_cast<uint32_t>(status));
  }
  void wait_recorder_worker_finish(){
    /**
     * @brief 
     * 
     */
    while(recorder_status.load() != static_cast<int32_t>(ExecutorStatus::STOP)){
      std::this_thread::yield();
    }
  }


  void coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    Percentile<int64_t> all_percentile, c_percentile, s_percentile,
        batch_size_percentile;

    while (!stopFlag.load()) {

      int64_t ack_wait_time_c = 0, ack_wait_time_s = 0;
      auto c_start = std::chrono::steady_clock::now();
      // start c-phase
      LOG(INFO) << "start C-Phase";

      n_completed_workers.store(0);
      n_started_workers.store(0);
      batch_size_percentile.add(batch_size);
      signal_worker(merge_value_to_signal(batch_size, ExecutorStatus::C_PHASE));
      
      LOG(INFO) << "wait_all_workers_start";

      wait_all_workers_start();

      LOG(INFO) << "wait_all_workers_finish";
      
      wait_all_workers_finish();
      set_worker_status(ExecutorStatus::STOP);
      broadcast_stop();

      LOG(INFO) << "wait_ack";

      wait4_ack();

      {
        auto now = std::chrono::steady_clock::now();
        c_percentile.add(
            std::chrono::duration_cast<std::chrono::microseconds>(now - c_start)
                .count());
      }

      // start data-transform 
      static int cur_data_transform_num = 0;
      if (context.enable_data_transfer == true && 
          cur_data_transform_num % context.data_transform_interval == context.data_transform_interval - 1 && 
          (cur_data_transform_num / context.data_transform_interval) % 2 == 0) {
        // 开始操作 
        set_record_worker_status(ExecutorStatus::START);
        LOG(INFO) << "wait_recorder_worker_finish";
        // wait_recorder_worker_finish();
      }
      if (context.enable_data_transfer == true && 
          cur_data_transform_num % context.data_transform_interval == context.data_transform_interval - 1 && 
          (cur_data_transform_num / context.data_transform_interval) % 2 == 1) {
        // 开始操作 
        set_record_worker_transmit_status(ExecutorStatus::START);
        LOG(INFO) << "set_record_worker_transmit_status";
        wait_recorder_worker_finish();
        LOG(INFO) << "set_record_worker_transmit_status_finish";

      }
      
      //// for debug 
      if(WorkloadType::which_workload == myTestSet::YCSB){
        for(int i = 0 ; i < 12; i ++ ){
          ITable *dest_table = db.find_table(ycsb::ycsb::tableID, i);
          LOG(INFO) << "TABLE [" << i << "]: " << dest_table->table_record_num();
        }
      }

      cur_data_transform_num ++;

      auto s_start = std::chrono::steady_clock::now();
      // start s-phase

      LOG(INFO) << "start S-Phase";

      n_completed_workers.store(0);
      n_started_workers.store(0);
      signal_worker(ExecutorStatus::S_PHASE);
      wait_all_workers_start();
      
      // LOG(INFO) << "wait_all_workers_finish";

      wait_all_workers_finish();
      
      // LOG(INFO) << "wait_all_workers_finish";

      broadcast_stop();
      wait4_stop(n_coordinators - 1);

      // LOG(INFO) << "wait_all_workers_finish";

      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      
      // LOG(INFO) << "wait4_ack";

      wait4_ack();

      // LOG(INFO) << "finished";
      {
        auto now = std::chrono::steady_clock::now();

        s_percentile.add(
            std::chrono::duration_cast<std::chrono::microseconds>(now - s_start)
                .count());

        auto all_time =
            std::chrono::duration_cast<std::chrono::microseconds>(now - c_start)
                .count();

        all_percentile.add(all_time);
        // if (context.star_dynamic_batch_size) {
        //   update_batch_size(all_time);
        // }
      }
    }

    signal_worker(ExecutorStatus::EXIT);

    LOG(INFO) << "Average phase switch length " << all_percentile.nth(50)
              << " us, average c phase length " << c_percentile.nth(50)
              << " us, average s phase length " << s_percentile.nth(50)
              << " us, average batch size " << batch_size_percentile.nth(50)
              << " .";
  }

  void non_coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    for (;;) {

      ExecutorStatus signal;
      std::tie(batch_size, signal) = split_signal(wait4_signal());

      if (signal == ExecutorStatus::EXIT) {
        set_worker_status(ExecutorStatus::EXIT);
        break;
      }

      LOG(INFO) << "start C-Phase";

      // start c-phase

      DCHECK(signal == ExecutorStatus::C_PHASE);
      n_completed_workers.store(0);
      n_started_workers.store(0);
      set_worker_status(ExecutorStatus::C_PHASE);
      
      LOG(INFO) << "wait_all_workers_start";

      wait_all_workers_start();
      
      LOG(INFO) << "wait4_stop";

      wait4_stop(1);
      
      
      set_worker_status(ExecutorStatus::STOP);
      
      LOG(INFO) << "wait_all_workers_finish";

      wait_all_workers_finish();

      LOG(INFO) << "send_ack";

      send_ack();
      if(WorkloadType::which_workload == myTestSet::YCSB){
        for(int i = 0 ; i < 12; i ++ ){
          if(c_partitioner->is_partition_replicated_on(i, coordinator_id)) {
            ITable *dest_table = db.find_table(ycsb::ycsb::tableID, i);
            LOG(INFO) << "TABLE [" << i << "]: " << dest_table->table_record_num();
          }
        }
      }

      LOG(INFO) << "start S-Phase";
      
      // start s-phase

      signal = wait4_signal();
      DCHECK(signal == ExecutorStatus::S_PHASE);
      n_completed_workers.store(0);
      n_started_workers.store(0);
      set_worker_status(ExecutorStatus::S_PHASE);
      // LOG(INFO) << "wait_all_workers_start";
      wait_all_workers_start();
      // LOG(INFO) << "wait_all_workers_finish";
      wait_all_workers_finish();
      broadcast_stop();
      LOG(INFO) << "wait4_stop";
      wait4_stop(n_coordinators - 1);
      // n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      // LOG(INFO) << "wait_all_workers_finish";
      wait_all_workers_finish();
      // LOG(INFO) << "send_ack";

      send_ack();
      // LOG(INFO) << "finished";

    }
  }

public:
  uint32_t batch_size;
  DatabaseType& db;
  std::atomic<uint32_t> recorder_status;
  std::atomic<uint32_t> transmit_status;
  std::unique_ptr<Partitioner> c_partitioner;
};
} // namespace star