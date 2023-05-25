//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include <chrono>

#include "common/Percentile.h"
#include "core/Manager.h"
#include "common/ShareQueue.h"
#include <mutex>

namespace star {

#define MAX_COORDINATOR_NUM 20

struct ScheduleMeta {
  ScheduleMeta(int coordinator_num, int batch_size){
    this->coordinator_num = coordinator_num;
    this->batch_size = 2 * batch_size * coordinator_num;
    for(size_t i = 0 ; i < coordinator_num; i ++ ){
      node_busy[i] = 0;
    }
    node_txns.resize(this->batch_size);
    
    txns_coord_cost.resize(this->batch_size, std::vector<int>(coordinator_num, 0));
    txn_id.store(0);
    reorder_done.store(false);

    start_schedule.store(0);
    done_schedule.store(0);
  }
  void clear(){
    for(size_t i = 0 ; i < coordinator_num; i ++ ){
      node_busy[i] = 0;
    }
    node_txns.resize(this->batch_size);
    txn_id.store(0);
    reorder_done.store(false);
    LOG(INFO) << " CLEAR !!!! " << txn_id.load();

    start_schedule.store(0);
    done_schedule.store(0);
  }
  int coordinator_num;
  int batch_size;
  
  std::mutex l;
  std::vector<std::shared_ptr<simpleTransaction>> node_txns;
  std::unordered_map<size_t, int> node_busy;
  std::vector<std::vector<int>> txns_coord_cost;

  std::atomic<uint32_t> txn_id;
  std::atomic<uint32_t> reorder_done;
  ShareQueue<uint32_t> send_txn_id;

  std::atomic<uint32_t> start_schedule;
  std::atomic<uint32_t> done_schedule;
};


template <class Workload> 
struct TransactionMeta {
  using TransactionType = LionTransaction;
  using WorkloadType = Workload;
  using StorageType = typename WorkloadType::StorageType;
  TransactionMeta(int coordinator_num, int batch_size){
    this->batch_size = batch_size;
    this->coordinator_num = coordinator_num;
    storages.resize(batch_size * coordinator_num * 2);
  }
  void clear(){
    s_txn_id.store(0);
    c_txn_id.store(0);
  }

  ShareQueue<simpleTransaction> router_transactions_queue;
  
  std::atomic<uint32_t> s_txn_id;
  std::atomic<uint32_t> c_txn_id;

  std::vector<std::unique_ptr<TransactionType>> s_transactions_queue;
  std::vector<std::unique_ptr<TransactionType>> c_transactions_queue;

  std::vector<StorageType> storages;
  
  ShareQueue<int> s_txn_id_queue;
  ShareQueue<int> c_txn_id_queue;

  std::mutex s_l;
  std::mutex c_l;

  int batch_size;
  int coordinator_num;
};


template <class Workload>
class LionManager : public star::Manager {
public:
  using base_type = star::Manager;
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;
  using TransactionType = LionTransaction;
  

  LionManager(std::size_t coordinator_id, std::size_t id,
              const Context &context, 
              std::atomic<bool> &stopFlag, 
              DatabaseType& db)
      : base_type(coordinator_id, id, context, stopFlag),
        db(db),
        c_partitioner(std::make_unique<StarCPartitioner>(
            coordinator_id, context.coordinator_num)),
        schedule_meta(context.coordinator_num, context.batch_size),
        txn_meta(context.coordinator_num, context.batch_size) {

    batch_size = context.batch_size;
    recorder_status.store(static_cast<int32_t>(ExecutorStatus::STOP));
    transmit_status.store(static_cast<int32_t>(ExecutorStatus::STOP));
    
    // node_txns.resize(MAX_COORDINATOR_NUM);
    // node_busy_.resize(MAX_COORDINATOR_NUM);
    // txns_coord_cost.resize(MAX_COORDINATOR_NUM);
    
    
    transactions_prepared.store(false);
    cur_real_distributed_cnt.store(0);
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
    DCHECK(coordinator_id == context.coordinator_num);
    std::tuple<uint32_t, ExecutorStatus> split = split_signal(status);
    set_worker_status(std::get<1>(split));

    // signal to everyone
    for (auto i = 0u; i <= context.coordinator_num; i++) {
      if (i == coordinator_id) {
        continue;
      }
      LOG(INFO) << " new_signal_message from " << coordinator_id << " -> " << i;
      ControlMessageFactory::new_signal_message(*messages[i],
                                                static_cast<uint32_t>(status));
    }
    flush_messages();
  }

  bool wait4_ack_time() {

    std::chrono::steady_clock::time_point start;

    // only coordinator waits for ack
    DCHECK(coordinator_id == context.coordinator_num);

    std::size_t n_coordinators = context.coordinator_num;

    for (auto i = 0u; i <= n_coordinators - 1; i++) {

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
    std::size_t n_coordinators = context.coordinator_num + 1;

    Percentile<int64_t> all_percentile, c_percentile, s_percentile,
        batch_size_percentile;

    while (!stopFlag.load()) {

      int64_t ack_wait_time_c = 0, ack_wait_time_s = 0;
      auto c_start = std::chrono::steady_clock::now();
      // start c-phase
      VLOG(DEBUG_V) << "start C-Phase";

      n_completed_workers.store(0);
      n_started_workers.store(0);
      batch_size_percentile.add(batch_size);
      signal_worker(merge_value_to_signal(batch_size, ExecutorStatus::C_PHASE));
      
      VLOG(DEBUG_V) << "wait_all_workers_start";

      wait_all_workers_start();

      VLOG(DEBUG_V) << "wait_all_workers_finish";
      
      wait_all_workers_finish();
      set_worker_status(ExecutorStatus::STOP);
      broadcast_stop();

      VLOG(DEBUG_V) << "wait_ack c-phase";

      wait4_ack();

      {
        auto now = std::chrono::steady_clock::now();
        c_percentile.add(
            std::chrono::duration_cast<std::chrono::microseconds>(now - c_start)
                .count());
      }

      // cur_data_transform_num ++;

      auto s_start = std::chrono::steady_clock::now();
      // start s-phase

      VLOG(DEBUG_V) << "start S-Phase";

      n_completed_workers.store(0);
      n_started_workers.store(0);
      
      signal_worker(merge_value_to_signal(skip_s_phase.load(), ExecutorStatus::S_PHASE));

      if(skip_s_phase.load() == false){
        wait_all_workers_start();
        
        VLOG(DEBUG_V) << "wait_all_workers_finish";

        wait_all_workers_finish();
        
        VLOG(DEBUG_V) << "wait_all_workers_finish";

        broadcast_stop();
        wait4_stop(n_coordinators - 1);

        VLOG(DEBUG_V) << "wait_all_workers_finish";

        n_completed_workers.store(0);
        set_worker_status(ExecutorStatus::STOP);
        wait_all_workers_finish();
        
        VLOG(DEBUG_V) << "wait4_ack s-phase";

        wait4_ack();
      } else {
        LOG(INFO) << "skip s phase";
        wait_all_workers_finish();
        
        VLOG(DEBUG_V) << "wait4_ack s-phase";

        wait4_ack();
      }

      VLOG(DEBUG_V) << "finished";
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
    std::size_t n_coordinators = context.coordinator_num + 1;

    for (;;) {

      ExecutorStatus signal;
      std::tie(batch_size, signal) = split_signal(wait4_signal());

      if (signal == ExecutorStatus::EXIT) {
        set_worker_status(ExecutorStatus::EXIT);
        break;
      }

      VLOG(DEBUG_V) << "start C-Phase";

      // start c-phase

      DCHECK(signal == ExecutorStatus::C_PHASE);
      n_completed_workers.store(0);
      n_started_workers.store(0);
      set_worker_status(ExecutorStatus::C_PHASE);
      
      VLOG(DEBUG_V) << "wait_all_workers_start";

      wait_all_workers_start();
      
      VLOG(DEBUG_V) << "wait4_stop";

      wait4_stop(1);
      
      
      set_worker_status(ExecutorStatus::STOP);
      
      VLOG(DEBUG_V) << "wait_all_workers_finish";

      wait_all_workers_finish();

      VLOG(DEBUG_V) << "send_ack c-phase";

      send_ack();


      VLOG(DEBUG_V) << "start S-Phase";
      
      // start s-phase
      bool is_s_phase_skip = false;
      std::tie(is_s_phase_skip, signal) = split_signal(wait4_signal());

      skip_s_phase.store(is_s_phase_skip);
      
      DCHECK(signal == ExecutorStatus::S_PHASE);
      n_completed_workers.store(0);
      n_started_workers.store(0);

      set_worker_status(ExecutorStatus::S_PHASE);

      if(skip_s_phase.load() == false){
        VLOG(DEBUG_V) << "wait_all_workers_start";
        wait_all_workers_start();
        VLOG(DEBUG_V) << "wait_all_workers_finish";
        wait_all_workers_finish();
        broadcast_stop();
        VLOG(DEBUG_V) << "wait4_stop";
        wait4_stop(n_coordinators - 1);
        // n_completed_workers.store(0);
        set_worker_status(ExecutorStatus::STOP);
        VLOG(DEBUG_V) << "wait_all_workers_finish";
        wait_all_workers_finish();
        VLOG(DEBUG_V) << "send_ack s-phase";

        send_ack();
      } else {
        LOG(INFO) << "skip s phase";
        LOG(INFO) << "wait_all_workers_finish";
        wait_all_workers_finish();
        LOG(INFO) << "send_ack";
        send_ack();
      }

      VLOG(DEBUG_V) << "finished";

    }
  }

public:
  uint32_t batch_size;
  DatabaseType& db;
  std::atomic<uint32_t> recorder_status;
  std::atomic<uint32_t> transmit_status;
  std::unique_ptr<Partitioner> c_partitioner;

  std::atomic<uint32_t> is_full_signal;
  std::atomic<uint32_t> schedule_done;

  std::atomic<uint32_t> skip_s_phase;

  ShareQueue<simpleTransaction*, 40960> transactions_queue;

  std::atomic<uint32_t> transactions_prepared; 
  std::atomic<uint32_t> cur_real_distributed_cnt;

  
  // std::vector<std::unique_ptr<TransactionType>> s_transactions_queue; 
  // std::vector<std::unique_ptr<TransactionType>> c_transactions_queue; 

  // std::mutex s_l;
  // std::mutex c_l;

  // ShareQueue<simpleTransaction> router_transactions_queue;

  // ShareQueue<int> s_txn_id_queue;
  // ShareQueue<int> c_txn_id_queue;

  // std::vector<StorageType> storages;

  ScheduleMeta schedule_meta;
  TransactionMeta<WorkloadType> txn_meta;

};
} // namespace star