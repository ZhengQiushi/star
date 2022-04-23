//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include <chrono>

#include "common/Percentile.h"
#include "core/Manager.h"

namespace star {


template <class Workload>
class LionNSManager : public star::Manager {
public:
  using base_type = star::Manager;
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;

  LionNSManager(std::size_t coordinator_id, std::size_t id,
              const Context &context, 
              std::atomic<bool> &stopFlag, 
              DatabaseType& db)
      : base_type(coordinator_id, id, context, stopFlag),
        db(db),
        l_partitioner(std::make_unique<LionDynamicPartitioner<WorkloadType> >(
            context.coordinator_id, context.coordinator_num, db)) {

    batch_size = context.batch_size;
    coordinators_load.resize(context.coordinator_num);
    recorder_status.store(static_cast<int32_t>(ExecutorStatus::STOP));
    transmit_status.store(static_cast<int32_t>(ExecutorStatus::STOP));
  }



  void update_batch_size(uint64_t running_time) {
    // running_time in microseconds
    // context.group_time in ms
    batch_size = batch_size * (context.group_time * 1000) / running_time;

    if (batch_size % 10 != 0) {
      batch_size += (10 - batch_size % 10);
    }
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

  void send_lion_ack(size_t lion_king_coordinator_id) {
    // only non-coordinator calls this function
    DCHECK(coordinator_id != lion_king_coordinator_id);
    ControlMessageFactory::new_ack_message(*messages[lion_king_coordinator_id]);
    flush_messages();
  }

  void wait4_lion_ack(size_t lion_king_coordinator_id) {

    std::chrono::steady_clock::time_point start;

    // only coordinator waits for ack
    DCHECK(coordinator_id == lion_king_coordinator_id);

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
  }

  void signal_worker(ExecutorStatus status) {

    // only the coordinator node calls this function
    // DCHECK(coordinator_id == lion_king_coordinator_id);
    set_worker_status(status);

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
  
  bool lion_action_start(ExecutorStatus status, size_t lion_king_coordinator_id){

      std::size_t n_coordinators = context.coordinator_num;
      if(lion_king_coordinator_id == coordinator_id){
        // current node is the true-coordinator 
        int64_t ack_wait_time_c = 0, ack_wait_time_s = 0;
        auto c_start = std::chrono::steady_clock::now();

        // start c-phase
        VLOG(DEBUG_V) << "start C-Phase: C" << coordinator_id << " is the king";

        batch_size_percentile.add(batch_size);
        VLOG(DEBUG_V) << "wait_all_workers_start";

        wait_all_workers_start();

        VLOG(DEBUG_V) << "wait_all_workers_finish";
        
        wait_all_workers_finish();
        set_worker_status(ExecutorStatus::STOP);
        broadcast_stop();

        VLOG(DEBUG_V) << "wait_ack";

        wait4_lion_ack(lion_king_coordinator_id);

        VLOG(DEBUG_V) << "finished";
        {
          auto now = std::chrono::steady_clock::now();

          auto all_time =
              std::chrono::duration_cast<std::chrono::microseconds>(now - c_start)
                  .count();

          all_percentile.add(all_time);
          // if (context.star_dynamic_batch_size) {
          //   update_batch_size(all_time);
          // }
        }

      } else {
        // ExecutorStatus signal = st;
        VLOG(DEBUG_V) << "start C-Phase: C" << coordinator_id << " is normal";

        ExecutorStatus signal = status; // wait4_signal();

        if (signal == ExecutorStatus::EXIT) {
          set_worker_status(ExecutorStatus::EXIT);
          return true;
        }

        VLOG(DEBUG_V) << "start C-Phase";

        // start c-phase

        DCHECK(signal == ExecutorStatus::C_PHASE);
        
        set_worker_status(merge_value_to_signal(lion_king_coordinator_id, ExecutorStatus::C_PHASE));
        
        VLOG(DEBUG_V) << "wait_all_workers_start";

        wait_all_workers_start();
        
        VLOG(DEBUG_V) << "wait4_stop";

        wait4_stop(1);
        
        VLOG(DEBUG_V) << "getStop";
        
        set_worker_status(ExecutorStatus::STOP);
        
        VLOG(DEBUG_V) << "wait_all_workers_finish";

        wait_all_workers_finish();

        VLOG(DEBUG_V) << "send_ack";

        send_lion_ack(lion_king_coordinator_id);

        VLOG(DEBUG_V) << "finished";

      }
    
    
        if(WorkloadType::which_workload == myTestSet::YCSB){
          for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
            // if(l_partitioner->is_partition_replicated_on(ycsb::tableId, i, coordinator_id)) {
              ITable *dest_table = db.find_router_table(ycsb::ycsb::tableID, i);
              VLOG(DEBUG_V) << "C[" << i << "]: " << dest_table->table_record_num();
            // }
          }
        } else {
            for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
            // if(l_partitioner->is_partition_replicated_on(ycsb::tableId, i, coordinator_id)) {
              ITable *dest_table = db.find_router_table(tpcc::stock::tableID, i);
              VLOG(DEBUG_V) << "C[" << i << "]: " << dest_table->table_record_num();
            // }
          }
        }

        if(WorkloadType::which_workload == myTestSet::YCSB){
          for(size_t i = 0 ; i < context.partition_num; i ++ ){
            // if(l_partitioner->is_partition_replicated_on(ycsb::tableId, i, coordinator_id)) {
              ITable *dest_table = db.find_table(ycsb::ycsb::tableID, i);
              VLOG(DEBUG_V) << "P[" << i << "]: " << dest_table->table_record_num();
            // }
          }
        } else {
            for(size_t i = 0 ; i < context.partition_num; i ++ ){
            // if(l_partitioner->is_partition_replicated_on(ycsb::tableId, i, coordinator_id)) {
              
              ITable *dest_table = db.find_table(tpcc::stock::tableID, i);
              VLOG(DEBUG_V) << "P[" << i << "]: " << dest_table->table_record_num();
            // }
          }
        }
    
    return false;
  }
  
  void coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;



    while (!stopFlag.load()) {

      static size_t lion_king_coordinator_id = -1;
      lion_king_coordinator_id = (lion_king_coordinator_id + 1) % n_coordinators;
      
      n_completed_workers.store(0);
      n_started_workers.store(0);

      signal_worker(merge_value_to_signal(lion_king_coordinator_id, ExecutorStatus::C_PHASE));
      // signal_lion_worker(lion_king_coordinator_id);

      ExecutorStatus status = ExecutorStatus::C_PHASE; // static_cast<ExecutorStatus>(worker_status.load());
      bool is_exit = lion_action_start(status, lion_king_coordinator_id);
      if(is_exit){
        break;
      }
    }

    signal_worker(ExecutorStatus::EXIT);

    LOG(INFO) << "Average phase switch length " << all_percentile.nth(50)
              << " us, average c phase length " << c_percentile.nth(50)
              << " us, average s phase length " << s_percentile.nth(50)
              << " us, average batch size " << batch_size_percentile.nth(50)
              << " .";
  }

  ExecutorStatus wait4_lion_signal() {

    signal_in_queue.wait_till_non_empty();

    std::unique_ptr<Message> message(signal_in_queue.front());
    bool ok = signal_in_queue.pop();
    CHECK(ok);

    CHECK(message->get_message_count() == 1);

    MessagePiece messagePiece = *(message->begin());
    auto type = static_cast<ControlMessage>(messagePiece.get_message_type());
    CHECK(type == ControlMessage::SIGNAL);

    uint32_t status;
    StringPiece stringPiece = messagePiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> status;

    return static_cast<ExecutorStatus>(status);
  }

  void non_coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    for (;;) {
      size_t lion_king_coordinator_id;
      ExecutorStatus signal, status;
      // ExecutorStatus signal = wait4_signal();
      
      n_completed_workers.store(0);
      n_started_workers.store(0);
      
      signal = wait4_lion_signal();
      std::tie(lion_king_coordinator_id, status) =  split_signal(signal);
      set_worker_status(signal);

      if (status == ExecutorStatus::EXIT) {
        break;
      }
      DCHECK(status == ExecutorStatus::C_PHASE);
      
      // if(signal == ExecutorStatus::C_PHASE){
        
      bool is_exit = lion_action_start(status, lion_king_coordinator_id);
      if(is_exit){
        break;
      }
    }
  }
private:
  Percentile<int64_t> all_percentile, c_percentile, s_percentile,
                      batch_size_percentile;
public:
  uint32_t batch_size;
  DatabaseType& db;
  std::atomic<uint32_t> recorder_status;
  std::atomic<uint32_t> transmit_status;
  std::unique_ptr<Partitioner> l_partitioner;
  
  std::vector<int> coordinators_load;
};
} // namespace star