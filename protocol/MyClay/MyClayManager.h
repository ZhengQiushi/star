//
// Created by Yi Lu on 9/10/18.
//

#pragma once

#include "core/Manager.h"
#include "common/ShareQueue.h"

namespace star {
namespace clay {


#define MAX_COORDINATOR_NUM 20

struct ScheduleMeta {
  ScheduleMeta(int coordinator_num, int batch_size){
    this->coordinator_num = coordinator_num;
    this->batch_size = 2 * batch_size * coordinator_num;
    for(int i = 0 ; i < coordinator_num; i ++ ){
      node_busy[i] = 0;
    }
    node_txns.resize(this->batch_size);
    
    txns_coord_cost.resize(this->batch_size, std::vector<int>(coordinator_num, 0));
    txn_id.store(0);
    reorder_done.store(false);

    start_schedule.store(0);
    done_schedule.store(0);
    all_done_schedule.store(0);
  }
  void clear(){
    for(int i = 0 ; i < coordinator_num; i ++ ){
      node_busy[i] = 0;
    }
    node_txns.resize(this->batch_size);
    txn_id.store(0);
    reorder_done.store(false);
    LOG(INFO) << " CLEAR !!!! " << txn_id.load();

    start_schedule.store(0);
    done_schedule.store(0);
    all_done_schedule.store(0);
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

  std::atomic<uint32_t> all_done_schedule;
};

template <class Workload> 
struct TransactionMeta {
  using TransactionType = MyClayTransaction;
  using WorkloadType = Workload;
  using StorageType = typename WorkloadType::StorageType;
  TransactionMeta(int coordinator_num, int batch_size){
    this->batch_size = batch_size;
    this->coordinator_num = coordinator_num;
    c_storages.resize(batch_size * coordinator_num * 2);
    t_storages.resize(batch_size * coordinator_num * 2);
  }
  void clear(){
    t_txn_id.store(0);
    c_txn_id.store(0);
    t_transactions_queue.clear();
    c_transactions_queue.clear();
  }

  ShareQueue<simpleTransaction> router_transactions_queue;
  
  std::atomic<uint32_t> t_txn_id;
  std::atomic<uint32_t> c_txn_id;

  std::vector<std::unique_ptr<TransactionType>> t_transactions_queue;
  std::vector<std::unique_ptr<TransactionType>> c_transactions_queue;

  
  std::vector<StorageType> t_storages;
  std::vector<StorageType> c_storages;
  
  ShareQueue<int> t_txn_id_queue;
  ShareQueue<int> c_txn_id_queue;

  std::mutex t_l;
  std::mutex c_l;

  int batch_size;
  int coordinator_num;
};


template <class Workload>
class MyClayManager: public star::Manager {
public:
  using base_type = star::Manager;
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;
  using TransactionType = SiloTransaction;

  std::atomic<uint32_t> transactions_prepared; 
  std::vector<std::unique_ptr<TransactionType>> r_transactions_queue;
  ShareQueue<int> txn_id_queue;
  std::vector<StorageType> storages;
  
  // std::vector<std::unique_ptr<TransactionType>> c_transactions_queue;

  ShareQueue<simpleTransaction*, 54096> transactions_queue;
  std::atomic<uint32_t> is_full_signal;
  
  ScheduleMeta schedule_meta;
  TransactionMeta<WorkloadType> txn_meta;

  MyClayManager(std::size_t coordinator_id, std::size_t id, const Context &context,
          std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag),
        schedule_meta(context.coordinator_num, context.batch_size),
        txn_meta(context.coordinator_num, context.batch_size) {
        is_full_signal.store(false);
        transactions_prepared.store(false);
        storages.resize(context.batch_size * 4);
      }

  void coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num + 1;

    while (!stopFlag.load()) {

      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::START);
      LOG(INFO) << "signal start, wait start";
      wait_all_workers_start();
      // std::this_thread::sleep_for(
      //     std::chrono::milliseconds(context.group_time));
      // set_worker_status(ExecutorStatus::STOP);
      LOG(INFO) << "wait finish";
      wait_all_workers_finish();
      
      broadcast_stop();
      LOG(INFO) << "broadcast stop, wait stop";
      wait4_stop(n_coordinators - 1);
      // process replication
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::CLEANUP);
      LOG(INFO) << "set_worker_status, wait_all_workers_finish";
      wait_all_workers_finish();
      LOG(INFO) << "all_workers_finish, wait 4 ack";
      wait4_ack();
    }

    signal_worker(ExecutorStatus::EXIT);
  }

  void non_coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num + 1;

    for (;;) {

      LOG(INFO) << "wait 4 signal";
      ExecutorStatus status = wait4_signal();
      if (status == ExecutorStatus::EXIT) {
        set_worker_status(ExecutorStatus::EXIT);
        break;
      }

      DCHECK(status == ExecutorStatus::START);
      n_completed_workers.store(0);
      n_started_workers.store(0);
      set_worker_status(ExecutorStatus::START);
      LOG(INFO) << "wait_all_workers_start";
      wait_all_workers_start();
      LOG(INFO) << "start, wait 4 stop";
      wait4_stop(1);
      set_worker_status(ExecutorStatus::STOP);
      LOG(INFO) << "wait_all_workers_finish";
      wait_all_workers_finish();
      
      broadcast_stop();
      LOG(INFO) << "broadcast_stop, wait4_stop";
      wait4_stop(n_coordinators - 2);
      
      // process replication
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::CLEANUP);
      LOG(INFO) << "wait_all_workers_finish";
      wait_all_workers_finish();
      LOG(INFO) << "send_ack";
      send_ack();
    }
  }
};

} // namespace clay
} // namespace star
