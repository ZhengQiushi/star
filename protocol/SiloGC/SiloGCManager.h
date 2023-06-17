//
// Created by Yi Lu on 9/10/18.
//

#pragma once

#include <chrono>

#include "common/Percentile.h"
#include "core/Manager.h"


namespace star {
namespace silo {

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
  using TransactionType = SiloTransaction;
  using WorkloadType = Workload;
  using StorageType = typename WorkloadType::StorageType;
  TransactionMeta(int coordinator_num, int batch_size){
    this->batch_size = batch_size;
    this->coordinator_num = coordinator_num;
    storages.resize(batch_size * coordinator_num * 2);
    transactions_prepared.store(0);
  }
  void clear(){
    s_txn_id.store(0);
    c_txn_id.store(0);
    s_transactions_queue.clear();
    c_transactions_queue.clear();
    transactions_prepared.store(0);
  }

  std::atomic<uint32_t> transactions_prepared;

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
class SiloGCManager : public star::Manager {
public:
  using base_type = star::Manager;
  using WorkloadType = Workload;
  // using DatabaseType = typename WorkloadType::DatabaseType;

  ShareQueue<simpleTransaction*, 54096> transactions_queue;
  std::atomic<uint32_t> is_full_signal;

  SiloGCManager(std::size_t coordinator_id, std::size_t id, const Context &context,
          std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag),
        schedule_meta(context.coordinator_num, context.batch_size),
        txn_meta(context.coordinator_num, context.batch_size) {
        is_full_signal.store(false);
      }

  void coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num + 1;

    while (!stopFlag.load()) {

      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::START);
      wait_all_workers_start();
      // std::this_thread::sleep_for(
      //     std::chrono::milliseconds(context.group_time));
      // set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      // process replication
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::CLEANUP);
      wait_all_workers_finish();
      wait4_ack();
    }

    signal_worker(ExecutorStatus::EXIT);
  }

  void non_coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num + 1;

    for (;;) {

      ExecutorStatus status = wait4_signal();
      if (status == ExecutorStatus::EXIT) {
        set_worker_status(ExecutorStatus::EXIT);
        break;
      }

      DCHECK(status == ExecutorStatus::START);
      n_completed_workers.store(0);
      n_started_workers.store(0);
      set_worker_status(ExecutorStatus::START);
      wait_all_workers_start();
      wait4_stop(1);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 2);
      // process replication
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::CLEANUP);
      wait_all_workers_finish();
      send_ack();
    }
  }
public:
  ScheduleMeta schedule_meta;
  TransactionMeta<WorkloadType> txn_meta;
};

} // namespace normal
} // namespace star
