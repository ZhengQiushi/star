//
// Created by Yi Lu on 9/10/18.
//

#pragma once

#include "core/Manager.h"
#include "common/ShareQueue.h"

namespace star {
namespace group_commit {

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
  ShareQueue<int, 10086> txn_id_queue;
  std::vector<StorageType> storages;
  
  // std::vector<std::unique_ptr<TransactionType>> c_transactions_queue;

  ShareQueue<simpleTransaction*, 54096> transactions_queue;
  std::atomic<uint32_t> is_full_signal;

  MyClayManager(std::size_t coordinator_id, std::size_t id, const Context &context,
          std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag) {
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

} // namespace group_commit
} // namespace star
