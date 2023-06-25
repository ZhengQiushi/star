//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "core/Manager.h"
#include "core/Partitioner.h"
#include "protocol/Aria/Aria.h"
#include "protocol/Aria/AriaExecutor.h"
#include "protocol/Aria/AriaHelper.h"
#include "protocol/Aria/AriaTransaction.h"
#include "protocol/Aria/AriaMeta.h"

#include <atomic>
#include <thread>
#include <vector>

namespace star {

template <class Workload> class AriaManager : public star::Manager {
public:
  using base_type = star::Manager;

  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = AriaTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  AriaManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
              const ContextType &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag), db(db), epoch(0),
        schedule_meta(context.coordinator_num, context.batch_size),
        txn_meta(context.coordinator_num, context.batch_size) {

    storages.resize(context.batch_size * 5);
    // transactions.resize(context.batch_size);
  }

  void coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num + 1;

    while (!stopFlag.load()) {

      // the coordinator on each machine first moves the aborted transactions
      // from the last batch earlier to the next batch and set remaining
      // transaction slots to null.

      // then, each worker threads generates a transaction using the same seed.
      // LOG(INFO) << "Seed: " << random.get_seed();
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::Aria_READ);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      // wait for all machines until they finish the Aria_READ phase.
      wait4_ack();

      // Allow each worker to commit transactions
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::Aria_COMMIT);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      
      epoch.fetch_add(1);
      cleanup_batch();

      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      // wait for all machines until they finish the Aria_COMMIT phase.
      wait4_ack();
    }

    signal_worker(ExecutorStatus::EXIT);
  }

  void non_coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num + 1;

    for (;;) {
      // LOG(INFO) << "Seed: " << random.get_seed();
      LOG(INFO) << "wait4_signal";
      ExecutorStatus status = wait4_signal();
      if (status == ExecutorStatus::EXIT) {
        set_worker_status(ExecutorStatus::EXIT);
        break;
      }

      DCHECK(status == ExecutorStatus::Aria_READ);
      // the coordinator on each machine first moves the aborted transactions
      // from the last batch earlier to the next batch and set remaining
      // transaction slots to null.

      n_started_workers.store(0);
      n_completed_workers.store(0);
      LOG(INFO) << "Aria_READ";
      set_worker_status(ExecutorStatus::Aria_READ);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      LOG(INFO) << "broadcast_stop";
      wait4_stop(n_coordinators - 1);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      send_ack();
      LOG(INFO) << "send_ack";

      status = wait4_signal();
      LOG(INFO) << "Aria_COMMIT";
      DCHECK(status == ExecutorStatus::Aria_COMMIT);
      n_started_workers.store(0);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::Aria_COMMIT);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      LOG(INFO) << "broadcast_stop";
      wait4_stop(n_coordinators - 1);

      epoch.fetch_add(1);
      cleanup_batch();

      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      send_ack();
      LOG(INFO) << "send_ack";
    }
  }

  void cleanup_batch() {
    std::size_t it = 0;
    auto& transactions = txn_meta.s_transactions_queue;
    auto& storages     = txn_meta.storages;
    for (auto i = 0u; i < transactions.size(); i++) {
      if (transactions[i] == nullptr) {
        continue;;
      }
      if (transactions[i]->abort_lock) {
        transactions[it].swap(transactions[i]);
        // storages[it] = ;

        // for(int j = 0 ; j < transactions[it]->readSet.size(); j ++ ){
        //   LOG(INFO) << it << " <- " << i << " " << *(int*)transactions[it]->readSet[j].get_key();
        // }
        
        transactions[it]->set_id(it);
        it += 1;
      }
    }
    LOG(INFO) << "cleanup_batch : " <<it;

    total_abort.store(it);
  }

public:
  RandomType random;
  DatabaseType &db;
  std::atomic<uint32_t> epoch;
  std::vector<StorageType> storages;
  // std::vector<std::unique_ptr<TransactionType>> transactions;
  std::vector<std::shared_ptr<simpleTransaction>> txns;
  std::atomic<uint32_t> total_abort;
public:
  aria::ScheduleMeta schedule_meta;
  aria::TransactionMeta<WorkloadType> txn_meta;
};
} // namespace star