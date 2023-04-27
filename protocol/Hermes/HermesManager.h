//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include "core/Manager.h"
#include "core/Partitioner.h"
#include "protocol/Hermes/Hermes.h"
#include "protocol/Hermes/HermesExecutor.h"
#include "protocol/Hermes/HermesHelper.h"
#include "protocol/Hermes/HermesTransaction.h"

#include <thread>
#include <vector>

namespace star {

template <class Workload> class HermesManager : public star::Manager {
public:
  using base_type = star::Manager;

  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = HermesTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  HermesManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
                const ContextType &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag), db(db),
        partitioner(coordinator_id, context.coordinator_num,
                    HermesHelper::string_to_vint(context.replica_group), db) {

    storages.resize(context.batch_size);
    transactions.resize(context.batch_size);// 由manager统一生成txn
  }

  void coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    while (!stopFlag.load()) {


        // if(WorkloadType::which_workload == myTestSet::YCSB){
        //   int replica_num = partitioner.replica_num();
        //   for(int r = 0 ; r < replica_num; r ++ ){
        //     VLOG(DEBUG_V) << "Replica[" << r << "] : ";
        //     for(size_t i = 0 ; i < context.partition_num; i ++ ){
        //       ITable *dest_table = db.find_table(ycsb::ycsb::tableID, i, r);
        //       VLOG(DEBUG_V) << "   P[" << i << "]: " << dest_table->table_record_num();
        //     }
        //   }
        // } else {
        //     for(size_t i = 0 ; i < context.partition_num; i ++ ){
        //     // if(l_partitioner->is_partition_replicated_on(ycsb::tableId, i, coordinator_id)) {
              
        //       ITable *dest_table = db.find_table(tpcc::stock::tableID, i);
        //       VLOG(DEBUG_V) << "P[" << i << "]: " << dest_table->table_record_num();
        //     // }
        //   }
        // }
      // the coordinator on each machine generates
      // a batch of transactions using the same random seed.

      // VLOG(DEBUG_V4) << "Seed: " << random.get_seed();
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::Analysis);
      VLOG(DEBUG_V4) << "Analysis START";

      // Allow each worker to analyse the read/write set
      // each worker analyse i, i + n, i + 2n transaction
      wait_all_workers_start();
      VLOG(DEBUG_V4) << "wait Analysis done...";

      wait_all_workers_finish();


      // wait for all machines until they finish the analysis phase.
      wait4_ack();

      VLOG(DEBUG_V4) << "Analysis FIN. SEND ACK";
      // for(int i = 0 ; i < (int)transactions.size(); i ++ ){
      //   auto &txn = transactions[i];
      //   auto keys = txn.get()->get_query();
      //   std::string tmp;
      //   for(int j = 0 ; j < (int)keys.size(); j ++ ){
      //     char t[20];
      //     sprintf(t, "%d ", *(int*)& keys[j]);
      //     tmp += t;
      //   }
      //   VLOG(DEBUG_V4) << i << ": " << tmp;
      // }

      // Allow each worker to run transactions
      // DB is partitioned by the number of lock managers.
      // The first k workers act as lock managers to grant locks to other
      // workers The remaining workers run transactions upon assignment via the
      // queue.
      n_started_workers.store(0);
      n_completed_workers.store(0);
      clear_lock_manager_status();
      signal_worker(ExecutorStatus::Execute);
      
      VLOG(DEBUG_V4) << "Execute START";


      wait_all_workers_start();
      VLOG(DEBUG_V4) << "wait Execute done...";

      wait_all_workers_finish();
      // wait for all machines until they finish the execution phase.
      wait4_ack();
    }

    signal_worker(ExecutorStatus::EXIT);
  }

  void non_coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    for (;;) {
        // if(WorkloadType::which_workload == myTestSet::YCSB){
        //   int replica_num = partitioner.replica_num();
        //   for(int r = 0 ; r < replica_num; r ++ ){
        //     VLOG(DEBUG_V8) << "Replica[" << r << "] : ";

        //     for(size_t i = 0 ; i < context.partition_num; i ++ ){
        //       ITable *dest_table = db.find_table(ycsb::ycsb::tableID, i, r);
        //       VLOG(DEBUG_V8) << "P[" << i << "]: " << dest_table->table_record_num();
        //     }
        //   }
        // } else {
        //     for(size_t i = 0 ; i < context.partition_num; i ++ ){
        //     // if(l_partitioner->is_partition_replicated_on(ycsb::tableId, i, coordinator_id)) {
              
        //       ITable *dest_table = db.find_table(tpcc::stock::tableID, i);
        //       VLOG(DEBUG_V8) << "P[" << i << "]: " << dest_table->table_record_num();
        //     // }
        //   }
        // }

      // VLOG(DEBUG_V4) << "Seed: " << random.get_seed();
      ExecutorStatus status = wait4_signal();
      if (status == ExecutorStatus::EXIT) {
        set_worker_status(ExecutorStatus::EXIT);
        break;
      }

      DCHECK(status == ExecutorStatus::Analysis);
      // the coordinator on each machine generates
      // a batch of transactions using the same random seed.
      // Allow each worker to analyse the read/write set
      // each worker analyse i, i + n, i + 2n transaction

      n_started_workers.store(0);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::Analysis);
      VLOG(DEBUG_V4) << "Analysis START";

      wait_all_workers_start();
      VLOG(DEBUG_V4) << "wait Analysis done";

      wait_all_workers_finish();

      send_ack();
      VLOG(DEBUG_V4) << "Analysis FIN. SEND ACK";
      // for(int i = 0 ; i < (int)transactions.size(); i ++ ){
      //   auto &txn = transactions[i];
      //   auto keys = txn.get()->get_query();
      //   std::string tmp;
      //   for(int j = 0 ; j < (int)keys.size(); j ++ ){
      //     char t[20];
      //     sprintf(t, "%d ", *(int*)& keys[j]);
      //     tmp += t;
      //   }
      //   VLOG(DEBUG_V4) << i << ": " << tmp;
      // }
      status = wait4_signal();
      DCHECK(status == ExecutorStatus::Execute);
      // Allow each worker to run transactions
      // DB is partitioned by the number of lock managers.
      // The first k workers act as lock managers to grant locks to other
      // workers The remaining workers run transactions upon assignment via the
      // queue.
      n_started_workers.store(0);
      n_completed_workers.store(0);
      clear_lock_manager_status();
      set_worker_status(ExecutorStatus::Execute);
      VLOG(DEBUG_V4) << "Execute START";

      wait_all_workers_start();
      VLOG(DEBUG_V4) << "wait Execute done";

      wait_all_workers_finish();
      send_ack();
      VLOG(DEBUG_V4) << "Execute FIN. SEND ACK";
    }
  }

  void add_worker(const std::shared_ptr<HermesExecutor<WorkloadType>>

                      &w) {
    workers.push_back(w);
  }

  void clear_lock_manager_status() { lock_manager_status.store(0); }

public:
  RandomType random;
  DatabaseType &db;
  HermesPartitioner<Workload> partitioner;
  std::atomic<uint32_t> lock_manager_status;
  std::vector<std::shared_ptr<HermesExecutor<WorkloadType>>> workers;
  std::vector<StorageType> storages;
  std::vector<std::unique_ptr<TransactionType>> transactions;
};
} // namespace star