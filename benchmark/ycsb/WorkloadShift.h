//
// Created by Yi Lu on 7/25/18.
//

#pragma once

#include "benchmark/tpcc/Context.h"
#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Random.h"
#include "benchmark/ycsb/Storage.h"
#include "benchmark/ycsb/Transaction.h"
#include "core/Partitioner.h"

namespace star {

namespace ycsb {

template <class Transaction> class Workload {
public:
  using TransactionType = Transaction;
  using DatabaseType = Database;
  using ContextType = Context;
  using RandomType = Random;
  using StorageType = Storage;
  static myTestSet which_workload;

  Workload(std::size_t coordinator_id, 
           std::atomic<uint32_t> &worker_status, DatabaseType &db, RandomType &random,
           Partitioner &partitioner, std::chrono::steady_clock::time_point start_time)
      : coordinator_id(coordinator_id), 
        worker_status(worker_status), db(db), random(random),
        partitioner(partitioner), start_time(start_time) {}

  std::unique_ptr<TransactionType> next_transaction(const ContextType &context,
                                                    std::size_t &partition_id,
                                                    StorageType &storage) {
    
    double cur_timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
                 std::chrono::steady_clock::now() - start_time)
                 .count() * 1.0 / 1000 / 1000;

    int workload_type_num = 4;
    int workload_type = ((int)cur_timestamp / context.workload_time % workload_type_num);// which_workload_(crossPartition, (int)cur_timestamp);
    // 同一个节点的不同部分（前后部分）
    // if(workload_type % 2 == 1) {
    // // if(workload_type % 2 == 0) {
    // partition_id = partition_id % (context.partition_num / 2);
    // } else {
    //   partition_id = context.partition_num / 2 + partition_id % (context.partition_num / 2);
    // }
    
    partition_id = (partition_id + workload_type) % context.partition_num;

    std::unique_ptr<TransactionType> p =
        std::make_unique<ReadModifyWrite<Transaction>>(
            coordinator_id, partition_id, worker_status, db, context, random, partitioner,
            storage, cur_timestamp);

    return p;
  }

  std::unique_ptr<TransactionType> unpack_transaction(const ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage, simpleTransaction& simple_txn) {

    std::unique_ptr<TransactionType> p =
        std::make_unique<ReadModifyWrite<Transaction>>(
            coordinator_id, partition_id, worker_status, db, context, random, partitioner,
            storage, simple_txn);

    return p;
  }


  std::unique_ptr<TransactionType> unpack_transaction(const ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage, simpleTransaction& simple_txn, bool is_transmit) {

    std::unique_ptr<TransactionType> p =
        std::make_unique<ReadModifyWrite<Transaction>>(
            coordinator_id, partition_id, worker_status, db, context, random, partitioner,
            storage, simple_txn);

    return p;
  }

  std::unique_ptr<TransactionType> reset_transaction(const ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage, 
                                                    TransactionType& txn) {

    std::unique_ptr<TransactionType> p =
        std::make_unique<ReadModifyWrite<Transaction>>(
            coordinator_id, partition_id, worker_status, db, context, random, partitioner,
            storage, 
            txn);
            
    p->startTime = txn.startTime;

    return p;
  }


private:
  std::size_t coordinator_id;
  std::atomic<uint32_t> &worker_status;
  DatabaseType &db;
  RandomType &random;
  Partitioner &partitioner;

public:
  std::chrono::steady_clock::time_point start_time;
};
template <class Transaction>
myTestSet Workload<Transaction>::which_workload = myTestSet::YCSB;

} // namespace ycsb
} // namespace star
