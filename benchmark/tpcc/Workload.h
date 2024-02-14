//
// Created by Yi Lu on 7/24/18.
//

#pragma once

#include "benchmark/tpcc/Context.h"
#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Random.h"
#include "benchmark/tpcc/Storage.h"
#include "benchmark/tpcc/Transaction.h"
#include "core/Partitioner.h"

namespace star {

namespace tpcc {

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

    int x = random.uniform_dist(1, 100);
    std::unique_ptr<TransactionType> p;

    double cur_timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
                 std::chrono::steady_clock::now() - start_time)
                 .count() * 1.0 / 1000 / 1000;

    // int workload_type_num = 3;
    // int workload_type = ((int)cur_timestamp / context.workload_time % workload_type_num) + 1;// which_workload_(crossPartition, (int)cur_timestamp);
    // 同一个节点的不同部分（前后部分）
    // if(workload_type <= 2) {
    // // if(workload_type % 2 == 0) {
    //   partition_id = partition_id % (context.partition_num / 2);
    // } else {
    //   partition_id = context.partition_num / 2 + partition_id % (context.partition_num / 2);
    // }


    if (context.workloadType == TPCCWorkloadType::MIXED) {
      if (x <= 50) {
        p = std::make_unique<NewOrder<Transaction>>(
            coordinator_id, partition_id, 
            worker_status,
            db, context, random, partitioner,
            storage, cur_timestamp);
      } else {
        p = std::make_unique<Payment<Transaction>>(coordinator_id, partition_id,
                                                   db, context, random,
                                                   partitioner, storage);
      }
    } else if (context.workloadType == TPCCWorkloadType::NEW_ORDER_ONLY) {
      p = std::make_unique<NewOrder<Transaction>>(coordinator_id, partition_id,
                                                  worker_status, 
                                                  db, context, random,
                                                  partitioner, storage, 
                                                  cur_timestamp);
    } else {
      p = std::make_unique<Payment<Transaction>>(coordinator_id, partition_id,
                                                 db, context, random,
                                                 partitioner, storage);
    }

    return p;
  }

  std::unique_ptr<TransactionType> unpack_transaction(const ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage, simpleTransaction& simple_txn) {
    std::unique_ptr<TransactionType>  p = std::make_unique<NewOrder<Transaction>>(coordinator_id, partition_id,
     worker_status,
     db, context, random,
     partitioner, storage, simple_txn);
    return p;
  }

    std::unique_ptr<TransactionType> unpack_transaction(const ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage, simpleTransaction& simple_txn, bool is_transmit) {
    std::unique_ptr<TransactionType>  p = std::make_unique<NewOrder<Transaction>>(coordinator_id, partition_id,
     worker_status,
     db, context, random,
     partitioner, storage, simple_txn, is_transmit);
    return p;
  }

  std::unique_ptr<TransactionType> reset_transaction(const ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage, 
                                                    TransactionType& txn) {
    std::unique_ptr<TransactionType> p =
        std::make_unique<NewOrder<Transaction>>(
            coordinator_id, partition_id, 
            worker_status, 
            db, context, random, 
            partitioner, storage, txn);
            
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
myTestSet Workload<Transaction>::which_workload = myTestSet::TPCC;
} // namespace tpcc
} // namespace star
