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

  static uint64_t next_transaction_id(uint64_t coordinator_id) {
    constexpr int coordinator_id_offset = 32;
    static std::atomic<int64_t> tid_static{1};
    auto tid = tid_static.fetch_add(1);
    return (coordinator_id << coordinator_id_offset) | tid;
  }

  Workload(std::size_t coordinator_id, 
           std::atomic<uint32_t> &worker_status, DatabaseType &db, RandomType &random,
           Partitioner &partitioner, std::chrono::steady_clock::time_point start_time)
      : coordinator_id(coordinator_id), 
        worker_status(worker_status), db(db), random(random),
        partitioner(partitioner), start_time(start_time) {}

  std::unique_ptr<TransactionType> next_transaction(const ContextType &context,
                                                    std::size_t &partition_id,
                                                    std::size_t worker_id,
                                                    StorageType &storage, 
                                                    std::size_t granule_id = 0) {

    auto random_seed = Time::now();
    random.set_seed(random_seed);

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
            storage, cur_timestamp, 
            granule_id);
      } else {
        p = std::make_unique<Payment<Transaction>>(coordinator_id, partition_id,
                                                   db, context, random,
                                                   partitioner, storage, 
                                                   granule_id);
      }
    } else if (context.workloadType == TPCCWorkloadType::NEW_ORDER_ONLY) {
      p = std::make_unique<NewOrder<Transaction>>(coordinator_id, partition_id,
                                                  worker_status, 
                                                  db, context, random,
                                                  partitioner, storage, 
                                                  cur_timestamp, granule_id);
    } else {
      p = std::make_unique<Payment<Transaction>>(coordinator_id, partition_id,
                                                 db, context, random,
                                                 partitioner, storage, 
                                                 granule_id);
    }
    p->txn_random_seed_start = random_seed;
    p->transaction_id = next_transaction_id(coordinator_id);
    return p;
  }

  std::unique_ptr<TransactionType> unpack_transaction(const ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage, simpleTransaction& simple_txn, 
                                                    std::size_t granule_id = 0) {
    std::unique_ptr<TransactionType>  p = std::make_unique<NewOrder<Transaction>>(coordinator_id, partition_id,
     worker_status,
     db, context, random,
     partitioner, storage, simple_txn, granule_id);
    return p;
  }

    std::unique_ptr<TransactionType> unpack_transaction(const ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage, simpleTransaction& simple_txn, bool is_transmit, 
                                                    std::size_t granule_id = 0) {
    std::unique_ptr<TransactionType>  p = std::make_unique<NewOrder<Transaction>>(coordinator_id, partition_id,
     worker_status,
     db, context, random,
     partitioner, storage, simple_txn, is_transmit, granule_id);
    return p;
  }

  std::unique_ptr<TransactionType> reset_transaction(const ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage, 
                                                    TransactionType& txn, 
                                                    std::size_t granule_id = 0) {
    std::unique_ptr<TransactionType> p =
        std::make_unique<NewOrder<Transaction>>(
            coordinator_id, partition_id, 
            worker_status, 
            db, context, random, 
            partitioner, storage, txn, granule_id);
            
    p->startTime = txn.startTime;

    return p;
  }


  std::unique_ptr<TransactionType> deserialize_from_raw(const ContextType &context, 
                                                        StorageType &storage, 
                                                        const std::string & data) {

    double cur_timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
                 std::chrono::steady_clock::now() - start_time)
                 .count() * 1.0 / 1000 / 1000;


    Decoder decoder(data);
    uint64_t seed;
    uint32_t txn_type;
    std::size_t ith_replica;
    std::size_t partition_id;
    int64_t transaction_id;
    uint64_t straggler_wait_time;
    decoder >> transaction_id >> txn_type >> straggler_wait_time >> ith_replica >> seed >> partition_id;
    RandomType random;
    random.set_seed(seed);

    if (txn_type == 0) {
      auto p = std::make_unique<NewOrder<Transaction>>(
            coordinator_id, partition_id, 
            worker_status,
            db, context, random, partitioner,
            storage, cur_timestamp, ith_replica);
      p->txn_random_seed_start = seed;
      p->transaction_id = transaction_id;
      p->straggler_wait_time = straggler_wait_time;
      p->deserialize_lock_status(decoder);
      return p;
    } else {
      auto p = std::make_unique<Payment<Transaction>>(coordinator_id, partition_id,
                                                   db, context, random,
                                                   partitioner, storage,
                                                   ith_replica);
      p->txn_random_seed_start = seed;
      p->transaction_id = transaction_id;
      p->straggler_wait_time = straggler_wait_time;
      p->deserialize_lock_status(decoder);
      return p;
    }
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
