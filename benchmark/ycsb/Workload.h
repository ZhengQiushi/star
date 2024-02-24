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

  static uint64_t next_transaction_id(uint64_t coordinator_id, uint64_t cluster_worker_id) {
    constexpr int coordinator_id_offset = 40;
    constexpr int worker_id_offset = 32;
    static std::atomic<int64_t> tid_static{1};
    auto tid = tid_static.fetch_add(1);
    return (coordinator_id << coordinator_id_offset) | (cluster_worker_id << worker_id_offset) | tid;
  }
  
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

  std::unique_ptr<TransactionType> next_transaction(ContextType &context,
                                                    std::size_t partition_id,
                                                    std::size_t worker_id,
                                                    std::size_t granule_id) {
    // const static uint32_t num_workers_per_node = context.partition_num / context.coordinator_num;
    // int cluster_worker_id = coordinator_id * num_workers_per_node + worker_id;
    // if (cluster_worker_id == 1) {
    //   context.crossPartitionProbability = 100;
    // }
    double cur_timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
                 std::chrono::steady_clock::now() - start_time)
                 .count() * 1.0 / 1000 / 1000;

    static std::atomic<uint64_t> tid_cnt(0);
    long long transactionId = tid_cnt.fetch_add(1);
    auto random_seed = Time::now();
    random.set_seed(random_seed);
    std::unique_ptr<TransactionType> p =
        std::make_unique<ReadModifyWrite<Transaction>>(
            coordinator_id, partition_id, granule_id, worker_status, db, context, random, partitioner, 0, cur_timestamp);
    p->txn_random_seed_start = random_seed;
    p->transaction_id = next_transaction_id(coordinator_id, worker_id);
    return p;
  }


  std::unique_ptr<TransactionType> next_transaction(const ContextType &context,
                                                    std::size_t &partition_id,
                                                    std::size_t worker_id,
                                                    StorageType &storage) {
    
    double cur_timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
                 std::chrono::steady_clock::now() - start_time)
                 .count() * 1.0 / 1000 / 1000;

    int workload_type_num = 3;
    int workload_type = ((int)cur_timestamp / context.workload_time % workload_type_num) + 1;// which_workload_(crossPartition, (int)cur_timestamp);
    // 同一个节点的不同部分（前后部分）
    // if(workload_type % 2 == 1) {
    // // if(workload_type % 2 == 0) {
    // partition_id = partition_id % (context.partition_num / 2);
    // } else {
    //   partition_id = context.partition_num / 2 + partition_id % (context.partition_num / 2);
    // }
    auto random_seed = Time::now();
    random.set_seed(random_seed);
    
    std::unique_ptr<TransactionType> p =
        std::make_unique<ReadModifyWrite<Transaction>>(
            coordinator_id, partition_id, worker_status, db, context, random, partitioner,
            storage, cur_timestamp);
    p->txn_random_seed_start = random_seed;
    p->transaction_id = next_transaction_id(coordinator_id, worker_id);
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

  std::unique_ptr<TransactionType> deserialize_from_raw(ContextType &context, const std::string & data) {
    Decoder decoder(data);
    uint64_t seed;
    std::size_t ith_replica;
    std::size_t partition_id;
    std::size_t granule_id;
    int32_t partition_count;
    int64_t transaction_id;
    uint64_t straggler_wait_time;

    // std::vector<int32_t> partitions_from_command, granules_from_command;
    // int32_t granule_count = 0;
    decoder >> transaction_id >> straggler_wait_time >> ith_replica >> seed >> partition_id >> granule_id >> partition_count;
    // for (int32_t i = 0; i < partition_count; ++i){
    //   int32_t p;
    //   decoder >> p;
    //   partitions_from_command.push_back(p);
    // }
    // decoder >> granule_count;
    // for (int32_t i = 0; i < granule_count; ++i){
    //   int32_t g;
    //   decoder >> g;
    //   granules_from_command.push_back(g);
    // }
    RandomType random;
    random.set_seed(seed);

     double cur_timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
                 std::chrono::steady_clock::now() - start_time)
                 .count() * 1.0 / 1000 / 1000;

    std::unique_ptr<TransactionType> p =
        std::make_unique<ReadModifyWrite<Transaction>>(
            coordinator_id, partition_id, granule_id, worker_status, db, context, random, partitioner, ith_replica, cur_timestamp);
    p->txn_random_seed_start = seed;
    DCHECK(p->get_partition_count() == partition_count);
    // std::vector<int32_t> partitions, granules;
    // for (int32_t i = 0; i < partition_count; ++i){
    //   partitions.push_back(p->get_partition(i));
    //   for (int32_t j = 0; j < p->get_partition_granule_count(i); ++j) {
    //     granules.push_back(p->get_granule(i, j));
    //   }
    // }
    // sort(granules.begin(), granules.end());
    // sort(partitions.begin(), partitions.end());
    // sort(partitions_from_command.begin(), partitions_from_command.end());
    // sort(granules_from_command.begin(), granules_from_command.end());
    // DCHECK(granules == granules_from_command);
    // DCHECK(partitions == partitions_from_command);
    p->transaction_id = transaction_id;
    p->straggler_wait_time = straggler_wait_time;
    p->deserialize_lock_status(decoder);
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
