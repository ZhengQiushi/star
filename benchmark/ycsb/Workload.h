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

  Workload(std::size_t coordinator_id, DatabaseType &db, RandomType &random,
           Partitioner &partitioner, int workload_type)
      : coordinator_id(coordinator_id), db(db), random(random),
        partitioner(partitioner), workload_type(workload_type) {}

  std::unique_ptr<TransactionType> next_transaction(const ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage) {

    std::unique_ptr<TransactionType> p =
        std::make_unique<ReadModifyWrite<Transaction>>(
            coordinator_id, partition_id, db, context, random, partitioner,
            storage, workload_type);

    return p;
  }

  std::unique_ptr<TransactionType> unpack_transaction(const ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage, simpleTransaction& simple_txn) {

    std::unique_ptr<TransactionType> p =
        std::make_unique<ReadModifyWrite<Transaction>>(
            coordinator_id, partition_id, db, context, random, partitioner,
            storage, simple_txn);

    return p;
  }
private:
  std::size_t coordinator_id;
  DatabaseType &db;
  RandomType &random;
  Partitioner &partitioner;
  int workload_type;
};
template <class Transaction>
myTestSet Workload<Transaction>::which_workload = myTestSet::YCSB;

} // namespace ycsb
} // namespace star
