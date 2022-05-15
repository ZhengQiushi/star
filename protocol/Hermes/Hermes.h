//
// Created by Yi Lu on 9/14/18.
//

#pragma once

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Hermes/HermesHelper.h"
#include "protocol/Hermes/HermesMessage.h"
#include "protocol/Hermes/HermesTransaction.h"

namespace star {

template <class Database> class Hermes {
public:
  using DatabaseType = Database;  
  using MetaDataType = std::atomic<uint64_t>;
  using ContextType = typename DatabaseType::ContextType;
  using MessageType = HermesMessage;
  using TransactionType = HermesTransaction;

  using MessageFactoryType = HermesMessageFactory;
  using MessageHandlerType = HermesMessageHandler;

  Hermes(DatabaseType &db, const ContextType& context, Partitioner &partitioner)
      : db(db), context(context), partitioner(partitioner) {}

  void abort(TransactionType &txn, std::size_t lock_manager_id,
             std::size_t n_lock_manager, std::size_t replica_group_size) {
    // release read locks
    release_read_locks(txn, lock_manager_id, n_lock_manager,
                       replica_group_size);
  }

  bool commit(TransactionType &txn, std::size_t lock_manager_id,
              std::size_t n_lock_manager, std::size_t replica_group_size) {

    // write to db
    write(txn, lock_manager_id, n_lock_manager, replica_group_size);

    // release read/write locks
    release_read_locks(txn, lock_manager_id, n_lock_manager,
                       replica_group_size);
    release_write_locks(txn, lock_manager_id, n_lock_manager,
                        replica_group_size);

    return true;
  }

  void write(TransactionType &txn, std::size_t lock_manager_id,
             std::size_t n_lock_manager, std::size_t replica_group_size) {

    auto &writeSet = txn.writeSet;
    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto key = writeKey.get_key();

      if (!partitioner.is_partition_replicated_on(tableId, partitionId, key, context.coordinator_id)) {
        continue;
      }

      if (HermesHelper::partition_id_to_lock_manager_id(
              writeKey.get_partition_id(), n_lock_manager,
              replica_group_size) != lock_manager_id) {
        continue;
      }

      auto value = writeKey.get_value();
      table->update(key, value);
    }
  }

  void release_read_locks(TransactionType &txn, std::size_t lock_manager_id,
                          std::size_t n_lock_manager,
                          std::size_t replica_group_size) {
    // release read locks
    auto &readSet = txn.readSet;

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readKey = readSet[i];
      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto key = readKey.get_key();

      if (readKey.get_master_coordinator_id() != context.coordinator_id && 
          readKey.get_secondary_coordinator_id() != context.coordinator_id) {
        continue;
      }

      if (!readKey.get_read_lock_bit()) {
        continue;
      }

      if (HermesHelper::partition_id_to_lock_manager_id(
              readKey.get_partition_id(), n_lock_manager, replica_group_size) !=
          lock_manager_id) {
        continue;
      }

      auto value = readKey.get_value();
      std::atomic<uint64_t> &tid = table->search_metadata(key);
      HermesHelper::read_lock_release(tid);
    }
  }

  void release_write_locks(TransactionType &txn, std::size_t lock_manager_id,
                           std::size_t n_lock_manager,
                           std::size_t replica_group_size) {

    // release write lock
    auto &writeSet = txn.writeSet;

    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto key = writeKey.get_key();

      if (!partitioner.is_partition_replicated_on(tableId, partitionId, key, context.coordinator_id)) {
        continue;
      }

      if (HermesHelper::partition_id_to_lock_manager_id(
              writeKey.get_partition_id(), n_lock_manager,
              replica_group_size) != lock_manager_id) {
        continue;
      }

      auto value = writeKey.get_value();
      std::atomic<uint64_t> &tid = table->search_metadata(key);
      HermesHelper::write_lock_release(tid);
    }
  }

private:
  DatabaseType &db;
  const ContextType &context;
  Partitioner &partitioner;
};
} // namespace star