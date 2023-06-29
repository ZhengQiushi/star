//
// Created by Yi Lu on 9/10/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/TwoPL/TwoPLHelper.h"
#include "protocol/MyClay/MyClayTransaction.h"
#include "protocol/MyClay/MyClayMessage.h"
#include <glog/logging.h>

namespace star {

template <class Database> class MyClay {
public:
  using DatabaseType = Database;
  using MetaDataType = std::atomic<uint64_t>;
  using ContextType = typename DatabaseType::ContextType;
  using MessageType = MyClayMessage;
  using TransactionType = MyClayTransaction;

  using MessageFactoryType = MyClayMessageFactory;
  using MessageHandlerType = MyClayMessageHandler<DatabaseType>;

  MyClay(DatabaseType &db, const ContextType &context, Partitioner &partitioner)
      : db(db), context(context), partitioner(partitioner) {}

  uint64_t search(std::size_t table_id, std::size_t partition_id,
                  const void *key, void *value) const {

    ITable *table = db.find_table(table_id, partition_id);
    auto value_bytes = table->value_size();
    auto row = table->search(key);
    return TwoPLHelper::read(row, value, value_bytes);
  }

  uint64_t generate_tid(TransactionType &txn) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    uint64_t next_tid = 0;

    /*
     *  A timestamp is a 64-bit word.
     *  The most significant bit is the lock bit.
     *  The lower 63 bits are for transaction sequence id.
     *  [  lock bit (1)  |  id (63) ]
     */

    // larger than the TID of any record read or written by the transaction

    for (std::size_t i = 0; i < readSet.size(); i++) {
      next_tid = std::max(next_tid, readSet[i].get_tid());
    }

    for (std::size_t i = 0; i < writeSet.size(); i++) {
      next_tid = std::max(next_tid, writeSet[i].get_tid());
    }

    // larger than the worker's most recent chosen TID

    next_tid = std::max(next_tid, max_tid);

    // increment

    next_tid++;

    // update worker's most recent chosen TID

    max_tid = next_tid;

    return next_tid;
  }


  void abort(TransactionType &txn,
             std::vector<std::unique_ptr<Message>> &syncMessages) {

    // auto &writeSet = txn.writeSet;
    auto &readSet = txn.readSet;
    // unlock locked records

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readKey = readSet[i];
      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      auto coordinatorID = readKey.get_dynamic_coordinator_id();
      auto key = readKey.get_key();

      if (readKey.get_read_lock_bit()) {
        if (coordinatorID == context.coordinator_id) {
          auto value = readKey.get_value();
          std::atomic<uint64_t> &tid = table->search_metadata(key);
          TwoPLHelper::read_lock_release(tid);
        } else {
          // auto coordinatorID = partitioner.master_coordinator(partitionId);
          LOG(INFO) << " ABORT_REQUEST " << *(int*)key << " " << false;

          txn.network_size += MessageFactoryType::new_abort_message(
              *syncMessages[coordinatorID], *table, readKey.get_key(), false);
        }
      }

      if (readKey.get_write_lock_bit()) {
        if (coordinatorID == context.coordinator_id) {
          auto value = readKey.get_value();
          std::atomic<uint64_t> &tid = table->search_metadata(key);
          TwoPLHelper::write_lock_release(tid);
        } else {
          // auto coordinatorID = partitioner.master_coordinator(partitionId);
          LOG(INFO) << " ABORT_REQUEST " << *(int*)key << " " << true;
          txn.network_size += MessageFactoryType::new_abort_message(
              *syncMessages[coordinatorID], *table, readKey.get_key(), true);
        }
      }
    }

    sync_messages(txn, false);
  }

  bool commit(TransactionType &txn,
              std::vector<std::unique_ptr<Message>> &syncMessages,
              std::vector<std::unique_ptr<Message>> &asyncMessages) {

    if (txn.abort_lock) {
      abort(txn, syncMessages);
      return false;
    }

    // generate tid
    uint64_t commit_tid = generate_tid(txn);

    // write and replicate
    write(txn, commit_tid, syncMessages);

    // release locks
    release_lock(txn, commit_tid, syncMessages);

    // replicate
    replicate(txn, commit_tid, asyncMessages);

    return true;
  }

  void release_lock(TransactionType &txn, uint64_t commit_tid,
                    std::vector<std::unique_ptr<Message>> &messages) {

    // release read locks
    auto &readSet = txn.readSet;

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readKey = readSet[i];
      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      auto coordinatorID = readKey.get_dynamic_coordinator_id();

      if (readKey.get_read_lock_bit()) {
        if (coordinatorID == context.coordinator_id) {
          auto key = readKey.get_key();
          auto value = readKey.get_value();
          std::atomic<uint64_t> &tid = table->search_metadata(key);
          TwoPLHelper::read_lock_release(tid);
        } else {
          // auto coordinatorID = partitioner.master_coordinator(partitionId);
          txn.network_size += MessageFactoryType::new_release_read_lock_message(
              *messages[coordinatorID], *table, readKey.get_key());
        }
      }

      if (readKey.get_write_lock_bit()) {
        if (coordinatorID == context.coordinator_id) {
          auto key = readKey.get_key();
          auto value = readKey.get_value();
          std::atomic<uint64_t> &tid = table->search_metadata(key);
          TwoPLHelper::write_lock_release(tid);
        } else {
          // auto coordinatorID = partitioner.master_coordinator(partitionId);
          txn.network_size += MessageFactoryType::new_release_write_lock_message(
              *messages[coordinatorID], *table, readKey.get_key(), commit_tid);
        }
      }
    }
  }


private:

  void write(TransactionType &txn, uint64_t commit_tid,
             std::vector<std::unique_ptr<Message>> &messages) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readKey = readSet[i];
      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      if(!readKey.get_write_lock_bit()){
        continue;
      }
      // write
      auto coordinatorID = readKey.get_dynamic_coordinator_id();

      // write
      if (coordinatorID == context.coordinator_id) {
        auto key = readKey.get_key();
        auto value = readKey.get_value();
        table->update(key, value);
      } else {
        txn.pendingResponses++;
        txn.network_size += MessageFactoryType::new_write_message(
            *messages[coordinatorID], *table, readKey.get_key(),
            readKey.get_value());
      }
    }

    sync_messages(txn);
  }


  void replicate(TransactionType &txn, uint64_t commit_tid,
                 std::vector<std::unique_ptr<Message>> &messages) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readKey = readSet[i];
      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      if(!readKey.get_write_lock_bit()){
        continue;
      }
      // value replicate

      std::size_t replicate_count = 0;

      auto coordinatorID = readKey.get_dynamic_coordinator_id();
      const RouterValue* const router_val = readKey.get_router_value();


      for (auto k = 0u; k < partitioner.total_coordinators(); k++) {

        // k does not have this partition
        if (!router_val->count_secondary_coordinator_id(k) && k != coordinatorID) {
          continue;
        }

        // already write
        if (k == coordinatorID) {
          continue;
        }
        replicate_count++;

        // local replicate
        if (k == txn.coordinator_id) {
          auto key = readKey.get_key();
          auto value = readKey.get_value();
          table->update(key, value);
        } else {

          txn.pendingResponses++;
          auto coordinatorID = k;
          txn.network_size += MessageFactoryType::new_replication_message(
              *messages[coordinatorID], *table, readKey.get_key(),
              readKey.get_value(), commit_tid);
        }
      }

      // DCHECK(replicate_count == partitioner.replica_num() - 1);
    }

    sync_messages(txn, false);
  }

  void sync_messages(TransactionType &txn, bool wait_response = true) {
    txn.message_flusher();
    if (wait_response) {
      while (txn.pendingResponses > 0) {
        txn.remote_request_handler();
      }
    }
  }

private:
  DatabaseType &db;
  const ContextType &context;
  Partitioner &partitioner;
  uint64_t max_tid = 0;
};

} // namespace star
