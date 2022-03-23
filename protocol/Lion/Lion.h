//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include "core/ControlMessage.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "core/Worker.h"
#include "protocol/Silo/SiloHelper.h"
#include "protocol/Silo/SiloRWKey.h"
#include "protocol/Silo/SiloTransaction.h"
#include "protocol/Lion/LionManager.h"
#include "protocol/Lion/LionMessage.h"
#include <glog/logging.h>

namespace star {

template <class Database> class Lion {
public:
  using DatabaseType = Database;
  using MetaDataType = std::atomic<uint64_t>;
  using ContextType = typename DatabaseType::ContextType;
  using MessageType = LionMessage;
  using TransactionType = SiloTransaction;

  using MessageFactoryType = LionMessageFactory;
  using MessageHandlerType = LionMessageHandler<DatabaseType>;

  Lion(DatabaseType &db, const ContextType &context, Partitioner &partitioner, size_t id)
      : db(db), context(context), partitioner(partitioner), id(id) {}

  uint64_t search(std::size_t table_id, std::size_t partition_id,
                  const void *key, void *value) const {
    /**
     * @brief read value from the key in table[table_id] from partition[partition_id]
     * 
     */
    ITable *table = db.find_table(table_id, partition_id);
    auto value_bytes = table->value_size();
    auto row = table->search(key);
    return SiloHelper::read(row, value, value_bytes);
  }

  void abort(TransactionType &txn, const size_t& cur_ptr) {
    auto &writeSet = txn.writeSet;
    // unlock locked records
    size_t limits = writeSet.size(); // min(size, cur_ptr) 防止把别人刚刚锁上的内容给解锁了....
    if(cur_ptr < limits){
      limits = cur_ptr;
    }

    for (auto i = 0u; i < limits; i++) {
      auto &writeKey = writeSet[i];

      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto key = writeKey.get_key();
      std::atomic<uint64_t> &tid = table->search_metadata(key);
      // only unlock the one operated by itself!
      SiloHelper::unlock_if_locked(tid);
    }
  }

  bool commit(TransactionType &txn,
              std::vector<std::unique_ptr<Message>> &syncMessages,
              std::vector<std::unique_ptr<Message>> &asyncMessages,
              std::vector<std::unique_ptr<Message>> &recordMessages,
              std::atomic<uint32_t>& async_message_num
              ) {
    // lock write set
    // LOG(INFO) << "LionExecutor: "<< id << " " << "lock_write_set";
    size_t cur_ptr = 0;

    if (lock_write_set(txn, cur_ptr)) {
      abort(txn, cur_ptr);
      return false;
    }

    // LOG(INFO) << "LionExecutor: "<< id << " " << "validate_read_set";

    // commit phase 2, read validation
    if (!validate_read_set(txn)) {
      abort(txn, cur_ptr);
      return false;
    }

    // generate tid
    uint64_t commit_tid = generateTid(txn);

    // LOG(INFO) << "LionExecutor: "<< id << " " << "write_and_replicate";

    // write and replicate
    write_and_replicate(txn, commit_tid, syncMessages, asyncMessages, 
                        async_message_num);


    // LOG(INFO) << "LionExecutor: "<< id << " " << "async_txn_to_recorder";

    // 记录txn的record情况
    if(context.enable_data_transfer == true){
      async_txn_to_recorder(txn, recordMessages);
    }
    return true;
  }

private:
  bool lock_write_set(TransactionType &txn, size_t& cur_ptr) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;
    // 正常退出，cur_ptr == 本身
    cur_ptr = writeSet.size();

    for (auto i = 0u; i < writeSet.size(); i++) {
      // 发现后面的被其他人锁了咋办？
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      auto key = writeKey.get_key();
      std::atomic<uint64_t> &tid = table->search_metadata(key);

      bool success;
      uint64_t latestTid = SiloHelper::lock(tid, success);
      if (!success) {
        txn.abort_lock = true;
        cur_ptr = i;
        break;
      }

      writeKey.set_write_lock_bit();

      auto readKeyPtr = txn.get_read_key(key);
      // assume no blind write
      DCHECK(readKeyPtr != nullptr);
      uint64_t tidOnRead = readKeyPtr->get_tid();
      if (latestTid != tidOnRead) {
        txn.abort_lock = true;
        break;
      }

      writeKey.set_tid(latestTid);
    }
    return txn.abort_lock;
  }

  bool validate_read_set(TransactionType &txn) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    auto isKeyInWriteSet = [&writeSet](const void *key) {
      for (auto &writeKey : writeSet) {
        if (writeKey.get_key() == key) {
          return true;
        }
      }
      return false;
    };

    for (auto &readKey : readSet) {
      bool in_write_set = isKeyInWriteSet(readKey.get_key());
      if (in_write_set)
        continue; // already validated in lock write set

      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto key = readKey.get_key();
      uint64_t tid = table->search_metadata(key).load();

      // already been modified by other write txn, abort!
      if (SiloHelper::remove_lock_bit(tid) != readKey.get_tid()) {
        txn.abort_read_validation = true;
        return false;
      }

      if (SiloHelper::is_locked(tid)) { // must be locked by others
        txn.abort_read_validation = true;
        return false;
      }
    }
    return true;
  }

  uint64_t generateTid(TransactionType &txn) {

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
  void async_txn_to_recorder(TransactionType &txn, std::vector<std::unique_ptr<Message>> &recordMessages) {
    /**
     * @brief 发给每个coordinator的recorder， 统计txn的record关联度情况
     * @add by truth 22-01-12
     * @modify by truth 22-02-24
     * @note std::vector<int32_t> record_key_in_this_txn
     *      |  4bit    |  28bit  |
     *      |  tableID |  keyID  |
    */
    const std::vector<u_int64_t> record_key_in_this_txn = txn.get_query();

    for (auto k = 0u; k < partitioner.total_coordinators(); k++) {
      // 给每一个coordinator都发送该信息
        txn.network_size +=
            MessageFactoryType::new_async_txn_of_record_message(
                *recordMessages[k], record_key_in_this_txn);
    }
  }
  void
  write_and_replicate(TransactionType &txn, uint64_t commit_tid,
                      std::vector<std::unique_ptr<Message>> &syncMessages,
                      std::vector<std::unique_ptr<Message>> &asyncMessages,
                      std::atomic<uint32_t>& async_message_num) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    std::vector<std::atomic<uint64_t> *> tids;

    // write to local db

    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      auto key = writeKey.get_key();
      auto value = writeKey.get_value();

      std::atomic<uint64_t> &tid = table->search_metadata(key);
      if(!SiloHelper::is_locked(tid.load())){
        DCHECK(false);
      }
      // 1000057
      // 200002
      tids.push_back(&tid);
      table->update(key, value);
    }

    // replicate to remote db

    // operation replication optimization in the partitioned phase
    if (context.operation_replication) {
      //! TODO: not finished yet
      DCHECK(false);
      // txn.operation.set_tid(commit_tid);
      // auto partition_id = txn.operation.partition_id;

      // for (auto k = 0u; k < partitioner.total_coordinators(); k++) {
      //   // k does not have this partition
      //   if (!partitioner.is_partition_replicated_on(tableId, partitionId, key, k)) {
      //     continue;
      //   }
      //   // already write
      //   if (k == txn.coordinator_id) {
      //     continue;
      //   }

      //   txn.network_size +=
      //       MessageFactoryType::new_operation_replication_message(
      //           *asyncMessages[k], txn.operation);
      // }
    } else {
      // value replication
      for (auto i = 0u; i < writeSet.size(); i++) {
        auto &writeKey = writeSet[i];
        auto tableId = writeKey.get_table_id();
        auto partitionId = writeKey.get_partition_id();
        auto table = db.find_table(tableId, partitionId);

        auto key = writeKey.get_key();
        auto value = writeKey.get_value();

        // value replicate
        for (auto k = 0u; k < partitioner.total_coordinators(); k++) {

          // k does not have this partition
          if (!partitioner.is_partition_replicated_on(tableId, partitionId, key, k)) {
            continue;
          }

          // already write
          if (k == txn.coordinator_id) {
            continue;
          }
          // if (context.star_sync_in_single_master_phase) {
            // txn.pendingResponses++;
            txn.network_size +=
                MessageFactoryType::new_sync_value_replication_message(
                    *syncMessages[k], *table, key, value, commit_tid);
            async_message_num.fetch_add(1);
            // static int total_send = 0 ;
            // total_send ++ ;
            // LOG(INFO) << "total send : " << total_send;
          // } else {
          //   txn.network_size +=
          //       MessageFactoryType::new_async_value_replication_message(
          //           *asyncMessages[k], *table, key, value, commit_tid);
          // }
        }
        
      }
    }

    // if (context.star_sync_in_single_master_phase) {
    //   sync_messages(txn);
    // }

    for (auto i = 0u; i < tids.size(); i++) {
      auto tmp = tids[i]->load();
      if(SiloHelper::is_locked(tmp)){
        SiloHelper::unlock(*tids[i], commit_tid);
      } else {
        DCHECK(false);
      }
      
    }
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

public:
  size_t id;

};

} // namespace star
