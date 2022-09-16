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
// #include "protocol/TwoPL/TwoPLHelper.h"
// #include "protocol/TwoPL/TwoPLRWKey.h"
// #include "protocol/TwoPL/TwoPLTransaction.h"

// #include "protocol/Silo/SiloHelper.h"
#include "protocol/Lion/LionRWKey.h"
// #include "protocol/Silo/SiloTransaction.h"

#include "protocol/Lion/LionTransaction.h"
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
  using TransactionType = LionTransaction; // TwoPLTransaction;// 

  using MessageFactoryType = LionMessageFactory;
  using MessageHandlerType = LionMessageHandler<DatabaseType>;


  Lion(DatabaseType &db, const ContextType &context, Partitioner &partitioner, size_t id)
      : db(db), context(context), partitioner(partitioner), id(id) {}

  uint64_t search(std::size_t table_id, std::size_t partition_id,
                  const void *key, void *value, bool& success) const {
    /**
     * @brief read value from the key in table[table_id] from partition[partition_id]
     * 
     */
    ITable *table = db.find_table(table_id, partition_id);
    auto value_bytes = table->value_size();
    success = table->contains(key);
    if(success == true){
      auto row = table->search(key);
      return TwoPLHelper::read(row, value, value_bytes);
    } else {
      return 0;
    }
  }

  void abort(TransactionType &txn,
             std::vector<std::unique_ptr<Message>> &messages) {

    auto &readSet = txn.readSet;
    // unlock locked records
    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readKey = readSet[i];
      if(!readKey.get_read_respond_bit()){
        continue;
      }
      // only unlock locked records
      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto key = readKey.get_key();

      // remote
      auto coordinatorID = readKey.get_dynamic_coordinator_id();

      if (coordinatorID == context.coordinator_id) {
        std::atomic<uint64_t> &tid = table->search_metadata(key);
        if(readKey.get_write_lock_bit()){
          TwoPLHelper::write_lock_release(tid);
          VLOG(DEBUG_V14) << "  unLOCK-write " << *(int*)key << " " << tid.load();
        } else {
          TwoPLHelper::read_lock_release(tid);
          VLOG(DEBUG_V14) << "  unLOCK-read " << *(int*)key << " " << tid.load();
        }
      } else {
        DCHECK(false);
      }
    }

    // unlock_all_read_locks(txn);
    
    sync_messages(txn, false);
  }


  bool commit(TransactionType &txn,
              std::vector<std::unique_ptr<Message>> &messages,
              std::atomic<uint32_t> &async_message_num) {

    // // lock write set
    // if (lock_write_set(txn, messages)) {
    //   abort(txn, messages);
    //   return false;
    // }

    // // commit phase 2, read validation
    // if (!validate_read_set(txn, messages)) {
    //   abort(txn, messages);
    //   return false;
    // }
    if(txn.is_abort()){
      abort(txn, messages);
    }

    // generate tid
    uint64_t commit_tid = generate_tid(txn);

    // write and replicate
    write_and_replicate(txn, commit_tid, messages, async_message_num);

    // release locks
    release_lock(txn, commit_tid, messages);

    return true;
  }

private:
  bool lock_write_set(TransactionType &txn,
                      std::vector<std::unique_ptr<Message>> &messages) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet; // temporary for test ! 

    for (auto i = 0u; i < writeSet.size(); i++) {
      if(txn.is_abort()){
        break;
      }
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto key = writeKey.get_key();

      // 
      auto coordinatorID = writeKey.get_dynamic_coordinator_id();// partitioner.master_coordinator(tableId, partitionId, key);

      // lock local records
      if (coordinatorID == context.coordinator_id) {
        
        std::atomic<uint64_t> &tid = table->search_metadata(key);
        bool success;
        uint64_t latestTid = TwoPLHelper::write_lock(tid, success);

        if (!success) {
          txn.abort_lock = true;
          VLOG(DEBUG_V14) << "failed to LOCK " << *(int*)key;
          break;
        }
        VLOG(DEBUG_V14) << "LOCK " << *(int*)key;

        writeKey.set_write_lock_bit();
        writeKey.set_tid(latestTid);

        auto readKeyPtr = txn.get_read_key(key);
        // assume no blind write
        DCHECK(readKeyPtr != nullptr);
        uint64_t tidOnRead = readKeyPtr->get_tid();
        if (latestTid != tidOnRead) {
          txn.abort_lock = true;
          break;
        }

      } else {
        // remote reads
        VLOG(DEBUG_V14) << "failed to LOCK " << *(int*)key;
        txn.abort_lock = true;
        break;
      }
    }
    if(!txn.is_abort()){
      sync_messages(txn);
    }
    
    return txn.is_abort();
  }


  bool validate_read_set(TransactionType &txn,
                         std::vector<std::unique_ptr<Message>> &messages) {

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

    for (auto i = 0u; i < readSet.size(); i++) {
      if(txn.is_abort()){
        break;
      }
      auto &readKey = readSet[i];

      if (readKey.get_local_index_read_bit()) {
        continue; // read only index does not need to validate
      }

      bool in_write_set = isKeyInWriteSet(readKey.get_key());
      if (in_write_set) {
        continue; // already validated in lock write set
      }

      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto key = readKey.get_key();
      auto tid = readKey.get_tid();


      // lock 
      auto coordinatorID = readKey.get_dynamic_coordinator_id(); // partitioner.master_coordinator(tableId, partitionId, key);
      if (coordinatorID == context.coordinator_id) {
        // local read
        bool is_success = table->contains(key);
        if(is_success == false){
          txn.abort_read_validation = true;
          txn.abort_lock = true;
          break; 
        }
        uint64_t latest_tid = table->search_metadata(key).load();
        if (TwoPLHelper::remove_lock_bit(latest_tid) != tid) {
          txn.abort_read_validation = true;
          break;
        }
        if (TwoPLHelper::is_write_locked(latest_tid)) { // must be locked by others
          txn.abort_read_validation = true;
          break;
        }
      } else {
        // remote 
        DCHECK(false);
      }
    }

    if (txn.pendingResponses == 0) {
      txn.local_validated = true;
    }
    if(!txn.is_abort()){
      sync_messages(txn);
    }
    

    return !txn.is_abort(); //txn.abort_read_validation;
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

  void write_and_replicate(TransactionType &txn, uint64_t commit_tid,
                           std::vector<std::unique_ptr<Message>> &messages,
                           std::atomic<uint32_t> &async_message_num) {

    auto &readSet = txn.readSet;

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readKey = readSet[i];
      if(!readKey.get_write_lock_bit()){
        continue;
      }
      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto key = readKey.get_key();

      // lock dynamic replica
      auto coordinatorID = readKey.get_dynamic_coordinator_id(); // partitioner.master_coordinator(tableId, partitionId, key);
      auto secondary_coordinatorIDs = readKey.get_dynamic_secondary_coordinator_id();
      // DCHECK(coordinatorID != secondary_coordinatorID);
      // auto router_table = db.find_router_table(tableId, coordinatorID);
      // std::atomic<uint64_t> &tid_ = router_table->search_metadata(key);

      // uint64_t last_tid_ = TwoPLHelper::lock(tid_);
      // DCHECK(last_tid_ < commit_tid);
      // LOG(INFO) << "LOCK " << *(int*)key;
      
      // write
      if (coordinatorID == context.coordinator_id) {
        
        auto value = readKey.get_value();
        table->update(key, value);
      } else {
        DCHECK(false) << *(int*) key;
      }

      // value replicate

      std::size_t replicate_count = 0;
      
      bool send_replica = false;

      for (auto k = 0u; k < partitioner.total_coordinators(); k++) {
        
        // k does not have this partition
        if (!secondary_coordinatorIDs.count(k) && k != coordinatorID) {
          continue;
        }

        // already write
        if (k == coordinatorID) {
          continue;
        }

        // local replicate
        // txn execute here but the master replica was at some other place. 
        if (k == txn.coordinator_id && coordinatorID != k) {
          DCHECK(false);
        } else {
          // txn.pendingResponses++;
          auto coordinatorID = k;
          txn.network_size += MessageFactoryType::new_replication_message(
              *messages[coordinatorID], *table, readKey.get_key(),
              readKey.get_value(), commit_tid);
          
          VLOG(DEBUG_V14) << " async_message_num: " << context.coordinator_id << " -> " << k << " " << async_message_num.load() << " " << *(int*)readKey.get_key() << " " << (char*)readKey.get_value();
          DCHECK(strlen((char*)readKey.get_value()) > 0);
          async_message_num.fetch_add(1);
          send_replica = true;
        }
      }

      if(send_replica == false){
        DCHECK(false);
      }
      // DCHECK(replicate_count == partitioner.replica_num() - 1);
    }

    sync_messages(txn, false);
  }

  void release_lock(TransactionType &txn, uint64_t commit_tid,
                    std::vector<std::unique_ptr<Message>> &messages) {

    auto &readSet = txn.readSet;

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readKey = readSet[i];
      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto key = readKey.get_key();

      auto coordinatorID = readKey.get_dynamic_coordinator_id(); // partitioner.master_coordinator(tableId, partitionId, key);
      // write
      if (coordinatorID == context.coordinator_id) {
        std::atomic<uint64_t> &tid = table->search_metadata(key);
        if(readKey.get_write_lock_bit()){
          TwoPLHelper::write_lock_release(tid, commit_tid);
          VLOG(DEBUG_V14) << "  unLOCK-write " << *(int*)key << " " << tid.load() << " " << commit_tid;
        } else {
          TwoPLHelper::read_lock_release(tid);
          VLOG(DEBUG_V14) << "  unLOCK-read " << *(int*)key << " " << tid.load() << " " << commit_tid;
        }

      } else {
        DCHECK(false);
      }

    }

    // unlock_all_read_locks(txn);

    sync_messages(txn, false);
  }

  // void unlock_all_read_locks(TransactionType &txn){
  //   auto &readSet = txn.readSet;
  //   // release local lock. All data item should be moved to the current node. 
  //   for (auto i = 0u; i < readSet.size(); i++) {
  //     auto &readKey = readSet[i];
  //     auto tableId = readKey.get_table_id();
  //     auto partitionId = readKey.get_partition_id();
  //     auto table = db.find_table(tableId, partitionId);
  //     auto key = readKey.get_key();
          
  //     // if(partitioner.is_dynamic()){
  //     //   // unlock dynamic replica
  //     //   auto coordinatorID = readKey.get_dynamic_coordinator_id(); // partitioner.master_coordinator(tableId, partitionId, key);

  //     //   auto router_table = db.find_router_table(tableId, coordinatorID);
  //     //   std::atomic<uint64_t> &tid = router_table->search_metadata(key);
  //     //   TwoPLHelper::unlock_if_locked(tid);
  //     // }
  //   }
  // }
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
