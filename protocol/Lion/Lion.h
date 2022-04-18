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
  using TransactionType = SiloTransaction; // TwoPLTransaction;// 

  using MessageFactoryType = LionMessageFactory;
  using MessageHandlerType = LionMessageHandler<DatabaseType>;

  using HelperType = SiloHelper;// TwoPLHelper;

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
    return HelperType::read(row, value, value_bytes);
  }

  void abort(TransactionType &txn,
             std::vector<std::unique_ptr<Message>> &messages) {

    auto &writeSet = txn.writeSet;
    // unlock locked records
    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      // only unlock locked records
      if (!writeKey.get_write_lock_bit())
        continue;
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto key = writeKey.get_key();

      // remote
      auto coordinatorID = writeKey.get_dynamic_coordinator_id();

      if (coordinatorID == context.coordinator_id) {
        
        std::atomic<uint64_t> &tid = table->search_metadata(key);
        HelperType::unlock(tid);
      } else {
        
        txn.network_size += MessageFactoryType::new_abort_message(
            *messages[coordinatorID], *table, writeKey.get_key());
      }
    }

    // unlock_all_read_locks(txn);
    
    sync_messages(txn, false);
  }

  // bool router_abort(TransactionType &txn){
  //   auto &routerSet = txn.routerSet;

  //   for (int i = int(routerSet.size()) - 1; i >= 0; i--) {
  //     auto &routerKey = routerSet[i];
  //     if(!routerKey.get_write_lock_bit()){
  //       // only unlock the locked router-items
  //       continue;
  //     } else {
  //       auto tableId = routerKey.get_table_id();
  //       auto partitionId = routerKey.get_partition_id();
  //       auto table = db.find_table(tableId, partitionId);
  //       auto key = routerKey.get_key();
            
  //       if(partitioner.is_dynamic()){
  //         // unlock dynamic replica
  //         auto coordinatorID = routerKey.get_dynamic_coordinator_id();// partitioner.master_coordinator(tableId, partitionId, key);
  //         auto router_table = db.find_router_table(tableId, coordinatorID);
  //         std::atomic<uint64_t> &tid = router_table->search_metadata(key);
  //         HelperType::unlock(tid);
  //       }
  //     }
  //   }
  //   return true;
  // }

  // bool lock_router_set(TransactionType &txn){
  //   auto &routerSet = txn.routerSet;
  //   for (auto i = 0u; i < routerSet.size(); i++) {
  //     auto &routerKey = routerSet[i];
  //     auto tableId = routerKey.get_table_id();
  //     auto partitionId = routerKey.get_partition_id();
  //     auto table = db.find_table(tableId, partitionId);
  //     auto key = routerKey.get_key();
          
  //     if(partitioner.is_dynamic()){
  //       // unlock dynamic replica
  //       auto coordinatorID = routerKey.get_dynamic_coordinator_id();// partitioner.master_coordinator(tableId, partitionId, key);
        
  //       auto router_table = db.find_router_table(tableId, coordinatorID);
  //       std::atomic<uint64_t> &tid = router_table->search_metadata(key);
  //       bool success = true;
  //       HelperType::lock(tid, success);
  //       if(success == false){
  //         return false;
  //       } else {
  //         routerKey.set_write_lock_bit();
  //         routerKey.set_tid(tid);
  //       }
  //     }
      
  //   }
  //   return true;
  // }

  bool commit(TransactionType &txn,
              std::vector<std::unique_ptr<Message>> &messages,
              std::atomic<uint32_t> &async_message_num) {

    // lock write set
    if (lock_write_set(txn, messages)) {
      abort(txn, messages);
      return false;
    }

    // commit phase 2, read validation
    if (!validate_read_set(txn, messages)) {
      abort(txn, messages);
      return false;
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
        uint64_t latestTid = HelperType::lock(tid, success);

        if (!success) {
          txn.abort_lock = true;
          break;
        }

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
        // DCHECK(false);
        txn.pendingResponses++;
        txn.network_size += MessageFactoryType::new_lock_message(
            *messages[coordinatorID], *table, writeKey.get_key(), i);
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
        uint64_t latest_tid = table->search_metadata(key).load();
        if (HelperType::remove_lock_bit(latest_tid) != tid) {
          txn.abort_read_validation = true;
          break;
        }
        if (HelperType::is_locked(latest_tid)) { // must be locked by others
          txn.abort_read_validation = true;
          break;
        }
      } else {
        // remote 
        txn.pendingResponses++;
        txn.network_size += MessageFactoryType::new_read_validation_message(
            *messages[coordinatorID], *table, key, i, tid);
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
    auto &writeSet = txn.writeSet;

    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto key = writeKey.get_key();

      // lock dynamic replica
      auto coordinatorID = writeKey.get_dynamic_coordinator_id(); // partitioner.master_coordinator(tableId, partitionId, key);
      // auto router_table = db.find_router_table(tableId, coordinatorID);
      // std::atomic<uint64_t> &tid_ = router_table->search_metadata(key);

      // uint64_t last_tid_ = HelperType::lock(tid_);
      // DCHECK(last_tid_ < commit_tid);
      // LOG(INFO) << "LOCK " << *(int*)key;
      
      // write
      if (coordinatorID == context.coordinator_id) {
        
        auto value = writeKey.get_value();
        table->update(key, value);
      } else {
        // DCHECK(false);
        txn.pendingResponses++;
        txn.network_size += MessageFactoryType::new_write_message(
            *messages[coordinatorID], *table, writeKey.get_key(),
            writeKey.get_value());
      }

      // value replicate

      std::size_t replicate_count = 0;
      
      bool send_replica = false;

      for (auto k = 0u; k < partitioner.total_coordinators(); k++) {
        
        // k does not have this partition
        if (!partitioner.is_partition_replicated_on(tableId, partitionId, key, k)) {
          continue;
        }

        // already write
        if (k == coordinatorID) {
          continue;
        }

        replicate_count++;

        // local replicate
        if (k == txn.coordinator_id) {
          auto value = writeKey.get_value();
          std::atomic<uint64_t> &tid = table->search_metadata(key);

          uint64_t last_tid = HelperType::lock(tid);
          // DCHECK(last_tid < commit_tid);
          table->update(key, value);
          HelperType::unlock(tid, commit_tid);

        } else {
          // txn.pendingResponses++;
          auto coordinatorID = k;
          txn.network_size += MessageFactoryType::new_replication_message(
              *messages[coordinatorID], *table, writeKey.get_key(),
              writeKey.get_value(), commit_tid);
          async_message_num.fetch_add(1);
          send_replica = true;
        }
      }
      
      if(send_replica == false){
        txn.network_size += MessageFactoryType::ignore_message(
              *messages[coordinatorID], *table, writeKey.get_key(),
              writeKey.get_value(), commit_tid);
      }
      // DCHECK(replicate_count == partitioner.replica_num() - 1);
    }

    sync_messages(txn, false);
  }

  void release_lock(TransactionType &txn, uint64_t commit_tid,
                    std::vector<std::unique_ptr<Message>> &messages) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto key = writeKey.get_key();

      auto coordinatorID = writeKey.get_dynamic_coordinator_id(); // partitioner.master_coordinator(tableId, partitionId, key);
      // write
      if (coordinatorID == context.coordinator_id) {
        
        auto value = writeKey.get_value();
        std::atomic<uint64_t> &tid = table->search_metadata(key);
        table->update(key, value);
        HelperType::unlock(tid, commit_tid);
      } else {
        txn.network_size += MessageFactoryType::new_release_lock_message(
            *messages[coordinatorID], *table, writeKey.get_key(), commit_tid);
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
  //     //   HelperType::unlock_if_locked(tid);
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
