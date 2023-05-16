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

#include "protocol/Lion/LionRWKey.h"
#include "protocol/Lion/LionTransaction.h"
#include "protocol/Lion/LionManager.h"
#include "protocol/Lion/LionMessage.h"
#include "protocol/Lion/LionMetisMessage.h"
#include <glog/logging.h>

namespace star {

template <class Database> class Lion {
public:
  using DatabaseType = Database;
  // using WorkloadType = Workload;
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

    // std::string debug = "";
    // for (int i = int(readSet.size()) - 1; i >= 0; i--) {
    //   // early return
    //   // if (!readSet[i].get_read_request_bit()) {
    //   //   break;
    //   // }
    //   debug += " " + std::to_string(*(int*)readSet[i].get_key()) + "(" + std::to_string(readSet[i].get_write_lock_bit()) + "_" + std::to_string(readSet[i].get_read_respond_bit()) + ")";
    // }
    // VLOG(DEBUG_V14) << "ABORT DEBUG TXN READ SET " << debug;

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

      std::atomic<uint64_t> &tid = table->search_metadata(key);
      if (coordinatorID == context.coordinator_id) {
        if(readKey.get_write_lock_bit()){
          VLOG(DEBUG_V14) << "  unLOCK-write " << *(int*)key << " " << tid.load();
          TwoPLHelper::write_lock_release(tid);
        } else {
          VLOG(DEBUG_V14) << "  unLOCK-read " << *(int*)key << " " << tid.load();
          TwoPLHelper::read_lock_release(tid);
        }
      } else {
        // DCHECK(false);
        if(readKey.get_write_lock_bit()){
          if(TwoPLHelper::is_write_locked(tid.load())){
            VLOG(DEBUG_V14) << "  unLOCK-write " << *(int*)key << " " << tid.load();
            TwoPLHelper::write_lock_release(tid);
          }
        } else {
          if(TwoPLHelper::is_read_locked(tid.load())){
            VLOG(DEBUG_V14) << "  unLOCK-read " << *(int*)key << " " << tid.load();
            TwoPLHelper::read_lock_release(tid);
          }
        }
      }
    }

    // for (auto i = 0u; i < readSet.size(); i++) {
    //   auto &readKey = readSet[i];
    //   if(!readKey.get_read_respond_bit()){
    //     continue;
    //   }
    //   // only unlock locked records
    //   auto tableId = readKey.get_table_id();
    //   auto partitionId = readKey.get_partition_id();
    //   auto table = db.find_table(tableId, partitionId);
    //   auto key = readKey.get_key();

    //   // remote
    //   auto coordinatorID = readKey.get_dynamic_coordinator_id();

    //   if (coordinatorID == context.coordinator_id) {
    //     std::atomic<uint64_t> &tid = table->search_metadata(key);
    //     if(readKey.get_write_lock_bit()){
    //       TwoPLHelper::write_lock_release(tid);
    //       VLOG(DEBUG_V14) << "  unLOCK-write " << *(int*)key << " " << tid.load();
    //     } else {
    //       TwoPLHelper::read_lock_release(tid);
    //       VLOG(DEBUG_V14) << "  unLOCK-read " << *(int*)key << " " << tid.load();
    //     }
    //   } else {
    //     DCHECK(false);
    //   }
    // }

    // unlock_all_read_locks(txn);
    
    // sync_messages(txn, false);
  }


  bool commit(TransactionType &txn,
              std::vector<std::unique_ptr<Message>> &messages,
              std::atomic<uint32_t> &async_message_num, bool is_metis) {

    if(txn.is_abort()){
      abort(txn, messages);
      return false;
    }

    // generate tid
    uint64_t commit_tid = generate_tid(txn);

    // write and replicate
    if(!is_metis){
      write_and_replicate(txn, commit_tid, messages, async_message_num);
    }

    // release locks
    auto &readSet = txn.readSet;
    DCHECK(txn.tids.size() == readSet.size());

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readKey = readSet[i];
      auto coordinatorID = readKey.get_dynamic_coordinator_id(); // partitioner.master_coordinator(tableId, partitionId, key);
      // write
      auto key = readKey.get_key();

      DCHECK(txn.tids[i] != nullptr && readKey.get_read_respond_bit());
      if (coordinatorID == context.coordinator_id) {
        if(readKey.get_write_lock_bit()){
          VLOG(DEBUG_V14) << "  unLOCK-write " << *(int*)key << " " << *txn.tids[i] << " " << commit_tid << " " << is_metis;
          TwoPLHelper::write_lock_release(*txn.tids[i], commit_tid);
        } else {
          VLOG(DEBUG_V14) << "  unLOCK-read " << *(int*)key << " "  << *txn.tids[i] <<  " " << commit_tid << " " << is_metis;
          TwoPLHelper::read_lock_release(*txn.tids[i]);
        }
      } else {
        if(readKey.get_write_lock_bit()){
          DCHECK(false);
        } else {
          VLOG(DEBUG_V14) << "  unLOCK-read " << *(int*)key << " "  << *txn.tids[i] <<  " " << commit_tid << " " << is_metis;
          TwoPLHelper::read_lock_release(*txn.tids[i]);
        }
      }
    }

    return true;
  }

private:
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
      const RouterValue* const router_val = readKey.get_router_value();

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
        if (!router_val->count_secondary_coordinator_id(k) && k != coordinatorID) {
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
              readKey.get_value(), commit_tid, txn.id);
          
          DCHECK(strlen((char*)readKey.get_value()) > 0);
          // async_message_num.fetch_add(1);
          VLOG(DEBUG_V11) << " async_message_num: " << context.coordinator_id << " -> " << k << " " << async_message_num.load() << " " << *(int*)readKey.get_key() << " " << (char*)readKey.get_value();
          send_replica = true;
        }
      }

      if(send_replica == false && context.coordinator_num > 1){
        DCHECK(false);
      }
      // DCHECK(replicate_count == partitioner.replica_num() - 1);
    }

    // sync_messages(txn, false);
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
