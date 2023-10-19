//
// Created by Yi Lu on 9/11/18.
//

#pragma once

#include "common/Message.h"
#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/TwoPL/TwoPLRWKey.h"
#include <chrono>
#include <glog/logging.h>
#include <vector>

namespace star {
class TwoPLTransaction {

public:
  using MetaDataType = std::atomic<uint64_t>;

  TwoPLTransaction(std::size_t coordinator_id, std::size_t partition_id,
                   Partitioner &partitioner)
      : coordinator_id(coordinator_id), partition_id(partition_id),
        startTime(std::chrono::steady_clock::now()), partitioner(partitioner) {
    reset();
  }

  virtual ~TwoPLTransaction() = default;

  void reset() {
    pendingResponses = 0;
    network_size = 0;
    abort_lock = false;
    abort_read_validation = false;
    local_validated = false;
    si_in_serializable = false;
    distributed_transaction = false;
    execution_phase = true;
    operation.clear();
    readSet.clear();
    writeSet.clear();
  }
  virtual bool is_transmit_requests() = 0;

  virtual ExecutorStatus get_worker_status() = 0;
  virtual TransactionResult prepare_read_execute(std::size_t worker_id) = 0;
  virtual TransactionResult read_execute(std::size_t worker_id, ReadMethods local_read_only) = 0;
  virtual TransactionResult prepare_update_execute(std::size_t worker_id) = 0;

  virtual TransactionResult execute(std::size_t worker_id) = 0;
  
  virtual std::vector<size_t> debug_record_keys() = 0;
  virtual std::vector<size_t> debug_record_keys_master() = 0;
  virtual TransactionResult transmit_execute(std::size_t worker_id) = 0;

  virtual void reset_query() = 0;
  virtual std::string print_raw_query_str() =0;
  virtual const std::vector<u_int64_t> get_query() = 0;
  virtual const std::string get_query_printed() = 0;
  virtual const std::vector<u_int64_t> get_query_master() = 0;
  virtual const std::vector<bool> get_query_update() = 0;

  virtual std::set<int> txn_nodes_involved(bool is_dynamic) = 0;
  virtual std::unordered_map<int, int> txn_nodes_involved(int& max_node, bool is_dynamic) = 0;
  virtual bool check_cross_node_txn(bool is_dynamic) = 0;
  virtual std::size_t get_partition_id() = 0;
  
  template <class KeyType, class ValueType>
  void search_local_index(std::size_t table_id, std::size_t partition_id,
                          const KeyType &key, ValueType &value) {
    TwoPLRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_local_index_read_bit();
    readKey.set_read_lock_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_read(std::size_t table_id, std::size_t partition_id,
                       const KeyType &key, ValueType &value) {

    if (!partitioner.has_master_partition(table_id, partition_id, (void*)& key)) {
      pendingResponses++;
    }

    TwoPLRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_lock_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_update(std::size_t table_id, std::size_t partition_id,
                         const KeyType &key, ValueType &value) {

    if (!partitioner.has_master_partition(table_id, partition_id, (void*)& key)) {
      pendingResponses++;
    }

    TwoPLRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_write_lock_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value) {

    TwoPLRWKey writeKey;

    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);

    writeKey.set_key(&key);
    // the object pointed by value will not be updated
    writeKey.set_value(const_cast<ValueType *>(&value));

    add_to_write_set(writeKey);
  }

  bool process_requests(std::size_t worker_id) {

    // cannot use unsigned type in reverse iteration
    for (int i = int(readSet.size()) - 1; i >= 0; i--) {
      // early return
      if (!readSet[i].get_read_lock_request_bit() &&
          !readSet[i].get_write_lock_request_bit()) {
        break;
      }

      const TwoPLRWKey &readKey = readSet[i];
      bool success, remote;
      auto tid = lock_request_handler(
          readKey.get_table_id(), readKey.get_partition_id(), i,
          readKey.get_key(), readKey.get_value(),
          readSet[i].get_local_index_read_bit(),
          readSet[i].get_write_lock_request_bit(), success, remote);

      if (!remote) {
        if (success) {
          readSet[i].set_tid(tid);
          if (readSet[i].get_read_lock_request_bit() &&
              !readSet[i].get_local_index_read_bit()) {
            readSet[i].set_read_lock_bit();
          }

          if (readSet[i].get_write_lock_request_bit()) {
            readSet[i].set_write_lock_bit();
          }
        } else {
          abort_lock = true;
        }
      }

      readSet[i].clear_read_lock_request_bit();
      readSet[i].clear_write_lock_request_bit();
    }

    if (pendingResponses > 0) {
      message_flusher();
      while (pendingResponses > 0) {
        remote_request_handler();
        std::this_thread::sleep_for(std::chrono::microseconds(5));
      }
    }
    return false;
  }

  bool process_remaster_requests(std::size_t worker_id) {
    DCHECK(false);
    return true;
  }
  
  bool process_migrate_requests(std::size_t worker_id){
    // 
    DCHECK(false);
    return false;
  }
  bool process_read_only_requests(std::size_t worker_id){
    // 
    DCHECK(false);
    return false;
  } 
  TwoPLRWKey *get_read_key(const void *key) {

    for (auto i = 0u; i < readSet.size(); i++) {
      if (readSet[i].get_key() == key) {
        return &readSet[i];
      }
    }

    return nullptr;
  }

  std::size_t add_to_read_set(const TwoPLRWKey &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const TwoPLRWKey &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

public:
  std::size_t coordinator_id, partition_id;
  std::chrono::steady_clock::time_point startTime;
  std::size_t pendingResponses;
  std::size_t network_size;
  bool abort_lock, abort_read_validation, local_validated, si_in_serializable;
  bool distributed_transaction;
  bool execution_phase;
  // bool is_transmit_request;


  // table id, partition id, key, value, local_index_read?, write_lock?,
  // success?, remote?
  std::function<uint64_t(std::size_t, std::size_t, uint32_t, const void *,
                         void *, bool, bool, bool &, bool &)>
      lock_request_handler;
  // processed a request?
  std::function<std::size_t(void)> remote_request_handler;

  std::function<void()> message_flusher;

  Partitioner &partitioner;
  Operation operation;
  std::vector<TwoPLRWKey> readSet, writeSet;
};
} // namespace star