//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "common/Message.h"
#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Lion/LionRWKey.h"
#include <limits.h>

#include <chrono>
#include <glog/logging.h>
#include <vector>

namespace star {

class LionTransaction {
public:
  using MetaDataType = std::atomic<uint64_t>;

  LionTransaction(std::size_t coordinator_id, std::size_t partition_id,
                  Partitioner &partitioner)
      : coordinator_id(coordinator_id), partition_id(partition_id),
        startTime(std::chrono::steady_clock::now()), partitioner(partitioner) {
    reset();
  }

  virtual ~LionTransaction() = default;

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

    routerSet.clear(); // add by truth 22-03-25
  }
  virtual TransactionResult prepare_read_execute(std::size_t worker_id) = 0;
  virtual TransactionResult read_execute(std::size_t worker_id, ReadMethods local_read_only) = 0;
  virtual TransactionResult prepare_update_execute(std::size_t worker_id) = 0;

  virtual TransactionResult execute(std::size_t worker_id) = 0;

  // virtual TransactionResult local_execute(std::size_t worker_id) = 0;



  virtual void reset_query() = 0;

  virtual const std::vector<u_int64_t> get_query() = 0;
  virtual const std::vector<bool> get_query_update() = 0;

    virtual std::set<int> txn_nodes_involved(bool is_dynamic) = 0;
  virtual std::unordered_map<int, int> txn_nodes_involved(int& max_node, bool is_dynamic) = 0;
  virtual bool check_cross_node_txn(bool is_dynamic) = 0;
  virtual std::size_t get_partition_id() = 0;

  template <class KeyType, class ValueType>
  void search_local_index(std::size_t table_id, std::size_t partition_id,
                          const KeyType &key, ValueType &value) {
    LionRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_local_index_read_bit(); // only difference between 'search_for_read'
    readKey.set_read_request_bit();

    add_to_read_set(readKey);
    // add by truth 22-03-25
    add_to_router_set(readKey);
    
  }

  template <class KeyType, class ValueType>
  void search_for_read(std::size_t table_id, std::size_t partition_id,
                       const KeyType &key, ValueType &value) {

    LionRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_request_bit();

    add_to_read_set(readKey);
    
    // add by truth 22-03-25
    add_to_router_set(readKey);

  }

  template <class KeyType, class ValueType>
  void search_for_update(std::size_t table_id, std::size_t partition_id,
                         const KeyType &key, ValueType &value) {

    LionRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);
    
    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_request_bit();
    readKey.set_write_lock_bit();

    add_to_read_set(readKey);

    // add by truth 22-03-25
    add_to_router_set(readKey);

  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value) {
    LionRWKey writeKey;

    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);

    writeKey.set_write_lock_bit();

    writeKey.set_key(&key);
    // the object pointed by value will not be updated
    writeKey.set_value(const_cast<ValueType *>(&value));

    add_to_write_set(writeKey);
  }

  bool process_requests(std::size_t worker_id) {
    /**
     * @brief get content for read set？
     * 
     * @param i 
     */
    bool success = true;
    // 
    tids.resize(readSet.size(), nullptr);
    // cannot use unsigned type in reverse iteration
    for (int i = int(readSet.size()) - 1; i >= 0; i--) {
      // early return
      if (!readSet[i].get_read_request_bit()) {
        break;
      }

      const LionRWKey &readKey = readSet[i];
      auto tid =
          readRequestHandler(readKey.get_table_id(), readKey.get_partition_id(),
                             i, readKey.get_key(), readKey.get_value(),
                             readKey.get_local_index_read_bit(), success);
      if(success == false){
        break;
      }
      readSet[i].clear_read_request_bit();
      readSet[i].set_tid(tid);
    }

    if (pendingResponses > 0) {
      message_flusher();
      while (pendingResponses > 0) {
        remote_request_handler();
      }
    }
    if(is_abort()){
      success = false;
    }
    if(success == true){
      return false;
    } else {
      return true;
    }
  }


  bool process_read_only_requests(std::size_t worker_id) {
    /**
     * @brief get content for read set？
     * 
     * @param i 
     */
    // cannot use unsigned type in reverse iteration
    for (int i = int(readSet.size()) - 1; i >= 0; i--) {
      // early return
      if (!readSet[i].get_read_request_bit()) {
        break;
      }

      const LionRWKey &readKey = readSet[i];
      auto tid =
          readOnlyRequestHandler(readKey.get_table_id(), readKey.get_partition_id(),
                             i, readKey.get_key(), readKey.get_value(),
                             readKey.get_local_index_read_bit());
      readSet[i].clear_read_request_bit();
      readSet[i].set_tid(tid);
    }

    if (pendingResponses > 0) {
      message_flusher();
      while (pendingResponses > 0) {
        remote_request_handler();
      }
    }
    return false;
  }


  bool process_local_requests(std::size_t worker_id) {
    /**
     * @brief 
     * 
     * @return true  (not local read)
     *         false (local read ok) 
     */
    // cannot use unsigned type in reverse iteration
    for (int i = int(readSet.size()) - 1; i >= 0; i--) {
      // early return
      if (!readSet[i].get_read_request_bit()) {
        break;
      }

      const LionRWKey &readKey = readSet[i];
      auto tid =
          localReadRequestHandler(readKey.get_table_id(), readKey.get_partition_id(),
                             i, readKey.get_key(), readKey.get_value(),
                             readKey.get_local_index_read_bit());
      if(tid == INT_MAX){
        return true;
      } 
      
      routerSet[i].set_write_lock_bit();

      readSet[i].clear_read_request_bit();
      readSet[i].set_tid(tid);

    }
    return false;
  }

  LionRWKey *get_read_key(const void *key) {

    for (auto i = 0u; i < readSet.size(); i++) {
      if (readSet[i].get_key() == key) {
        return &readSet[i];
      }
    }

    return nullptr;
  }

  LionRWKey *get_write_key(const void *key) {

    for (auto i = 0u; i < writeSet.size(); i++) {
      if (writeSet[i].get_key() == key) {
        return &writeSet[i];
      }
    }
    return nullptr;
  }

  std::size_t add_to_read_set(const LionRWKey &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const LionRWKey &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

  std::size_t add_to_router_set(const LionRWKey &key) {
    routerSet.push_back(key);
    return routerSet.size() - 1;
  }

  bool is_abort(){
    return abort_lock || abort_read_validation;
  }
  
public:
  std::size_t coordinator_id, partition_id;
  std::chrono::steady_clock::time_point startTime;
  std::size_t pendingResponses;
  std::vector<std::atomic<uint64_t> *> tids;

  std::size_t network_size;
  bool abort_lock, abort_read_validation, local_validated, si_in_serializable;
  bool distributed_transaction;
  bool execution_phase;
  // table_id, partition_id, key_offset
  // key, value, 
  // local_index_read
  std::function<uint64_t(std::size_t, std::size_t, uint32_t, 
                         const void *, void *, 
                         bool, bool&)>
      readRequestHandler;
  
  std::function<uint64_t(std::size_t, std::size_t, uint32_t, 
                       const void *, void *, 
                       bool)>
    localReadRequestHandler;

  std::function<uint64_t(std::size_t, std::size_t, uint32_t, 
                         const void *, void *, 
                         bool)>
      readOnlyRequestHandler;

  // processed a request?
  std::function<std::size_t(void)> remote_request_handler;

  std::function<void()> message_flusher;

  Partitioner &partitioner;
  Operation operation;
  std::vector<LionRWKey> readSet, writeSet, routerSet;
};

} // namespace star
