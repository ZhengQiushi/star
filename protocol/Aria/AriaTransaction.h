//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Aria/AriaHelper.h"
#include "protocol/Aria/AriaRWKey.h"
#include <chrono>
#include <glog/logging.h>
#include <thread>

namespace star {

class AriaTransaction {

public:
  using MetaDataType = std::atomic<uint64_t>;

  AriaTransaction(std::size_t coordinator_id, std::size_t partition_id,
                  Partitioner &partitioner)
      : coordinator_id(coordinator_id), partition_id(partition_id),
        startTime(std::chrono::steady_clock::now()), partitioner(partitioner) {
    reset();
    b.startTime = this->startTime;
  }

  virtual ~AriaTransaction() = default;

  void reset() {
    abort_lock = false;
    abort_no_retry = false;
    abort_read_validation = false;
    distributed_transaction = false;
    execution_phase = false;
    waw = false;
    war = false;
    raw = false;
    pendingResponses = 0;
    network_size = 0;
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
  virtual void reset_query() = 0;

  virtual const std::vector<u_int64_t> get_query() = 0;
  virtual const std::string get_query_printed() = 0;
  virtual const std::vector<bool> get_query_update() = 0;

  virtual std::set<int> txn_nodes_involved(bool is_dynamic) = 0;
  virtual std::unordered_map<int, int> txn_nodes_involved(int& max_node, bool is_dynamic) = 0;
  virtual bool check_cross_node_txn(bool is_dynamic) = 0;
  virtual std::size_t get_partition_id() = 0;

  template <class KeyType, class ValueType>
  void search_local_index(std::size_t table_id, std::size_t partition_id,
                          const KeyType &key, ValueType &value) {
    if (execution_phase) {
      return;
    }

    AriaRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_local_index_read_bit();
    readKey.set_read_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_read(std::size_t table_id, std::size_t partition_id,
                       const KeyType &key, ValueType &value) {
    if (execution_phase) {
      return;
    }
    AriaRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_update(std::size_t table_id, std::size_t partition_id,
                         const KeyType &key, ValueType &value) {
    if (execution_phase) {
      return;
    }
    AriaRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value) {
    if (execution_phase) {
      return;
    }
    AriaRWKey writeKey;

    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);

    writeKey.set_key(&key);
    // the object pointed by value will not be updated
    writeKey.set_value(const_cast<ValueType *>(&value));

    add_to_write_set(writeKey);
  }

  std::size_t add_to_read_set(const AriaRWKey &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const AriaRWKey &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

  bool process_remaster_requests(std::size_t worker_id) {
    DCHECK(false);
    return true;
  }
  
  bool process_local_requests(std::size_t worker_id){
    // 
    DCHECK(false);
    return false;
  }
  bool process_read_only_requests(std::size_t worker_id){
    // 
    DCHECK(false);
    return false;
  } 

  void set_id(std::size_t id) { this->id = id; }

  void set_tid_offset(std::size_t offset) { this->tid_offset = offset; }

  void set_epoch(uint32_t epoch) { this->epoch = epoch; }

  bool process_requests(std::size_t worker_id) {

    // cannot use unsigned type in reverse iteration
    for (int i = int(readSet.size()) - 1; i >= 0; i--) {
      // early return
      if (!readSet[i].get_read_request_bit()) {
        break;
      }

      AriaRWKey &readKey = readSet[i];
      readRequestHandler(readKey, id, i);
      readSet[i].clear_read_request_bit();
    }

    return false;
  }

  bool is_read_only() { return writeSet.size() == 0; }

public:
  std::size_t coordinator_id, partition_id, id, tid_offset;
  uint32_t epoch;

  std::chrono::steady_clock::time_point startTime;
  // int time_router = 0;
  // int time_scheuler = 0;
  // int time_local_locks = 0;
  // int time_remote_locks = 0;
  // int time_execute = 0;
  // int time_commit = 0;
  // int time_wait4serivce = 0;
  // int time_other_module = 0;
  Breakdown b;

  std::size_t pendingResponses;
  std::size_t network_size;

  bool abort_lock, abort_no_retry, abort_read_validation;
  bool distributed_transaction;
  bool execution_phase;
  bool waw, war, raw;

  // read_key, id, key_offset
  std::function<void(AriaRWKey &, std::size_t, std::size_t)> readRequestHandler;

  // processed a request?
  std::function<std::size_t(void)> remote_request_handler;

  std::function<void()> message_flusher;

  Partitioner &partitioner;
  Operation operation; // never used
  std::vector<AriaRWKey> readSet, writeSet;

  int router_coordinator_id;
  int on_replica_id; // 
  int is_real_distributed;

};
} // namespace star