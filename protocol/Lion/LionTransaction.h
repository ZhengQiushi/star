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
#include "common/Time.h"

#include <chrono>
#include <glog/logging.h>
#include <vector>

namespace star {

class LionTransaction {
public:
  using MetaDataType = std::atomic<uint64_t>;

  LionTransaction(std::size_t coordinator_id, std::size_t partition_id,
                  Partitioner &partitioner, std::size_t ith_replica)
      : coordinator_id(coordinator_id), partition_id(partition_id), ith_replica(ith_replica),
        startTime(std::chrono::steady_clock::now()), partitioner(partitioner) {
    reset();
    b.startTime = startTime;
  }

  virtual ~LionTransaction() = default;

  void reset() {
    pendingResponses = 0;
    asyncPendingResponses = 0;
    network_size = 0;
    abort_lock = false;
    abort_read_validation = false;
    local_validated = false;
    si_in_serializable = false;
    // distributed_transaction = false;
    fully_single_transaction = false;
    execution_phase = true;

    remaster_cnt = 0;
    migrate_cnt = 0;
    
    operation.clear();
    readSet.clear();
    writeSet.clear();

    // routerSet.clear(); // add by truth 22-03-25
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


  virtual int32_t get_partition_count() = 0;

  virtual int32_t get_partition(int i) = 0;

  virtual int32_t get_partition_granule_count(int i) = 0;

  virtual int32_t get_granule(int partition_id, int j) = 0;

  virtual bool is_single_partition() = 0;

  // Which replica this txn runs on
  virtual const std::string serialize(std::size_t ith_replica = 0) = 0;

  virtual void deserialize_lock_status(Decoder & dec) {}

  virtual void serialize_lock_status(Encoder & enc) {}

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
    LionRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_local_index_read_bit(); // only difference between 'search_for_read'
    readKey.set_read_request_bit();

    add_to_read_set(readKey);
    // add by truth 22-03-25
    // add_to_router_set(readKey);
    
  }

  template <class KeyType, class ValueType>
  void search_for_read(std::size_t table_id, std::size_t partition_id,
                       const KeyType &key, ValueType &value,
std::size_t granule_id = 0) {

    LionRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_request_bit();

    add_to_read_set(readKey);
    
    // add by truth 22-03-25
    // add_to_router_set(readKey);

  }

  template <class KeyType, class ValueType>
  void search_for_update(std::size_t table_id, std::size_t partition_id,
                         const KeyType &key, ValueType &value,
std::size_t granule_id = 0) {

    LionRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);
    
    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_request_bit();
    readKey.set_write_request_bit();
    readKey.set_write_lock_bit();

    add_to_read_set(readKey);

    // add by truth 22-03-25
    // add_to_router_set(readKey);

  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value, 
std::size_t granule_id = 0) {
    LionRWKey writeKey;

    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);

    writeKey.set_write_lock_bit();

    writeKey.set_key(&key);
    // the object pointed by value will not be updated
    writeKey.set_value(const_cast<ValueType *>(&value));

    add_to_write_set(writeKey);
  }


  void set_id(uint32_t id){
    this->id = id;
  }
  
  bool process_requests(std::size_t worker_id) {
    /**
     * @brief calling functions inited by handle
     * 
     * @param i 
     */
    bool success = true;
    // 
    tids.resize(readSet.size(), nullptr);
    // std::string debug = "";
    // for (int i = int(readSet.size()) - 1; i >= 0; i--) {
    //   // early return
    //   // if (!readSet[i].get_read_request_bit()) {
    //   //   break;
    //   // }
    //   debug += " " + std::to_string(*(int*)readSet[i].get_key()) + "(" + std::to_string(readSet[i].get_write_lock_bit()) + " " + std::to_string(readSet[i].get_read_respond_bit()) + ")";
    // }
    // VLOG(DEBUG_V14) << "DEBUG TXN READ SET " << debug;
    if(is_abort()){
      success = false;
    }
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
      // LOG(INFO) << "txn.pendingResponses " << id << " " << pendingResponses;
      while (pendingResponses > 0) {
        remote_request_handler();
        // 
        std::this_thread::sleep_for(std::chrono::microseconds(5));

        status = get_worker_status();
        if(status == ExecutorStatus::EXIT){
          LOG(INFO) << "TRANSMITER SHOULD BE STOPPED";
          success = false;
          break;
        }
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

  bool process_remaster_requests(std::size_t worker_id) {
    /**
     * @brief calling functions inited by handle
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
          remasterOnlyReadRequestHandler(readKey.get_table_id(), readKey.get_partition_id(),
                             i, readKey.get_key(), readKey.get_value(),
                             readKey.get_local_index_read_bit(), success);
      if(success == false){
        break;
      }
      readSet[i].clear_read_request_bit();
      readSet[i].set_tid(tid);
    }

    if (pendingResponses > 0) {
      DCHECK(false);
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

  // 



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
        std::this_thread::sleep_for(std::chrono::microseconds(5));

      }
    }
    return false;
  }


  bool process_migrate_requests(std::size_t worker_id) {
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
      
      // routerSet[i].set_write_lock_bit();

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

  // std::size_t add_to_router_set(const LionRWKey &key) {
  //   routerSet.push_back(key);
  //   return routerSet.size() - 1;
  // }

  bool is_abort(){
    return abort_lock || abort_read_validation;
  }

public:
  std::size_t coordinator_id, partition_id;
  std::chrono::steady_clock::time_point startTime;
  std::size_t pendingResponses;
  std::size_t asyncPendingResponses;

  std::vector<std::atomic<uint64_t> *> tids;

  std::size_t network_size;

  int remaster_cnt, migrate_cnt; // statistic

  bool abort_lock, abort_read_validation, local_validated, si_in_serializable;
  bool distributed_transaction;
  bool fully_single_transaction;
  bool execution_phase;
  // bool is_transmit_request;

  // table_id, partition_id, key_offset
  // key, value, 
  // local_index_read
  std::function<uint64_t(std::size_t, std::size_t, uint32_t, 
                         const void *, void *, 
                         bool, bool&)>
      readRequestHandler;
  
    std::function<uint64_t(std::size_t, std::size_t, uint32_t, 
                         const void *, void *, 
                         bool, bool&)>
      remasterOnlyReadRequestHandler;
  
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
  std::vector<LionRWKey> readSet, writeSet; // , routerSet;

  ExecutorStatus status;
  uint32_t id;

  Breakdown b;

  uint64_t txn_random_seed_start = 0;
  uint64_t transaction_id = 0;
  uint64_t straggler_wait_time = 0;
  std::size_t ith_replica;
};

} // namespace star
