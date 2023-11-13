//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include "core/Manager.h"
#include "core/Partitioner.h"
// #include "protocol/SiloGC/SiloGCExecutor.h"

#include <thread>
#include <vector>

namespace star {

namespace silogc {

#define MAX_COORDINATOR_NUM 20

struct ScheduleMeta {
  ScheduleMeta(int coordinator_num, int batch_size){
    this->coordinator_num = coordinator_num;
    this->batch_size = 2 * batch_size * coordinator_num;
    for(int i = 0 ; i < coordinator_num; i ++ ){
      node_busy[i] = 0;
    }
    node_txns.resize(this->batch_size);
    
    txns_coord_cost.resize(this->batch_size, std::vector<int>(coordinator_num, 0));
    txn_id.store(0);
    reorder_done.store(false);

    start_schedule.store(0);
    done_schedule.store(0);
  }
  void clear(){
    for(int i = 0 ; i < coordinator_num; i ++ ){
      node_busy[i] = 0;
    }
    node_txns.resize(this->batch_size);
    txn_id.store(0);
    reorder_done.store(false);
    LOG(INFO) << " CLEAR !!!! " << txn_id.load();

    start_schedule.store(0);
    done_schedule.store(0);
  }
  int coordinator_num;
  int batch_size;
  
  std::mutex l;
  std::vector<std::shared_ptr<simpleTransaction>> node_txns;
  std::unordered_map<size_t, int> node_busy;
  std::vector<std::vector<int>> txns_coord_cost;

  std::atomic<uint32_t> txn_id;
  std::atomic<uint32_t> reorder_done;
  ShareQueue<uint32_t> send_txn_id;

  std::atomic<uint32_t> start_schedule;
  std::atomic<uint32_t> done_schedule;


  std::atomic<int> router_transaction_done[20]; // for single-execution
};


template <class Workload> 
struct TransactionMeta {
  using TransactionType = SiloTransaction;
  using WorkloadType = Workload;
  using StorageType = typename WorkloadType::StorageType;
  TransactionMeta(int coordinator_num, int batch_size){
    this->batch_size = batch_size;
    this->coordinator_num = coordinator_num;
    storages.resize(batch_size * coordinator_num * 2);
    transactions_prepared.store(0);
  }
  void clear(){
    s_txn_id.store(0);
    c_txn_id.store(0);
    s_transactions_queue.clear();
    c_transactions_queue.clear();
    transactions_prepared.store(0);
  }

  std::atomic<uint32_t> transactions_prepared;

  ShareQueue<simpleTransaction> router_transactions_queue;
  
  std::atomic<uint32_t> s_txn_id;
  std::atomic<uint32_t> c_txn_id;

  std::vector<std::unique_ptr<TransactionType>> s_transactions_queue;
  std::vector<std::unique_ptr<TransactionType>> c_transactions_queue;

  std::vector<StorageType> storages;
  
  ShareQueue<int> s_txn_id_queue;
  ShareQueue<int> c_txn_id_queue;

  std::mutex s_l;
  std::mutex c_l;

  int batch_size;
  int coordinator_num;
};


} // namespace hermes
} // namespace star