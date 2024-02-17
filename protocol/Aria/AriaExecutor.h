//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "core/Partitioner.h"

#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Aria/Aria.h"
#include "protocol/Aria/AriaHelper.h"
#include "protocol/Aria/AriaMessage.h"
#include "protocol/Aria/AriaMeta.h"

#include <chrono>
#include <thread>

namespace star {

template <class Workload> class AriaExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = AriaTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");

  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = Aria<DatabaseType>;

  using MessageType = AriaMessage;
  using MessageFactoryType = AriaMessageFactory;
  using MessageHandlerType = AriaMessageHandler;

  AriaExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
               const ContextType &context,
              //  std::vector<std::unique_ptr<TransactionType>> &transactions,
               std::atomic<uint32_t> &epoch,
               std::atomic<uint32_t> &worker_status,
               std::atomic<uint32_t> &total_abort,
               std::atomic<uint32_t> &n_complete_workers,
               std::atomic<uint32_t> &n_started_workers,
               aria::TransactionMeta<WorkloadType>& txn_meta)
      : Worker(coordinator_id, id), db(db), context(context),
        epoch(epoch),
        worker_status(worker_status), total_abort(total_abort),
        n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        txn_meta(txn_meta),
        partitioner(PartitionerFactory::create_partitioner(
            context.partitioner, coordinator_id, context.coordinator_num)),
        workload(coordinator_id, worker_status, db, random, *partitioner, start_time),
        random(reinterpret_cast<uint64_t>(this)),
        protocol(db, context, *partitioner),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)) {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
    controlMessageHandlers = ControlMessageHandler<DatabaseType>::get_message_handlers();

    replica_num = partitioner->replica_num();
  }

  ~AriaExecutor() = default;


  void unpack_route_transaction(){
    // storages.erase(storages.begin() + n_abort, storages.end());

    // int size_ = router_transactions_queue.size();
    while(true){
      // size_ -- ;
      // bool is_ok = false;
      bool success = false;
      simpleTransaction simple_txn = 
        router_transactions_queue.pop_no_wait(success);
      if(!success) break;
      n_network_size.fetch_add(simple_txn.size);
      // router_planning(&simple_txn);
      size_t txn_id;
      {
        std::unique_ptr<TransactionType> null_txn(nullptr);
        std::lock_guard<std::mutex> l(txn_meta.s_l);
        txn_id = txn_meta.s_transactions_queue.size();
        txn_meta.s_transactions_queue.push_back(std::move(null_txn));
      }
      // LOG(INFO) << txn_id << " " << simple_txn.keys[0] << " " << simple_txn.keys[1];
      // storages.push_back(StorageType());
      auto p = workload.unpack_transaction(context, 0, txn_meta.storages[txn_id], simple_txn);
      p->set_id(txn_id);
      p->on_replica_id = simple_txn.on_replica_id;      
      p->router_coordinator_id = simple_txn.destination_coordinator;
      p->is_real_distributed = simple_txn.is_real_distributed;
      txn_meta.s_transactions_queue[txn_id] = std::move(p);
      if(txn_id >= context.batch_size){
        break;
      }
    }
  }

  bool is_router_stopped(int& router_recv_txn_num){
    bool ret = false;
    if(!router_stop_queue.empty()){
      int recv_txn_num = router_stop_queue.front();
      router_stop_queue.pop_front();
      router_recv_txn_num += recv_txn_num;
      VLOG(DEBUG_V8) << " RECV : " << recv_txn_num;
      ret = true;
    }
    return ret;
  }

  void start() override {

    LOG(INFO) << "AriaExecutor " << id << " started. ";

    for (;;) {
      auto begin = std::chrono::steady_clock::now();
      commit_num = 0;
      if(clear_status.load() == true){
        clear_time_status();
        clear_status.store(false);
      }

      auto cur_time = std::chrono::duration_cast<std::chrono::seconds>(
                     std::chrono::steady_clock::now() - start_time)
                     .count();

      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());
        process_request();
        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "AriaExecutor " << id << " exits. ";

          LOG(INFO) << " router : " << router_percentile.nth(50) << " " <<
          router_percentile.nth(80) << " " << router_percentile.nth(95) << " " << 
          router_percentile.nth(99);

          LOG(INFO) << " execution : " << execute_percentile.nth(50) << " " <<
          execute_percentile.nth(80) << " " << execute_percentile.nth(95) << " " << 
          execute_percentile.nth(99);

          LOG(INFO) << " commit : " << commit_percentile.nth(50) << " " <<
          commit_percentile.nth(80) << " " << commit_percentile.nth(95) << " " << 
          commit_percentile.nth(99);
          return;
        }
      } while (status != ExecutorStatus::Aria_READ);

      n_started_workers.fetch_add(1);
      
      VLOG_IF(DEBUG_V, id==0) << "wait for router stop";

      router_recv_txn_num = 0;
      // 准备transaction
      while(!is_router_stopped(router_recv_txn_num)){
        process_request();
        std::this_thread::sleep_for(std::chrono::microseconds(5));
      }

      auto router_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - begin)
                    .count();
      VLOG_IF(DEBUG_V, id==0) << "Unpack end time: "
              << router_time
              << " milliseconds.";

      router_percentile.add(router_time);
      
      unpack_route_transaction(); // 

      VLOG_IF(DEBUG_V, id==0) << "cur_time : " << cur_time << " unpack_route_transaction : " << txn_meta.s_transactions_queue.size();

      txn_meta.transactions_prepared.fetch_add(1);
      while(txn_meta.transactions_prepared.load() < context.worker_num){
        std::this_thread::yield();
        process_request();
      }

      auto unpack_router_time =std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - begin)
                    .count();
      VLOG_IF(DEBUG_V, id==0) << "Unpack finish time: "
              << unpack_router_time
              << " milliseconds.";

      read_snapshot();
      n_complete_workers.fetch_add(1);


      auto done_snapshot =                std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - begin)
                      .count();
        VLOG(DEBUG_V) << "done_snapshot time: "
                << done_snapshot
                << " milliseconds.";

      // wait to Aria_READ
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Aria_READ) {
        process_request();
        std::this_thread::sleep_for(std::chrono::microseconds(5));
      }



      auto execution_time =                std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - begin)
                      .count();
        VLOG(DEBUG_V) << "Execution time: "
                << execution_time
                << " milliseconds.";

      execute_percentile.add(execution_time);

      process_request();
      n_complete_workers.fetch_add(1);

      // wait till Aria_COMMIT
      while (static_cast<ExecutorStatus>(worker_status.load()) !=
             ExecutorStatus::Aria_COMMIT) {
        std::this_thread::yield();
        process_request();
        std::this_thread::sleep_for(std::chrono::microseconds(5));
      }

      auto start_commit_time =                std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - begin)
                      .count();
        VLOG(DEBUG_V) << "Start commit time: "
                << start_commit_time
                << " milliseconds.";

      n_started_workers.fetch_add(1);
      commit_transactions();
      n_complete_workers.fetch_add(1);
      // wait to Aria_COMMIT
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Aria_COMMIT) {
        process_request();
        std::this_thread::sleep_for(std::chrono::microseconds(5));
      }

      if(id == 0){
        txn_meta.clear();
        auto n_abort = total_abort.load();
        LOG(INFO) << " CLEANUP BATCH : " << n_abort; 
        // erase committed transactions;
        txn_meta.s_transactions_queue.erase(txn_meta.s_transactions_queue.begin() + n_abort, txn_meta.s_transactions_queue.end());
      }

      auto commit_time =                std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - begin)
                      .count();
        VLOG(DEBUG_V) << "Commit time: "
                << commit_time
                << " milliseconds.";

      commit_percentile.add(commit_time);


      LOG(INFO) << router_time << " " << execution_time << " " << commit_time << "\n"
                << "per/txn " << 1.0 * commit_time / commit_num;

      // time_total.add(1.0 * commit_time * 1000 / commit_num);

      process_request();
      n_complete_workers.fetch_add(1);
    }
  }

  std::size_t get_partition_id() {

    std::size_t partition_id;

    CHECK(context.partition_num % context.coordinator_num == 0);

    auto partition_num_per_node =
        context.partition_num / context.coordinator_num;
    partition_id = random.uniform_dist(0, partition_num_per_node - 1) *
                       context.coordinator_num +
                   coordinator_id;
    CHECK(partitioner->has_master_partition(partition_id));
    return partition_id;
  }

  void read_snapshot() {
    // load epoch
    auto cur_epoch = epoch.load();
    auto n_abort = total_abort.load();
    std::size_t count = 0;
    
    auto test = std::chrono::steady_clock::now();
    auto& transactions = txn_meta.s_transactions_queue;
    
    for (auto i = id; i < transactions.size(); i += context.worker_num) {
      // auto wait4service = std::chrono::steady_clock::now();
      

      // if null, generate a new transaction, on this node.
      // else only reset the query
      if(i < n_abort && transactions[i] != nullptr){
        auto replica_id = transactions[i]->on_replica_id;
        auto b_info = transactions[i]->b;
        transactions[i] = workload.reset_transaction(context, 0, txn_meta.storages[i], *transactions[i]);
        transactions[i]->on_replica_id = replica_id;
        transactions[i]->b = b_info;
      }
      if (transactions[i] == nullptr) { // || i >= n_abort
        // pass
        continue;
      } else {
        transactions[i]->reset();
      }
      
      auto txnStartTime = std::chrono::steady_clock::now(); // = transactions[i]->b.startTime

      
      process_request();

      transactions[i]->b.time_wait4serivce += std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - txnStartTime)
                           .count();
      txnStartTime = std::chrono::steady_clock::now();

      transactions[i]->set_epoch(cur_epoch);
      transactions[i]->set_id(i); // tid starts from 1
      transactions[i]->set_tid_offset(i);

      transactions[i]->execution_phase = false;
      setupHandlers(*transactions[i]);

      count++;

      // run transactions

      // LOG(INFO) << "exec: " << i << " " 
      //           << transactions[i]->get_query()[0] << " " 
      //           << transactions[i]->on_replica_id;

      // auto result = transactions[i]->execute(id);

          //#####
          int before_prepare = std::chrono::duration_cast<std::chrono::microseconds>(
                                                                    std::chrono::steady_clock::now() - txnStartTime)
                  .count();
          txnStartTime = std::chrono::steady_clock::now();
          transactions[i]->b.time_wait4serivce += before_prepare;
          // LOG(INFO) << before_prepare;
          //#####
          
          transactions[i]->prepare_read_execute(id);

          //#####
          int prepare_read = std::chrono::duration_cast<std::chrono::microseconds>(
                                                                    std::chrono::steady_clock::now() - txnStartTime)
                  .count();

          txnStartTime = std::chrono::steady_clock::now();
          transactions[i]->b.time_local_locks += prepare_read;
          //#####
          
          auto result = transactions[i]->read_execute(id, ReadMethods::REMOTE_READ_WITH_TRANSFER);
          // ####
          int remote_read = std::chrono::duration_cast<std::chrono::microseconds>(
                                                                    std::chrono::steady_clock::now() - txnStartTime)
                  .count();
          txnStartTime = std::chrono::steady_clock::now();
          transactions[i]->b.time_remote_locks += remote_read;
          // #### 
          
          if(result != TransactionResult::READY_TO_COMMIT){
            // non-active transactions, release lock
          } else {
            result = transactions[i]->prepare_update_execute(id);

            // ####
            int write_time = std::chrono::duration_cast<std::chrono::microseconds>(
                                                                    std::chrono::steady_clock::now() - txnStartTime)
                  .count();
            txnStartTime = std::chrono::steady_clock::now();
            transactions[i]->b.time_execute += write_time;
            // #### 
          }


      n_network_size.fetch_add(transactions[i]->network_size);
      if (result == TransactionResult::ABORT_NORETRY) {
        transactions[i]->abort_no_retry = true;
      }

      if (count % context.batch_flush == 0) {
        flush_messages();
      }
    }
    flush_messages();
    auto execute_time = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - test)
                           .count();
    test = std::chrono::steady_clock::now();
    if(count > 0)
      LOG(INFO) << " snapshot read [" << id << "] : " << count << " " << execute_time / count << " " << execute_time * 1.0 / 1000;

    // reserve
    count = 0;
    for (auto i = id; i < transactions.size(); i += context.worker_num) {
      auto txnStartTime = std::chrono::steady_clock::now();

      if (transactions[i] == nullptr) {
        continue;
      }
      if (transactions[i]->abort_no_retry) {
        continue;
      }

      count++;

      
      // wait till all reads are processed
      while (transactions[i]->pendingResponses > 0) {
        process_request();
      }

      transactions[i]->b.time_remote_locks += 
      std::chrono::duration_cast<std::chrono::microseconds>(
                                std::chrono::steady_clock::now() - txnStartTime)
                                .count();
      txnStartTime = std::chrono::steady_clock::now();


      auto local_locks = std::chrono::steady_clock::now();
      transactions[i]->execution_phase = true;
      // fill in writes in write set
      // transactions[i]->execute(id);

          //#####
          int before_prepare = std::chrono::duration_cast<std::chrono::microseconds>(
                                                                    std::chrono::steady_clock::now() - txnStartTime)
                  .count();
          txnStartTime = std::chrono::steady_clock::now();
          transactions[i]->b.time_wait4serivce += before_prepare;
          // LOG(INFO) << before_prepare;
          //#####
          
          transactions[i]->prepare_read_execute(id);

          //#####
          int prepare_read = std::chrono::duration_cast<std::chrono::microseconds>(
                                                                    std::chrono::steady_clock::now() - txnStartTime)
                  .count();

          // time_prepare_read += prepare_read;
          txnStartTime = std::chrono::steady_clock::now();
          transactions[i]->b.time_local_locks += prepare_read;
          //#####
          
          auto result = transactions[i]->read_execute(id, ReadMethods::REMOTE_READ_WITH_TRANSFER);
          // ####
          int remote_read = std::chrono::duration_cast<std::chrono::microseconds>(
                                                                    std::chrono::steady_clock::now() - txnStartTime)
                  .count();
          // time_read_remote += remote_read;
          txnStartTime = std::chrono::steady_clock::now();
          transactions[i]->b.time_remote_locks += remote_read;
          // #### 
          
          if(result != TransactionResult::READY_TO_COMMIT){
            // non-active transactions, release lock
            continue;
          } else {
            result = transactions[i]->prepare_update_execute(id);

            // ####
            int write_time = std::chrono::duration_cast<std::chrono::microseconds>(
                                                                    std::chrono::steady_clock::now() - txnStartTime)
                  .count();
            // time_read_remote1 += write_time;
            txnStartTime = std::chrono::steady_clock::now();
            transactions[i]->b.time_execute += write_time;
            // #### 
          }

      // transactions[i]->b.time_local_locks+= 
      // std::chrono::duration_cast<std::chrono::microseconds>(
      //                           std::chrono::steady_clock::now() - local_locks)
      //                           .count();
                                
      auto wait_reservation = std::chrono::steady_clock::now();
      // start reservation
      reserve_transaction(*transactions[i]);


      if (count % context.batch_flush == 0) {
        flush_messages();
      }

      transactions[i]->b.time_other_module += 
      std::chrono::duration_cast<std::chrono::microseconds>(
                                std::chrono::steady_clock::now() - wait_reservation)
                                .count();

    }
    flush_messages();

    auto reserve_time = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - test)
                           .count();
    if(count > 0)
      LOG(INFO) << " snapshot reserve [" << id << "] : " << count << " " << reserve_time / count << " " << reserve_time * 1.0 / 1000;
  }

  void reserve_transaction(TransactionType &txn) {

    if (context.aria_read_only_optmization && txn.is_read_only()) {
      return;
    }

    std::vector<AriaRWKey> &readSet = txn.readSet;
    std::vector<AriaRWKey> &writeSet = txn.writeSet;
    auto replicaId = txn.on_replica_id;

    // reserve reads;
    for (std::size_t i = 0u; i < readSet.size(); i++) {
      AriaRWKey &readKey = readSet[i];
      if (readKey.get_local_index_read_bit()) {
        continue;
      }

      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto coordinatorID = partitioner->master_coordinator(tableId, partitionId,
      readKey.get_key(), replicaId);

      // if (partitioner->has_master_partition(partitionId)) {
      if(coordinatorID == coordinator_id) {
        std::atomic<uint64_t> &tid = AriaHelper::get_metadata(table, readKey);
        readKey.set_tid(&tid);
        AriaHelper::reserve_read(tid, txn.epoch, txn.id);
      } else {
        // auto coordinatorID = this->partitioner->master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_reserve_message(
            *(this->messages[coordinatorID]), *table, txn.id, readKey.get_key(),
            txn.epoch, false);
      }
    }

    // reserve writes
    for (std::size_t i = 0u; i < writeSet.size(); i++) {
      AriaRWKey &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto coordinatorID = partitioner->master_coordinator(tableId, partitionId,
      writeKey.get_key(), replicaId);
      if(coordinatorID == coordinator_id) {
      // if (partitioner->has_master_partition(partitionId)) {
        std::atomic<uint64_t> &tid = AriaHelper::get_metadata(table, writeKey);
        writeKey.set_tid(&tid);
        AriaHelper::reserve_write(tid, txn.epoch, txn.id);
      } else {
        // auto coordinatorID = this->partitioner->master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_reserve_message(
            *(this->messages[coordinatorID]), *table, txn.id,
            writeKey.get_key(), txn.epoch, true);
      }
    }
  }

  void analyze_dependency(TransactionType &txn) {

    if (context.aria_read_only_optmization && txn.is_read_only()) {
      return;
    }

    const std::vector<AriaRWKey> &readSet = txn.readSet;
    const std::vector<AriaRWKey> &writeSet = txn.writeSet;
    auto replicaId = txn.on_replica_id;
    // analyze raw
    for (std::size_t i = 0u; i < readSet.size(); i++) {
      const AriaRWKey &readKey = readSet[i];
      if (readKey.get_local_index_read_bit()) {
        continue;
      }

      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      auto coordinatorID = partitioner->master_coordinator(tableId, partitionId,
      readKey.get_key(), replicaId);

      if(coordinatorID == coordinator_id) {
      // if (partitioner->has_master_partition(partitionId)) {
        uint64_t tid = AriaHelper::get_metadata(table, readKey).load();
        uint64_t epoch = AriaHelper::get_epoch(tid);
        uint64_t wts = AriaHelper::get_wts(tid);
        DCHECK(epoch == txn.epoch);
        if (epoch == txn.epoch && wts < txn.id && wts != 0) {
          txn.raw = true;
          break;
        }
      } else {
        // LOG(INFO) << txn.id << " on " << replicaId << " key = " << *(int*)readKey.get_key() << " " << coordinatorID;
        // auto coordinatorID = this->partitioner->master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_check_message(
            *(this->messages[coordinatorID]), *table, txn.id, txn.tid_offset,
            readKey.get_key(), txn.epoch, false);
        txn.pendingResponses++;
      }
    }

    // analyze war and waw

    for (std::size_t i = 0u; i < writeSet.size(); i++) {
      const AriaRWKey &writeKey = writeSet[i];

      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      auto coordinatorID = partitioner->master_coordinator(tableId, partitionId,
      writeKey.get_key(), replicaId);

      if(coordinatorID == coordinator_id) {
      // if (partitioner->has_master_partition(partitionId)) {
        uint64_t tid = AriaHelper::get_metadata(table, writeKey).load();
        uint64_t epoch = AriaHelper::get_epoch(tid);
        uint64_t rts = AriaHelper::get_rts(tid);
        uint64_t wts = AriaHelper::get_wts(tid);
        DCHECK(epoch == txn.epoch);
        if (epoch == txn.epoch && rts < txn.id && rts != 0) {
          txn.war = true;
        }
        if (epoch == txn.epoch && wts < txn.id && wts != 0) {
          txn.waw = true;
        }
      } else {
        // auto coordinatorID = this->partitioner->master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_check_message(
            *(this->messages[coordinatorID]), *table, txn.id, txn.tid_offset,
            writeKey.get_key(), txn.epoch, true);
        txn.pendingResponses++;
      }
    }
  }
  void record_commit_transactions(TransactionType &txn){

    // time_router.add(txn.b.time_router);
    // time_scheuler.add(txn.b.time_scheuler);
    // time_local_locks.add(txn.b.time_local_locks);
    // time_remote_locks.add(txn.b.time_remote_locks);
    // time_execute.add(txn.b.time_execute);
    // time_commit.add(txn.b.time_commit);
    // time_wait4serivce.add(txn.b.time_wait4serivce);
    // time_other_module.add(txn.b.time_other_module);

    txn.b.time_latency = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - txn.b.startTime)
                         .count();

    txn_statics.add(txn.b);

    total_latency.add(std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - txn.startTime)
                         .count());
  }
  void commit_transactions() {
    std::size_t count = 0;

    auto& transactions = txn_meta.s_transactions_queue;
    for (auto i = id; i < transactions.size(); i += context.worker_num) {
      if (transactions[i] == nullptr || transactions[i]->abort_no_retry) {
        continue;
      }

      count++;

      auto analyze_time = std::chrono::steady_clock::now();
      analyze_dependency(*transactions[i]);
      if (count % context.batch_flush == 0) {
        flush_messages();
      }
      transactions[i]->b.time_other_module += std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - analyze_time)
                           .count();
    }
    flush_messages();

    count = 0;
    for (auto i = id; i < transactions.size(); i += context.worker_num) {
      if (transactions[i] == nullptr || transactions[i]->abort_no_retry) {
        n_abort_no_retry.fetch_add(1);
        continue;
      }
      count++;
      auto analyze_time = std::chrono::steady_clock::now();

      // wait till all checks are processed
      while (transactions[i]->pendingResponses > 0) {
        process_request();
      }

      if (context.aria_read_only_optmization &&
          transactions[i]->is_read_only()) {
        if(transactions[i]->on_replica_id == 0)
          n_commit.fetch_add(1);
        auto latency =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - transactions[i]->startTime)
                .count();
        percentile.add(latency);
        continue;
      }

      if (transactions[i]->waw) {
        protocol.abort(*transactions[i], messages);
        if(transactions[i]->on_replica_id == 0)
          n_abort_lock.fetch_add(1);
        continue;
      }

      transactions[i]->b.time_other_module += std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - analyze_time)
                           .count();



      auto commit_time = std::chrono::steady_clock::now();
      bool commit_success = false;

      if (context.aria_snapshot_isolation) {
        protocol.commit(*transactions[i], messages);
        if(transactions[i]->on_replica_id == 0){
          commit_success = true;
        }
        auto latency =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - transactions[i]->startTime)
                .count();
        percentile.add(latency);
      } else {
        if (context.aria_reordering_optmization) {
          if (transactions[i]->war == false || transactions[i]->raw == false) {
            protocol.commit(*transactions[i], messages);
            if(transactions[i]->on_replica_id == 0){
              commit_success = true;
            }
            auto latency =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() -
                    transactions[i]->startTime)
                    .count();
            percentile.add(latency);
          } else {
            if(transactions[i]->on_replica_id == 0)
              n_abort_lock.fetch_add(1);
            protocol.abort(*transactions[i], messages);
          }
        } else {
          if (transactions[i]->raw) {
            if(transactions[i]->on_replica_id == 0)
              n_abort_lock.fetch_add(1);
            protocol.abort(*transactions[i], messages);
          } else {
            protocol.commit(*transactions[i], messages);
            if(transactions[i]->on_replica_id == 0){
              commit_success = true;
            }
            auto latency =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() -
                    transactions[i]->startTime)
                    .count();
            percentile.add(latency);
          }
        }
      }

      if (count % context.batch_flush == 0) {
        flush_messages();
      }
      transactions[i]->b.time_commit += std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - commit_time)
                           .count();

      if(commit_success){
        n_commit.fetch_add(1);
        commit_num += 1;
        record_commit_transactions(*transactions[i]);
      }
    }
    flush_messages();
  }

  void setupHandlers(TransactionType &txn) {

    txn.readRequestHandler = [this, &txn](AriaRWKey &readKey, std::size_t tid,
                                          uint32_t key_offset) {
      auto table_id = readKey.get_table_id();
      auto partition_id = readKey.get_partition_id();
      const void *key = readKey.get_key();
      void *value = readKey.get_value();
      bool local_index_read = readKey.get_local_index_read_bit();
      auto replica_id = txn.on_replica_id;

      bool local_read = false;

      auto coordinatorID = this->partitioner->master_coordinator(table_id, partition_id, readKey.get_key(), replica_id);

      if(coordinatorID == coordinator_id) {
      // if (this->partitioner->has_master_partition(partition_id)) {
        local_read = true;
      }

      ITable *table = db.find_table(table_id, partition_id);
      if (local_read || local_index_read) {
        // set tid meta_data
        auto row = table->search(key);
        AriaHelper::set_key_tid(readKey, row);
        AriaHelper::read(row, value, table->value_size());
      } else {
        // auto coordinatorID =
        //     this->partitioner->master_coordinator(partition_id);
        txn.network_size += MessageFactoryType::new_search_message(
            *(this->messages[coordinatorID]), *table, tid, txn.tid_offset, key,
            key_offset);
        txn.distributed_transaction = true;
        txn.pendingResponses++;
      }
    };

    txn.remote_request_handler = [this]() { return this->process_request(); };
    txn.message_flusher = [this]() { this->flush_messages(); };
  }

  void onExit() override {
    LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50)
              << " us (50%) " << percentile.nth(75) << " us (75%) "
              << percentile.nth(95) << " us (95%) " << percentile.nth(99)
              << " us (99%).";
  }

  void push_message(Message *message) override { in_queue.push(message); }

  Message *pop_message() override {
    if (out_queue.empty())
      return nullptr;

    Message *message = out_queue.front();

    if (delay->delay_enabled()) {
      auto now = std::chrono::steady_clock::now();
      if (std::chrono::duration_cast<std::chrono::microseconds>(now -
                                                                message->time)
              .count() < delay->message_delay()) {
        return nullptr;
      }
    }

    bool ok = out_queue.pop();
    CHECK(ok);

    return message;
  }
  void push_replica_message(Message *message) override { 
    DCHECK(false);
    in_queue.push(message); 
  }
  void flush_messages() {

    for (auto i = 0u; i < messages.size(); i++) {
      if (i == coordinator_id) {
        continue;
      }

      if (messages[i]->get_message_count() == 0) {
        continue;
      }

      auto message = messages[i].release();

      out_queue.push(message);
      messages[i] = std::make_unique<Message>();
      init_message(messages[i].get(), i);
    }
  }

  void init_message(Message *message, std::size_t dest_node_id) {
    message->set_source_node_id(coordinator_id);
    message->set_dest_node_id(dest_node_id);
    message->set_worker_id(id);
  }

  std::size_t process_request() {

    std::size_t size = 0;

    while (!in_queue.empty()) {
      std::unique_ptr<Message> message(in_queue.front());
      bool ok = in_queue.pop();
      CHECK(ok);

      for (auto it = message->begin(); it != message->end(); it++) {

        MessagePiece messagePiece = *it;
        auto type = messagePiece.get_message_type();
        DCHECK(type < messageHandlers.size());
        ITable *table = db.find_table(messagePiece.get_table_id(),
                                      messagePiece.get_partition_id());

        if(type < controlMessageHandlers.size()){
          // transaction router from Generator
          controlMessageHandlers[type](
            messagePiece,
            *messages[message->get_source_node_id()], db,
            &router_transactions_queue, 
            &router_stop_queue
          );
        } else {
          messageHandlers[type](messagePiece,
                                *messages[message->get_source_node_id()], *table,
                                txn_meta.s_transactions_queue);
        }
      }

      size += message->get_message_count();
      flush_messages();
    }
    return size;
  }

private:
  DatabaseType &db;
  const ContextType &context;
  std::atomic<uint32_t> &epoch, &worker_status, &total_abort;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  aria::TransactionMeta<WorkloadType>& txn_meta;

  int commit_num;
  int replica_num;
  std::unique_ptr<Partitioner> partitioner;
  WorkloadType workload;
  RandomType random;
  ProtocolType protocol;
  std::unique_ptr<Delay> delay;
  Percentile<int64_t> percentile;
  std::vector<std::unique_ptr<Message>> messages;
  std::vector<
      std::function<void(MessagePiece, Message &, ITable &,
                         std::vector<std::unique_ptr<TransactionType>> &)>>
      messageHandlers;

  std::vector<
      std::function<void(MessagePiece, Message &, DatabaseType &, ShareQueue<simpleTransaction>* ,std::deque<int>* )>>
      controlMessageHandlers;

  LockfreeQueue<Message *, 100860> in_queue, out_queue;

  ShareQueue<simpleTransaction> router_transactions_queue;
  std::deque<int> router_stop_queue;

  int router_recv_txn_num = 0; // generator router from 

};
} // namespace star