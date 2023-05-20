//
// Created by Yi Lu on 9/10/18.
//

#pragma once

#include "common/Percentile.h"
#include "core/ControlMessage.h"
#include "core/Defs.h"
#include "core/Delay.h"
#include "core/Partitioner.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "common/ShareQueue.h"
#include "protocol/MyClay/MyClayMessage.h"

#include <chrono>

namespace star {
template <class Workload> class MyClayExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;
  using TransactionType = SiloTransaction;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = MyClay<DatabaseType>;

  using MessageType = MyClayMessage;
  using MessageFactoryType = MyClayMessageFactory;
  using MessageHandlerType = MyClayMessageHandler<DatabaseType>;

  MyClayExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
           const ContextType &context, std::atomic<uint32_t> &worker_status,
           std::atomic<uint32_t> &n_complete_workers,
           std::atomic<uint32_t> &n_started_workers,
           std::atomic<uint32_t> &transactions_prepared,
           std::vector<std::unique_ptr<TransactionType>> &r_transactions_queue,
           ShareQueue<int, 10086> &txn_id_queue,
           std::vector<StorageType> &storages
           )
      : Worker(coordinator_id, id), db(db), context(context),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        transactions_prepared(transactions_prepared),
        r_transactions_queue(r_transactions_queue),
        txn_id_queue(txn_id_queue), 
        storages(storages),
        partitioner(std::make_unique<LionDynamicPartitioner<Workload> >(
            coordinator_id, context.coordinator_num, db)),
        random(reinterpret_cast<uint64_t>(this)),
        protocol(db, context, *partitioner),
        workload(coordinator_id, worker_status, db, random, *partitioner, start_time),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)) {

    for (auto i = 0u; i <= context.coordinator_num; i++) {
      sync_messages.emplace_back(std::make_unique<Message>());
      init_message(sync_messages[i].get(), i);

      async_messages.emplace_back(std::make_unique<Message>());
      init_message(async_messages[i].get(), i);
    }

    // partitioner_ptr = partitioner.get();

    messageHandlers = MessageHandlerType::get_message_handlers();
    controlMessageHandlers = ControlMessageHandler<DatabaseType>::get_message_handlers();

    message_stats.resize(messageHandlers.size(), 0);
    message_sizes.resize(messageHandlers.size(), 0);
  }


  void unpack_route_transaction(WorkloadType& workload, 
                                int router_recv_txn_num){
    int size_ = router_transactions_queue.size();
    r_transactions_queue.clear();
    int idx = 0;
    while(size_ > 0){ // && router_recv_txn_num > 0
      size_ -- ;
      // bool success = false;
      simpleTransaction simple_txn = router_transactions_queue.front();
      router_transactions_queue.pop_front();

      // DCHECK(success == true);
      
      n_network_size.fetch_add(simple_txn.size);
      auto p = workload.unpack_transaction(context, 0, storages[idx], simple_txn);
      txn_id_queue.push_no_wait(idx);

      idx += 1;

      r_transactions_queue.push_back(std::move(p));
      router_recv_txn_num -- ;
    }
  }

    void run_transaction(const ContextType& phase_context,
                         Partitioner *partitioner,
                         std::vector<std::unique_ptr<TransactionType>> & cur_txns) {
    /**
     * @brief 
     * @note modified by truth 22-01-24
     *       
    */
    ProtocolType protocol(db, phase_context, *partitioner);
    WorkloadType workload(coordinator_id, worker_status, db, random, *partitioner, start_time);

    uint64_t last_seed = 0;

    // int time1 = 0;
    int time_before_prepare_set = 0;
    int time_prepare_read = 0;
    int time_read_remote = 0;
    int time3 = 0;
    auto begin = std::chrono::steady_clock::now();

    std::vector<int> why(30, 0);

    auto i = 0u;
    size_t cur_queue_size = cur_txns.size(); 
    int count = 0;   
    // for (auto i = id; i < cur_queue_size; i += context.worker_num) {
    for(;;) {
      bool success = false;
      i = txn_id_queue.pop_no_wait(success);
      if(!success){
        break;
      }
      if(i >= cur_txns.size() || cur_txns[i].get() == nullptr){
        // DCHECK(false) << i << " " << cur_trans.size();
        continue;
      }
      auto now = std::chrono::steady_clock::now();
      bool retry_transaction = false;
      count += 1;
      transaction = std::move(cur_txns[i]);
      transaction->startTime = std::chrono::steady_clock::now();;

      do {
        process_request();
        last_seed = random.get_seed();

          time_before_prepare_set += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();

        if (retry_transaction) {
          transaction->reset();
        } else {
          // std::size_t partition_id = transaction->get_partition_id();
          setupHandlers(*transaction);
        }
        
        
        // auto result = transaction->execute(id);
          transaction->prepare_read_execute(id);

          time_prepare_read += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
          now = std::chrono::steady_clock::now();
          

          // time1 += std::chrono::duration_cast<std::chrono::microseconds>(
          //                                                       std::chrono::steady_clock::now() - now)
          //     .count();
          // now = std::chrono::steady_clock::now();
          
          auto result = transaction->read_execute(id, ReadMethods::REMOTE_READ_WITH_TRANSFER);
          
          if(result != TransactionResult::READY_TO_COMMIT){
            retry_transaction = false;
            protocol.abort(*transaction, sync_messages, async_messages);
            n_abort_no_retry.fetch_add(1);
            continue;
          } else {
            result = transaction->prepare_update_execute(id);
          }
          
          time_read_remote += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
          now = std::chrono::steady_clock::now();


        if(!transaction->is_transmit_requests()){
          if(transaction->distributed_transaction){
            cross_txn_num ++ ;
            if(i < 5){
              LOG(INFO) << "cross_txn_num ++ : " << *(int*)transaction->readSet[0].get_key() << " " << *(int*)transaction->readSet[1].get_key();
            }
          } else {
            single_txn_num ++ ;

            // debug
            // LOG(INFO) << "single_txn_num: " << transaction->get_query_printed();
          }
        } else {
          LOG(INFO) << "transmit txn: " << transaction->get_query_printed();
        }

        if (result == TransactionResult::READY_TO_COMMIT) {
          // // LOG(INFO) << "StarExecutor: "<< id << " " << "commit" << i;

          bool commit = protocol.commit(*transaction, sync_messages, async_messages);
          n_network_size.fetch_add(transaction->network_size);
          if (commit) {
            n_commit.fetch_add(1);
            retry_transaction = false;

            q.push(std::move(transaction));
          } else {
            if (transaction->abort_lock) {
              n_abort_lock.fetch_add(1);
            } else {
              DCHECK(transaction->abort_read_validation);
              n_abort_read_validation.fetch_add(1);
            }
            random.set_seed(last_seed);
            retry_transaction = false;
            protocol.abort(*transaction, sync_messages, async_messages);
            n_abort_no_retry.fetch_add(1);
          }
        } else if(result == TransactionResult::TRANSMIT_REQUEST){
          // pass
          n_migrate.fetch_add(transaction->migrate_cnt);
          n_remaster.fetch_add(transaction->remaster_cnt);
        } else {
          protocol.abort(*transaction, sync_messages, async_messages);
          n_abort_no_retry.fetch_add(1);
        }

          time3 += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
          now = std::chrono::steady_clock::now();


      } while (retry_transaction);

      // cur_txns.pop_front();

      if (i % phase_context.batch_flush == 0) {
        flush_async_messages();
        flush_sync_messages();
      }
    }
    flush_async_messages();
    flush_sync_messages();


    auto total_sec = std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count() * 1.0;
    LOG(INFO) << total_sec / 1000 / 1000 << " s, " << total_sec / count << " per/micros.";


    if(count > 0){
      VLOG(DEBUG_V4) << time_read_remote << " "<< count
      << " set: " << time_before_prepare_set / count 
      << " prepare: " << time_prepare_read / count 
      << " execute: " << time_read_remote / count 
      << " commit: " << time3 / count
      << "\n" 
      << " : "        << why[21] / count << " " << why[21]
      << " : "        << why[22] / count << " " << why[22]
      // << " : "        << why[19] / cur_queue_size << " " << why[19]
      ;// << "  router : " << time1 / cur_queue_size; 
      // LOG(INFO) << "remaster_delay_transactions: " << remaster_delay_transactions;
      // remaster_delay_transactions = 0;
    }
  }
  bool is_router_stopped(int& router_recv_txn_num){
    bool ret = false;
    size_t num = 1; // context.coordinator_num
    if(router_stop_queue.size() < num){
      ret = false;
    } else {
      //
      int i = num; // context.coordinator_num;
      while(i > 0){
        i --;
        DCHECK(router_stop_queue.size() > 0);
        int recv_txn_num = router_stop_queue.front();
        router_stop_queue.pop_front();
        router_recv_txn_num += recv_txn_num;
        VLOG(DEBUG_V8) << " RECV : " << recv_txn_num;
      }
      ret = true;
    }
    return ret;
  }


  void commit_transactions() {
    while (!q.empty()) {
      auto &ptr = q.front();
      auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - ptr->startTime)
                         .count();
      percentile.add(latency);
      q.pop();
    }
  }


  void start() override {

    LOG(INFO) << "Executor " << id << " starts.";


    uint64_t last_seed = 0;

    // transaction only commit in a single group
    std::size_t count = 0;
    // std::size_t becth_milli_window = 10; // milisecond
    
    for (;;) {
      auto begin = std::chrono::steady_clock::now();
      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "Executor " << id << " exits.";
          return;
        }
      } while (status != ExecutorStatus::START);

      commit_transactions();

      // auto now = std::chrono::steady_clock::now();
      
      process_request();

      int router_recv_txn_num = 0;
      VLOG_IF(DEBUG_V, id==0) << "wait for start "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";

      single_txn_num = 0;
      cross_txn_num = 0;

      if (id == 0) {
        while(!is_router_stopped(router_recv_txn_num)){
          process_request();
          std::this_thread::sleep_for(std::chrono::microseconds(5));
        }



        VLOG_IF(DEBUG_V, id==0) << "prepare_transactions_to_run "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - begin)
                      .count()
                << " milliseconds.";
        // now = std::chrono::steady_clock::now();
        unpack_route_transaction(workload, router_recv_txn_num); // 

        transactions_prepared.store(true);
      }

      while(transactions_prepared.load() == false){
        std::this_thread::yield();
        process_request();
      }

      n_started_workers.fetch_add(1);


      size_t r_size = r_transactions_queue.size();
      run_transaction(context, partitioner.get(), r_transactions_queue); // 

      VLOG_IF(DEBUG_V, id==0) << "run_transaction "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";

      LOG(INFO) << "single_txn_num: " << single_txn_num << " " << " cross_txn_num: " << cross_txn_num << " " << r_size;
      // for(size_t r = 0; r < r_size; r ++ ){
      //   // 发回原地...
      //   size_t generator_id = context.coordinator_num;
      //   // LOG(INFO) << static_cast<uint32_t>(ControlMessage::ROUTER_TRANSACTION_RESPONSE) << " -> " << generator_id;
      //   ControlMessageFactory::router_transaction_response_message(*(async_messages[generator_id]));
      //   flush_messages(async_messages);
      // }

      status = static_cast<ExecutorStatus>(worker_status.load());

      flush_async_messages();

      n_complete_workers.fetch_add(1);


      VLOG_IF(DEBUG_V, id==0) << "whole batch "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";

      // once all workers are stop, we need to process the replication
      // requests

      while (static_cast<ExecutorStatus>(worker_status.load()) !=
             ExecutorStatus::CLEANUP) {
        process_request();
      }

      if(id == 0){
        transactions_prepared.store(false);
      }

      VLOG_IF(DEBUG_V, id==0) << "sync "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";

      process_request();
      n_complete_workers.fetch_add(1);
    }
  }

  void onExit() override {

    LOG(INFO) << "Worker " << id << " latency: " << commit_latency.nth(50)
              << " us (50%) " << commit_latency.nth(75) << " us (75%) "
              << commit_latency.nth(95) << " us (95%) "
              << commit_latency.nth(99)
              << " us (99%). write latency: " << write_latency.nth(50)
              << " us (50%) " << write_latency.nth(75) << " us (75%) "
              << write_latency.nth(95) << " us (95%) " << write_latency.nth(99)
              << " us (99%). dist txn latency: " << dist_latency.nth(50)
              << " us (50%) " << dist_latency.nth(75) << " us (75%) "
              << dist_latency.nth(95) << " us (95%) " << dist_latency.nth(99)
              << " us (99%). local txn latency: " << local_latency.nth(50)
              << " us (50%) " << local_latency.nth(75) << " us (75%) "
              << local_latency.nth(95) << " us (95%) " << local_latency.nth(99)
              << " us (99%).";

    if (id == 0) {
      for (auto i = 0u; i < message_stats.size(); i++) {
        LOG(INFO) << "message stats, type: " << i
                  << " count: " << message_stats[i]
                  << " total size: " << message_sizes[i];
      }
      write_latency.save_cdf(context.cdf_path);
    }
  }

  std::size_t get_partition_id() {

    std::size_t partition_id;

    if (context.partitioner == "pb") {
      partition_id = random.uniform_dist(0, context.partition_num - 1);
    } else {
      auto partition_num_per_node =
          context.partition_num / context.coordinator_num;
      partition_id = random.uniform_dist(0, partition_num_per_node - 1) *
                         context.coordinator_num +
                     coordinator_id;
    }
    CHECK(partitioner->has_master_partition(partition_id));
    return partition_id;
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
            *async_messages[message->get_source_node_id()], db,
            &router_transactions_queue,
            &router_stop_queue
          );
        } else {
          messageHandlers[type](messagePiece,
                              *async_messages[message->get_source_node_id()], 
                              db, context, partitioner.get(),
                              transaction.get());
        }
        message_stats[type]++;
        message_sizes[type] += messagePiece.get_message_length();
      }

      size += message->get_message_count();
      flush_async_messages();
    }
    return size;
  }

  void setupHandlers(TransactionType &txn) {

    txn.readRequestHandler =
        [this, &txn](std::size_t table_id, std::size_t partition_id,
                     uint32_t key_offset, const void *key, void *value,
                     bool local_index_read) -> uint64_t {
      bool local_read = false;
      auto &readKey = txn.readSet[key_offset];
      ITable *table = this->db.find_table(table_id, partition_id);
      // master-replica
      size_t coordinatorID = this->partitioner->master_coordinator(table_id, partition_id, key);
      uint64_t coordinator_secondaryIDs = 0; // = context.coordinator_num + 1;
      if(readKey.get_write_request_bit()){
        // write key, the find all its replica
        LionInitPartitioner* tmp = (LionInitPartitioner*)(this->partitioner.get());
        coordinator_secondaryIDs = tmp->secondary_coordinator(table_id, partition_id, key);
      }
      // sec keys replicas
      readKey.set_dynamic_coordinator_id(coordinatorID);
      readKey.set_router_value(coordinatorID, coordinator_secondaryIDs);

      // bool remaster = false;
      if (coordinatorID == context.coordinator_id) {
        // local_read = true;
          // master-replica is at local node 
          bool success = false;
          std::atomic<uint64_t> &tid = table->search_metadata(key, success);
          if(success == false){
            return 0;
          }
          // immediatly lock local record 赶快本地lock
          if(readKey.get_write_request_bit()){
            SiloHelper::lock(tid, success);
            // VLOG(DEBUG_V14) << "LOCK-LOCAL-write " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " tid:" << tid;
          } 
          // 
          // txn.tids[key_offset] = &tid;

          if(success){
            // VLOG(DEBUG_V14) << "LOCK-LOCAL. " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " tid:" << tid ;
            readKey.set_read_respond_bit();
          } else {
            return 0;
          }
          local_read = true;

      } 

      if (local_index_read || local_read) {
        return this->protocol.search(table_id, partition_id, key, value);
      } else {
        
        for(size_t i = 0; i <= context.coordinator_num; i ++ ){ 
          // also send to generator to update the router-table
          if(i == coordinator_id){
            continue; // local
          }
          if(i == coordinatorID){
              VLOG(DEBUG_V14) << "Remote Read : " << *(int*)key << " " << context.coordinator_id << " -> " << coordinatorID;
              txn.network_size += MessageFactoryType::new_search_message(
                  *(this->sync_messages[coordinatorID]), *table, key, key_offset);
              txn.pendingResponses++;
          }       
        }
        txn.distributed_transaction = true;
        return 0;
      }
    };

    txn.remote_request_handler = [this]() { return this->process_request(); };
    txn.message_flusher = [this]() { this->flush_sync_messages(); };
  };

protected:
  void flush_messages(std::vector<std::unique_ptr<Message>> &messages) {
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

  void flush_sync_messages() { flush_messages(sync_messages); }

  void flush_async_messages() { flush_messages(async_messages); }

  void init_message(Message *message, std::size_t dest_node_id) {
    message->set_source_node_id(coordinator_id);
    message->set_dest_node_id(dest_node_id);
    message->set_worker_id(id);
  }

protected:
  DatabaseType &db;
  const ContextType &context;
  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  std::atomic<uint32_t> &transactions_prepared;
  std::vector<std::unique_ptr<TransactionType>>  &r_transactions_queue;
  ShareQueue<int, 10086> &txn_id_queue;
  std::vector<StorageType> &storages;


  std::unique_ptr<Partitioner> partitioner;
  // Partitioner* partitioner_ptr;
  std::mutex mm;
  
  RandomType random;
  ProtocolType protocol;
  WorkloadType workload;
  std::unique_ptr<Delay> delay;
  Percentile<int64_t> commit_latency, write_latency;
  Percentile<int64_t> dist_latency, local_latency;
  std::unique_ptr<TransactionType> transaction;
  std::vector<std::unique_ptr<Message>> sync_messages, async_messages, messages;
  std::vector<
      std::function<void(MessagePiece, Message &, DatabaseType &, const ContextType &,  Partitioner *, TransactionType *)>>
      messageHandlers;

  std::vector<
      std::function<void(MessagePiece, Message &, DatabaseType &, std::deque<simpleTransaction>*, std::deque<int>* )>>
      controlMessageHandlers;


  std::deque<simpleTransaction> router_transactions_queue;
  // ShareQueue<simpleTransaction> router_transactions_queue;           // router
  std::deque<int> router_stop_queue;           // router stop-SIGNAL

  std::vector<std::size_t> message_stats, message_sizes;
  LockfreeQueue<Message *> in_queue, out_queue;

  std::queue<std::unique_ptr<TransactionType>> q;
  Percentile<int64_t> percentile;

  int single_txn_num = 0;
  int cross_txn_num = 0;
};
} // namespace star