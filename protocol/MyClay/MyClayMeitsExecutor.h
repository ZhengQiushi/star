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

#include "protocol/MyClay/MyClayMessage.h"

#include <chrono>

namespace star {
template <class Workload> class MyClayMetisExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;
  using TransactionType = MyClayTransaction;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = MyClay<DatabaseType>;

  using MessageType = MyClayMessage;
  using MessageFactoryType = MyClayMessageFactory;
  using MessageHandlerType = MyClayMessageHandler<DatabaseType>;

  MyClayMetisExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
           const ContextType &context, std::atomic<uint32_t> &worker_status,
           std::atomic<uint32_t> &n_complete_workers,
           std::atomic<uint32_t> &n_started_workers,
           clay::TransactionMeta<WorkloadType>& txn_meta
           )
      : Worker(coordinator_id, id), db(db), context(context),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        txn_meta(txn_meta),
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


  void unpack_route_transaction(WorkloadType& workload, StorageType& storage){
    // int size_ = router_transactions_queue.size();

    // while(size_ > 0){
    //   size_ -- ;
    t_simple_txn.clear();
    
    while(true){
      // size_ -- ;
      // bool is_ok = false;
      bool success = false;
      simpleTransaction simple_txn = 
        router_transactions_queue.pop_no_wait(success);
      if(!success) break;

      // // bool success = false;
      // simpleTransaction simple_txn = router_transactions_queue.front();
      // router_transactions_queue.pop_front();
      // // DCHECK(success == true);
      
      n_network_size.fetch_add(simple_txn.size);
      auto p = workload.unpack_transaction(context, 0, storage, simple_txn, true);
      t_simple_txn.push_back(simple_txn);

      if(simple_txn.is_transmit_request){
        t_transactions_queue.push_back(std::move(p));
      } else {
        DCHECK(false);
      }
      // router_recv_txn_num -- ;
    }
  }

  void retry_transactions(simpleTransaction& simple_txn){
    uint32_t txn_id;
    {
      std::unique_ptr<TransactionType> null_txn(nullptr);
      std::lock_guard<std::mutex> l(txn_meta.t_l);
      txn_id = txn_meta.t_transactions_queue.size();
      if(txn_id >= txn_meta.t_storages.size()){
        DCHECK(false) << txn_id << " " << txn_meta.t_storages.size();
      }
      txn_meta.t_transactions_queue.push_back(std::move(null_txn));
      txn_meta.t_txn_id_queue.push_no_wait(txn_id);
    }
    auto p = workload.unpack_transaction(context, 0, txn_meta.t_storages[txn_id], simple_txn, true);
    txn_meta.t_transactions_queue[txn_id] = std::move(p);
  }

    void run_transaction(const ContextType& phase_context,
                         Partitioner *partitioner,
                         std::deque<std::unique_ptr<TransactionType>>& cur_transactions_queue) {
    /**
     * @brief 
     * @note modified by truth 22-01-24
     *       
    */
    ProtocolType protocol(db, phase_context, *partitioner);
    WorkloadType workload(coordinator_id, worker_status, db, random, *partitioner, start_time);

    uint64_t last_seed = 0;

    int single_txn_num = 0;
    int cross_txn_num = 0;

    auto i = 0u;
    size_t cur_queue_size = cur_transactions_queue.size();    
    for (auto i = 0u; i < cur_queue_size; i++) {
      if(cur_transactions_queue.empty()){
        break;
      }
      bool retry_transaction = false;

      transaction =
              std::move(cur_transactions_queue.front());
      transaction->startTime = std::chrono::steady_clock::now();;

      do {
        process_request();
        last_seed = random.get_seed();

        if (retry_transaction) {
          transaction->reset();
        } else {
          // std::size_t partition_id = transaction->get_partition_id();
          setupHandlers(*transaction);
        }
        auto result = transaction->transmit_execute(id);
        // if(!transaction->is_transmit_requests()){
        //   if(transaction->distributed_transaction){
        //     cross_txn_num ++ ;
            // if(i < 5){
            if(WorkloadType::which_workload == myTestSet::TPCC){
              auto debug = transaction->debug_record_keys();
              auto debug_master = transaction->debug_record_keys_master();

              LOG(INFO) << " new ";
              for(size_t i = 0 ; i < debug.size(); i ++){
                LOG(INFO) << " !!!!! : " << debug[i] << " " << debug_master[i]; 
              }
            }

            // }
        //   } else {
        //     single_txn_num ++ ;

        //     // debug
        //     // LOG(INFO) << "single_txn_num: " << transaction->get_query_printed();
        //   }
        // } else {
          // LOG(INFO) << "transmit txn: " << transaction->get_query_printed();
        // }
        if (result == TransactionResult::READY_TO_COMMIT) {
          // // LOG(INFO) << "StarExecutor: "<< id << " " << "commit" << i;
          DCHECK(false);
        } else if(result == TransactionResult::TRANSMIT_REQUEST){
          // pass
          uint64_t commit_tid = protocol.generate_tid(*transaction);
          protocol.release_lock(*transaction, commit_tid, sync_messages);
          if(!transaction->abort_lock){
            n_commit.fetch_add(1);
            n_migrate.fetch_add(transaction->migrate_cnt);
            n_remaster.fetch_add(transaction->remaster_cnt);
          } else {
            n_abort_lock.fetch_add(1);
            retry_transactions(t_simple_txn[i]);
          }
        } else {
          protocol.abort(*transaction, sync_messages);
          n_abort_no_retry.fetch_add(1);
        }
        n_network_size.fetch_add(transaction->network_size);
        // LOG(INFO) << transaction->network_size;
      } while (retry_transaction);

      cur_transactions_queue.pop_front();

      if (i % phase_context.batch_flush == 0) {
        flush_async_messages();
        flush_sync_messages();
      }
    }
    flush_async_messages();
    flush_sync_messages();


    LOG(INFO) << "cur_queue_size: " << cur_queue_size; //  << " " << " cross_txn_num: " << cross_txn_num;
  }

  // bool is_router_stopped(int& router_recv_txn_num){
  //   bool ret = false;
  //   if(router_stop_queue.size() < context.coordinator_num){
  //     ret = false;
  //   } else {
  //     //
  //     int i = context.coordinator_num;
  //     while(i > 0){
  //       i --;
  //       DCHECK(router_stop_queue.size() > 0);
  //       int recv_txn_num = router_stop_queue.front();
  //       router_stop_queue.pop_front();
  //       router_recv_txn_num += recv_txn_num;
  //       VLOG(DEBUG_V14) << " RECV : " << recv_txn_num << " cur total : " << router_recv_txn_num;
  //     }
  //     ret = true;
  //   }
  //   return ret;
  // }



  void start() override {

    LOG(INFO) << "Executor " << id << " starts.";

    // C-Phase to S-Phase, to C-phase ...
    StorageType storage;
    int times = 0;
    ExecutorStatus status;

    // simpleTransaction t;
    while(status != ExecutorStatus::EXIT){
      status = static_cast<ExecutorStatus>(worker_status.load());
      // process_metis_request();
      process_request();

      auto now = std::chrono::steady_clock::now();

      unpack_route_transaction(workload, storage); // 

      size_t r_size =  t_transactions_queue.size();
      if(r_size <= 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(5));
        continue;
      }
      VLOG_IF(DEBUG_V, id==0) << " " << t_transactions_queue.size();
      VLOG_IF(DEBUG_V, id==0) << "prepare_transactions_to_run "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - now)
                     .count()
              << " milliseconds.";
      

      // run_metis_transaction(ExecutorStatus::C_PHASE);
      run_transaction(context, partitioner.get(), t_transactions_queue); // 
    }
    LOG(INFO) << "transmiter " << " exits.";
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
    txn.lock_request_handler =
        [this, &txn](std::size_t table_id, std::size_t partition_id,
                     uint32_t key_offset, const void *key, void *value,
                     bool local_index_read, bool write_lock, bool &success,
                     bool &remote) -> uint64_t {
      if (local_index_read) {
        success = true;
        remote = false;
        return this->protocol.search(table_id, partition_id, key, value);
      }

      bool local_read = false;
      auto &readKey = txn.readSet[key_offset];
      ITable *table = this->db.find_table(table_id, partition_id);
      size_t coordinatorID = readKey.get_dynamic_coordinator_id();
      // master-replica
      // size_t coordinatorID = this->partitioner->master_coordinator(table_id, partition_id, key);
      // uint64_t coordinator_secondaryIDs = 0; // = context.coordinator_num + 1;
      // if(readKey.get_write_lock_request_bit()){
      //   // write key, the find all its replica
      //   LionInitPartitioner* tmp = (LionInitPartitioner*)(this->partitioner.get());
      //   coordinator_secondaryIDs = tmp->secondary_coordinator(table_id, partition_id, key);
      // }
      // // sec keys replicas
      // readKey.set_dynamic_coordinator_id(coordinatorID);
      // readKey.set_router_value(coordinatorID, coordinator_secondaryIDs);

      bool remaster = false;

      if (coordinatorID == context.coordinator_id) {
        
        remote = false;

        std::atomic<uint64_t> &tid = table->search_metadata(key);

        if (write_lock) {
          TwoPLHelper::write_lock(tid, success);
        } else {
          TwoPLHelper::read_lock(tid, success);
        }

        if (success) {
          return this->protocol.search(table_id, partition_id, key, value);
        } else {
          return 0;
        }

      } else {
        remote = true;

        for(size_t i = 0; i <= context.coordinator_num; i ++ ){ 
          // also send to generator to update the router-table
          if(i == coordinator_id){
            continue; // local
          }
          if(i == coordinatorID){
            // target
              VLOG(DEBUG_V14) << "new_transmit_message : " << *(int*)key << " " << context.coordinator_id << " -> " << coordinatorID;
              txn.network_size += MessageFactoryType::new_transmit_message(
                  *(this->sync_messages[coordinatorID]), *table, key, key_offset, remaster);
              //  txn.pendingResponses++; already added at myclayTransactions
          } else {
              // others, only change the router
              txn.network_size += MessageFactoryType::new_transmit_router_only_message(
                  *(this->sync_messages[i]), *table, key, key_offset);
              txn.pendingResponses++;
          }            
        }

        txn.distributed_transaction = true;
      }
      return 0;
    };
    txn.migrate_request_handler =
        [this, &txn](std::size_t table_id, std::size_t partition_id,
                     uint32_t key_offset, const void *key, void *value,
                     bool local_index_read, bool write_lock, bool &success,
                     bool &remote) -> uint64_t {
      if (local_index_read) {
        success = true;
        remote = false;
        return this->protocol.search(table_id, partition_id, key, value);
      }

      bool local_read = false;
      auto &readKey = txn.readSet[key_offset];
      ITable *table = this->db.find_table(table_id, partition_id);
      size_t coordinatorID = readKey.get_dynamic_coordinator_id();
      // master-replica
      // size_t coordinatorID = this->partitioner->master_coordinator(table_id, partition_id, key);
      // uint64_t coordinator_secondaryIDs = 0; // = context.coordinator_num + 1;
      // if(readKey.get_write_lock_request_bit()){
      //   // write key, the find all its replica
      //   LionInitPartitioner* tmp = (LionInitPartitioner*)(this->partitioner.get());
      //   coordinator_secondaryIDs = tmp->secondary_coordinator(table_id, partition_id, key);
      // }
      // // sec keys replicas
      // readKey.set_dynamic_coordinator_id(coordinatorID);
      // readKey.set_router_value(coordinatorID, coordinator_secondaryIDs);

      bool remaster = false;

      if (coordinatorID == context.coordinator_id) {
        
        remote = false;

        std::atomic<uint64_t> &tid = table->search_metadata(key);

        if (write_lock) {
          TwoPLHelper::write_lock(tid, success);
        } else {
          TwoPLHelper::read_lock(tid, success);
        }

        if (success) {
          return this->protocol.search(table_id, partition_id, key, value);
        } else {
          return 0;
        }

      } else {
        remote = true;

        for(size_t i = 0; i <= context.coordinator_num; i ++ ){ 
          // also send to generator to update the router-table
          if(i == coordinator_id){
            continue; // local
          }
          if(i == coordinatorID){
            // target
              VLOG(DEBUG_V14) << "new_transmit_message : " << *(int*)key << " " << context.coordinator_id << " -> " << coordinatorID;
              txn.network_size += MessageFactoryType::new_transmit_message(
                  *(this->sync_messages[coordinatorID]), *table, key, key_offset, remaster);
              //  txn.pendingResponses++; already added at myclayTransactions
          } else {
              // others, only change the router
              txn.network_size += MessageFactoryType::new_transmit_router_only_message(
                  *(this->sync_messages[i]), *table, key, key_offset);
              txn.pendingResponses++;
          }            
        }

        txn.distributed_transaction = true;
      }
      return 0;
    };

    txn.remote_request_handler = [this]() { return this->process_request(); };
    txn.message_flusher = [this]() { 
      this->flush_sync_messages();
      this->flush_async_messages();
     };
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
  std::unique_ptr<Partitioner> partitioner;
  // Partitioner* partitioner_ptr;
  clay::TransactionMeta<WorkloadType>& txn_meta;

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
      std::function<void(MessagePiece, Message &, DatabaseType &, ShareQueue<simpleTransaction>*, std::deque<int>* )>>
      controlMessageHandlers;

  ShareQueue<simpleTransaction> router_transactions_queue;           // router
  std::deque<int> router_stop_queue;           // router stop-SIGNAL
  std::deque<std::unique_ptr<TransactionType>> t_transactions_queue; // to transaction
  std::vector<simpleTransaction> t_simple_txn;

  std::vector<std::size_t> message_stats, message_sizes;
  LockfreeQueue<Message *> in_queue, out_queue;
};
} // namespace star