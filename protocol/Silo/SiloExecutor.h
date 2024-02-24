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
// #include "Manager.h"
#include <chrono>
#include "protocol/Silo/SiloMeta.h"

namespace star {
// namespace group_commit {

template <class Workload, class Protocol> class SiloExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using ProtocolType = Protocol;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using TransactionType = typename WorkloadType::TransactionType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using MessageType = typename ProtocolType::MessageType;
  using MessageFactoryType = typename ProtocolType::MessageFactoryType;
  using MessageHandlerType = typename ProtocolType::MessageHandlerType;

  using StorageType = typename WorkloadType::StorageType;

  SiloExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
           const ContextType &context, std::atomic<uint32_t> &worker_status,
           std::atomic<uint32_t> &n_complete_workers,
           std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        txn_meta(context.coordinator_num, context.batch_size),
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

      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
    controlMessageHandlers = ControlMessageHandler<DatabaseType>::get_message_handlers();

    message_stats.resize(messageHandlers.size(), 0);
    message_sizes.resize(messageHandlers.size(), 0);
  }

void unpack_route_transaction(){
    while(true){
      bool success = false;
      simpleTransaction simple_txn = 
        router_transactions_queue.pop_no_wait(success);
      if(!success) break;
      
      n_network_size.fetch_add(simple_txn.size);

      uint32_t txn_id;
      {
        std::unique_ptr<TransactionType> null_txn(nullptr);
        std::lock_guard<std::mutex> l(txn_meta.s_l);
        txn_id = txn_meta.s_transactions_queue.size();
        txn_meta.s_transactions_queue.push_back(std::move(null_txn));
        txn_meta.s_txn_id_queue.push_no_wait(txn_id);
      }
      auto p = workload.unpack_transaction(context, 0, txn_meta.storages[txn_id],simple_txn);
      txn_meta.s_transactions_queue[txn_id] = std::move(p);

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
  }

  void run_transaction(std::vector<std::unique_ptr<TransactionType>>& cur_txns,
                          ShareQueue<int>& txn_id_queue) {
    /**
     * @brief 
     * @note modified by truth 22-01-24
     *       
    */
    uint64_t last_seed = 0;
    int cur_queue_size = txn_id_queue.size();
    for(;;) {
      bool success = false;
      auto i = txn_id_queue.pop_no_wait(success);
      if(!success){
        break;
      }
      if(i >= cur_txns.size() || cur_txns[i].get() == nullptr){
        // DCHECK(false) << i << " " << cur_trans.size();
        continue;
      }
      // fetch new transaction
      ControlMessageFactory::router_transaction_response_message(*(messages[context.coordinator_num]));
      flush_messages(messages);
      // 
      VLOG(DEBUG_V16) << " txn: " << i;
      auto now = std::chrono::steady_clock::now();
      bool retry_transaction = false;
      count += 1;
      transaction = std::move(cur_txns[i]);
      transaction->startTime = std::chrono::steady_clock::now();;
      auto txnStartTime = transaction->startTime 
                        = transaction->b.startTime
                        = std::chrono::steady_clock::now();


      do {
        process_request();
        last_seed = random.get_seed();

        if (retry_transaction) {
          transaction->reset();
        } else {
          std::size_t partition_id = transaction->get_partition_id();
          setupHandlers(*transaction);
        }
          //#####
          int before_prepare = std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - transaction->b.startTime)
              .count();
          time_before_prepare_set += before_prepare;
          now = std::chrono::steady_clock::now();
          transaction->b.time_wait4serivce += before_prepare;
          //#####

          transaction->prepare_read_execute(id);

          //#####
          int prepare_read = std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - transaction->b.startTime)
              .count();

          time_prepare_read += prepare_read;
          now = std::chrono::steady_clock::now();
          transaction->b.time_local_locks += prepare_read;
          //#####
          

          auto result = transaction->read_execute(id, ReadMethods::REMOTE_READ_WITH_TRANSFER);

          // ####
          int remote_read = std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - transaction->b.startTime)
              .count();
          time_read_remote += remote_read;
          now = std::chrono::steady_clock::now();
          transaction->b.time_remote_locks += remote_read;
          // #### 
          

          if(result != TransactionResult::READY_TO_COMMIT){
            retry_transaction = false;
            protocol.abort(*transaction, sync_messages, async_messages);
            n_abort_no_retry.fetch_add(1);
            continue;
          } else {
            result = transaction->prepare_update_execute(id);
            // ####
            int write_time = std::chrono::duration_cast<std::chrono::microseconds>(
                                                                  std::chrono::steady_clock::now() - transaction->b.startTime)
                .count();
            time_read_remote1 += write_time;
            now = std::chrono::steady_clock::now();
            transaction->b.time_execute += write_time;
            // #### 
          }

          time_read_remote += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
          now = std::chrono::steady_clock::now();

        if (result == TransactionResult::READY_TO_COMMIT) {
          // // LOG(INFO) << "StarExecutor: "<< id << " " << "commit" << i;

          bool commit = protocol.commit(*transaction, sync_messages, async_messages);
          n_network_size.fetch_add(transaction->network_size);
          if (commit) {
            n_commit.fetch_add(1);
            retry_transaction = false;

              if(transaction->distributed_transaction > 0){
                distributed_num.fetch_add(1);
                // LOG(INFO) << distributed_num.load();
              } else {
                singled_num.fetch_add(1);
              }

              // ####
              int commit_time = std::chrono::duration_cast<std::chrono::microseconds>(
                                                                    std::chrono::steady_clock::now() - transaction->b.startTime)
                  .count();
              transaction->b.time_commit += commit_time;
              // ####
              record_commit_transactions(*transaction);

          } else {
            if (transaction->abort_lock) {
              n_abort_lock.fetch_add(1);
            } else {
              DCHECK(transaction->abort_read_validation);
              n_abort_read_validation.fetch_add(1);
            }
            random.set_seed(last_seed);
            retry_transaction = true;
          }
        } else {
          protocol.abort(*transaction, sync_messages, async_messages);
          n_abort_no_retry.fetch_add(1);
        }

        time3 += std::chrono::duration_cast<std::chrono::microseconds>(
                                                              std::chrono::steady_clock::now() - now)
            .count();
        now = std::chrono::steady_clock::now();

      } while (retry_transaction);


      if (i % context.batch_flush == 0) {
        flush_async_messages();
        flush_sync_messages();
      }
    }
    flush_async_messages();
    flush_sync_messages();

    if(cur_queue_size > 500)
      LOG(INFO) << "cur_queue_size: " << cur_queue_size;
  }


  void start() override {

    LOG(INFO) << "Executor " << id << " starts.";

    StorageType storage;
    uint64_t last_seed = 0;

    ExecutorStatus status;

    while ((status = static_cast<ExecutorStatus>(worker_status.load())) !=
           ExecutorStatus::START) {
      std::this_thread::yield();
    }

    n_started_workers.fetch_add(1);
    bool retry_transaction = false;

    auto begin = std::chrono::steady_clock::now();
    int print_per_second = 3;
    int cur_time_second = 0;
    do {
      
      process_request();

      unpack_route_transaction(); // 

      run_transaction(txn_meta.s_transactions_queue,
                      txn_meta.s_txn_id_queue); // 

      auto total_sec = std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::steady_clock::now() - begin)
                        .count();
      
      if(total_sec % print_per_second == 0 && count > 0 && cur_time_second != total_sec){

        LOG(INFO) << "  nums: "    << count 
                  << " pre: "     << time_before_prepare_request / count
                  << " set: "     << time_before_prepare_set / count 
                  << " gap: "     << time_before_prepare_read / count
                  << "  prepare: " << time_prepare_read / count  << " " << time_prepare_read
                  << "  execute: " << time_read_remote / count   << " " << time_read_remote
                  << " execute1: " << time_read_remote1 / count 
                  << "  commit: "  << time3 / count              << " " << time3
                  << "  router : " << time1 / count              << " " << time1
                  << print_per_second * 1000 * 1000 / count << " per/micros.";

        time_prepare_read = 0;
        time_before_prepare_set = 0;
        time_before_prepare_read = 0;
        time_before_prepare_request = 0;
        time_read_remote = 0;
        time_read_remote1 = 0;
        time1 = 0;
        time2 = 0;
        time3 = 0;
        time4 = 0;
        count = 0u;

        cur_time_second = total_sec;
      }
      txn_meta.clear();
      status = static_cast<ExecutorStatus>(worker_status.load());
    } while (status != ExecutorStatus::STOP);

    n_complete_workers.fetch_add(1);

    // once all workers are stop, we need to process the replication
    // requests

    while (static_cast<ExecutorStatus>(worker_status.load()) !=
           ExecutorStatus::CLEANUP) {
      process_request();
      std::this_thread::sleep_for(std::chrono::microseconds(5));
    }

    process_request();
    n_complete_workers.fetch_add(1);

    LOG(INFO) << "Executor " << id << " exits.";
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

  void push_message(Message *message) override { 
    // MessagePiece messagePiece = *(message->begin());
    // auto message_type =
    // static_cast<int>(messagePiece.get_message_type());
    // for (auto it = message->begin(); it != message->end(); it++) {
    //   auto messagePiece = *it;
    //   auto message_type = messagePiece.get_message_type();
    //   //!TODO replica 
    //   if(message_type == static_cast<int>(SiloGCMessage::REPLICATION_RESPONSE)){


    //     // async_message_respond_num.fetch_add(1); // "async_message_respond_num : " << async_message_respond_num.load()
    //     LOG(INFO) << " get RESPONSE from " << message->get_source_node_id() << " to " << message->get_dest_node_id(); // << " " << debug_key;
    //   } else if(message_type == static_cast<int>(SiloGCMessage::REPLICATION_REQUEST)){
    //     auto message_length = messagePiece.get_message_length();
    //     int debug_key;
    //     auto stringPiece = messagePiece.toStringPiece();
    //     Decoder dec(stringPiece);
    //     dec >> debug_key;

    //     // async_message_respond_num.fetch_add(1); // "async_message_respond_num : " << async_message_respond_num.load()
    //     LOG(INFO) << " get REPLICATION_REQUEST from " << message->get_source_node_id() << " to " << message->get_dest_node_id() << " " << debug_key;
    //   }
    // }
    in_queue.push(message); 
  }

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
                              *async_messages[message->get_source_node_id()], *table,
                              transaction.get());
        }
        // messageHandlers[type](messagePiece,
        //                       *sync_messages[message->get_source_node_id()],
        //                       *table, transaction.get());
        message_stats[type]++;
        message_sizes[type] += messagePiece.get_message_length();
      }

      size += message->get_message_count();
      flush_async_messages();
    }
    return size;
  }

  // virtual void setupHandlers(TransactionType &txn) = 0;

  void setupHandlers(TransactionType &txn){

    txn.readRequestHandler =
        [this, &txn](std::size_t table_id, std::size_t partition_id,
                     uint32_t key_offset, const void *key, void *value,
                     bool local_index_read) -> uint64_t {
      bool local_read = false;

      if (this->partitioner->has_master_partition(table_id, partition_id, key)) {
        local_read = true;
      }

      if (local_index_read || local_read) {
        return this->protocol.search(table_id, partition_id, key, value);
      } else {
        ITable *table = this->db.find_table(table_id, partition_id);
        auto coordinatorID =
            this->partitioner->master_coordinator(table_id, partition_id, key);
        txn.network_size += MessageFactoryType::new_search_message(
            *(this->sync_messages[coordinatorID]), *table, key, key_offset);
        txn.distributed_transaction = true;
        txn.pendingResponses++;
        return 0;
      }
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
  silo::TransactionMeta<WorkloadType> txn_meta;

  StorageType storage;
  std::unique_ptr<Partitioner> partitioner;
  RandomType random;
  ProtocolType protocol;
  WorkloadType workload;
  std::unique_ptr<Delay> delay;
  Percentile<int64_t> commit_latency, write_latency;
  Percentile<int64_t> dist_latency, local_latency;
  std::unique_ptr<TransactionType> transaction;
  std::vector<std::unique_ptr<Message>> sync_messages, async_messages, messages;
  std::vector<
      std::function<void(MessagePiece, Message &, ITable &, TransactionType *)>>
      messageHandlers;

  std::vector<
      std::function<void(MessagePiece, Message &, DatabaseType &, ShareQueue<simpleTransaction>*, std::deque<int>* )>>
      controlMessageHandlers;

  ShareQueue<simpleTransaction> router_transactions_queue;           // router
  std::deque<int> router_stop_queue;           // router stop-SIGNAL
  std::deque<std::unique_ptr<TransactionType>> r_transactions_queue; // to transaction

  std::vector<std::size_t> message_stats, message_sizes;
  LockfreeQueue<Message *, 100860> in_queue, out_queue;


  int time_prepare_read = 0;  
  int time_before_prepare_set = 0;
  int time_before_prepare_read = 0;  
  int time_before_prepare_request = 0;
  int time_read_remote = 0;
  int time_read_remote1 = 0;
  int time1 = 0;
  int time2 = 0;
  int time3 = 0;
  int time4 = 0;
  int count = 0;

};
// } // namespace group_commit

} // namespace star