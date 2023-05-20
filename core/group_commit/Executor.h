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

#include <chrono>

namespace star {
namespace group_commit {

template <class Workload, class Protocol> class Executor : public Worker {
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

  Executor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
           const ContextType &context, std::atomic<uint32_t> &worker_status,
           std::atomic<uint32_t> &n_complete_workers,
           std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        partitioner(PartitionerFactory::create_partitioner(
            context.partitioner, coordinator_id, context.coordinator_num)),
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

    messageHandlers = MessageHandlerType::get_message_handlers();
    controlMessageHandlers = ControlMessageHandler<DatabaseType>::get_message_handlers();

    message_stats.resize(messageHandlers.size(), 0);
    message_sizes.resize(messageHandlers.size(), 0);
  }

  void unpack_route_transaction(WorkloadType& workload, StorageType& storage, 
                                // ShareQueue<simpleTransaction>& router_transactions_queue_,
                                std::deque<std::unique_ptr<TransactionType>>& r_transactions_queue_){

    int size_ = router_transactions_queue.size();
    while(size_ > 0){
      size_ -- ;
      // bool is_ok = false;
      simpleTransaction simple_txn = router_transactions_queue.front();
      router_transactions_queue.pop_front();

      // DCHECK(is_ok == true);
      
      n_network_size.fetch_add(simple_txn.size);

      auto p = workload.unpack_transaction(context, 0, storage, simple_txn);
      r_transactions_queue_.push_back(std::move(p));
    }
  }


  // void unpack_route_transaction(WorkloadType& workload, StorageType& storage, 
  //                               ShareQueue<simpleTransaction>& router_transactions_queue_,
  //                               std::deque<std::unique_ptr<TransactionType>>& r_transactions_queue_,
  //                               int router_recv_txn_num, 
  //                               int router_recv_txn_num_indeed){

    
  //   while(!router_transactions_queue_.empty() && router_recv_txn_num > router_recv_txn_num_indeed){
  //     simpleTransaction simple_txn = router_transactions_queue_.front();
  //     router_transactions_queue_.pop_front();
      
  //     n_network_size.fetch_add(simple_txn.size);

  //     auto p = workload.unpack_transaction(context, 0, storage, simple_txn);
  //     r_transactions_queue_.push_back(std::move(p));
  //     router_recv_txn_num -- ;
  //   }
  // }

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
    int time_prepare_read = 0;
    int time_read_remote = 0;
    int time3 = 0;
    auto begin = std::chrono::steady_clock::now();

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
          std::size_t partition_id = transaction->get_partition_id();
          setupHandlers(*transaction);
        }

        auto now = std::chrono::steady_clock::now();

        auto result = transaction->execute(id);

          time_prepare_read += std::chrono::duration_cast<std::chrono::microseconds>(
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

      cur_transactions_queue.pop_front();

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
    LOG(INFO) << total_sec / 1000 / 1000 << " s, " << total_sec / cur_queue_size << " per/micros.";


    if(cur_queue_size > 0){
      LOG(INFO) << time_read_remote << " "<< cur_queue_size  << " prepare: " << time_prepare_read / cur_queue_size << "  execute: " << time_read_remote / cur_queue_size << "  commit: " << time3 / cur_queue_size ;// << "  router : " << time1 / cur_queue_size; 
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

  void start() override {

    LOG(INFO) << "Executor " << id << " starts.";

    StorageType storage;
    uint64_t last_seed = 0;

    // transaction only commit in a single group

    std::queue<std::unique_ptr<TransactionType>> q;
    std::size_t count = 0;
    std::size_t becth_milli_window = 10; // milisecond
    
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

      while (!q.empty()) {
        auto &ptr = q.front();
        auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - ptr->startTime)
                           .count();
        commit_latency.add(latency);
        q.pop();
      }

      n_started_workers.fetch_add(1);
      
      process_request();

      int router_recv_txn_num = 0;
      int router_recv_txn_num_indeed = 0;
      while(!is_router_stopped(router_recv_txn_num)){
        process_request();
        // std::this_thread::sleep_for(std::chrono::microseconds(5));
      }


      VLOG_IF(DEBUG_V, id==0) << "router "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";
              
      unpack_route_transaction(workload, storage, r_transactions_queue); // 

      VLOG_IF(DEBUG_V, id==0) << "unpack "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";

      run_transaction(context, partitioner.get(), r_transactions_queue); // 


      VLOG_IF(DEBUG_V, id==0) << "run transaction "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";

      // unpack_route_transaction(workload, storage, router_transactions_queue, r_transactions_queue); // 

      // VLOG_IF(DEBUG_V, id==0) << r_transactions_queue.size();
      // VLOG_IF(DEBUG_V, id==0) << "prepare_transactions_to_run "
      //         << std::chrono::duration_cast<std::chrono::milliseconds>(
      //                std::chrono::steady_clock::now() - now)
      //                .count()
      //         << " milliseconds.";
      // now = std::chrono::steady_clock::now();

      // size_t r_size = r_transactions_queue.size();
      // run_transaction(context, partitioner.get(), r_transactions_queue); // 
      // for(int r = 0; r < router_recv_txn_num; r ++ ){
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

  virtual void setupHandlers(TransactionType &txn) = 0;

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
      std::function<void(MessagePiece, Message &, DatabaseType &, std::deque<simpleTransaction>*, std::deque<int>* )>>
      controlMessageHandlers;

  std::deque<simpleTransaction> router_transactions_queue;           // router
  std::deque<int> router_stop_queue;           // router stop-SIGNAL
  std::deque<std::unique_ptr<TransactionType>> r_transactions_queue; // to transaction

  std::vector<std::size_t> message_stats, message_sizes;
  LockfreeQueue<Message *, 100860> in_queue, out_queue;
};
} // namespace group_commit

} // namespace star