//
// Created by Yi Lu on 8/29/18.
//

#pragma once

#include "common/Percentile.h"
#include "common/WALLogger.h"
#include "core/ControlMessage.h"
#include "core/Defs.h"
#include "core/Delay.h"
#include "core/Partitioner.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include <chrono>
#include <thread>

namespace star {

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
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
    controlMessageHandlers = ControlMessageHandler<DatabaseType>::get_message_handlers();

    message_stats.resize(messageHandlers.size(), 0);
    message_sizes.resize(messageHandlers.size(), 0);
  }

  ~Executor() = default;


  void unpack_route_transaction(WorkloadType& workload, StorageType& storage){

    // int size_ = router_transactions_queue.size();
    while(true){
      // size_ -- ;
      // bool is_ok = false;
      bool success = false;
      simpleTransaction simple_txn = 
        router_transactions_queue.pop_no_wait(success);
      if(!success) break;
      //  = router_transactions_queue.front();
      // router_transactions_queue.pop_front();
      // DCHECK(is_ok == true);
      
      n_network_size.fetch_add(simple_txn.size);

      auto p = workload.unpack_transaction(context, 0, storage, simple_txn);
      r_transactions_queue.push_back(std::move(p));
    }
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
        auto result = transaction->execute(id);

        if (result == TransactionResult::READY_TO_COMMIT) {
          // // LOG(INFO) << "StarExecutor: "<< id << " " << "commit" << i;

          bool commit = protocol.commit(*transaction, messages);
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
          protocol.abort(*transaction, messages);
          n_abort_no_retry.fetch_add(1);
        }
      } while (retry_transaction);

      cur_transactions_queue.pop_front();

      if (i % phase_context.batch_flush == 0) {
        flush_messages();
      }
    }
    flush_messages();
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

    do {
      process_request();

      unpack_route_transaction(workload, storage); // 
      if(r_transactions_queue.size() > 0){
        size_t r_size = r_transactions_queue.size();
        run_transaction(context, partitioner.get(), r_transactions_queue); // 
        for(size_t r = 0; r < r_size; r ++ ){
          // 发回原地...
          size_t generator_id = context.coordinator_num;
          ControlMessageFactory::router_transaction_response_message(*(messages[generator_id]));
          flush_messages();
        }
      }

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

    LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50)
              << " us (50%) " << percentile.nth(75) << " us (75%) "
              << percentile.nth(95) << " us (95%) " << percentile.nth(99)
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
      percentile.save_cdf(context.cdf_path);
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

  void push_replica_message(Message *message) override { 
    CHECK(false);
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

  Message *pop_transaction_message() override {
    if (out_transaction_queue.empty())
      return nullptr;

    Message *message = out_transaction_queue.front();

    if (delay->delay_enabled()) {
      auto now = std::chrono::steady_clock::now();
      if (std::chrono::duration_cast<std::chrono::microseconds>(now -
                                                                message->time)
              .count() < delay->message_delay()) {
        return nullptr;
      }
    }

    bool ok = out_transaction_queue.pop();
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
            *messages[message->get_source_node_id()], db,
            &router_transactions_queue,
            &router_stop_queue
          );
        } else {
          messageHandlers[type](messagePiece,
                              *messages[message->get_source_node_id()], *table,
                              transaction.get());
        }
        message_stats[type]++;
        message_sizes[type] += messagePiece.get_message_length();
      }

      size += message->get_message_count();
      flush_messages();
    }
    return size;
  }

  virtual void setupHandlers(TransactionType &txn) = 0;


  virtual void flush_messages() {

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
protected:
  void init_message(Message *message, std::size_t dest_node_id) {
    message->set_source_node_id(coordinator_id);
    message->set_dest_node_id(dest_node_id);
    message->set_worker_id(id);
  }

protected:
  DatabaseType &db;
  ContextType context;
  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  std::unique_ptr<Partitioner> partitioner;
  RandomType random;
  ProtocolType protocol;
  WorkloadType workload;
  std::unique_ptr<Delay> delay;

  Percentile<int64_t> percentile, dist_latency, local_latency, commit_latency; 
  Percentile<uint64_t> local_txn_stall_time_pct, local_txn_commit_work_time_pct, local_txn_commit_persistence_time_pct, local_txn_commit_prepare_time_pct, local_txn_commit_replication_time_pct, local_txn_commit_write_back_time_pct, local_txn_commit_unlock_time_pct, local_txn_local_work_time_pct, local_txn_remote_work_time_pct;
  Percentile<uint64_t> dist_txn_stall_time_pct, dist_txn_commit_work_time_pct, 
  dist_txn_commit_persistence_time_pct, dist_txn_commit_prepare_time_pct,dist_txn_commit_write_back_time_pct, dist_txn_commit_unlock_time_pct, dist_txn_local_work_time_pct, dist_txn_commit_replication_time_pct, dist_txn_remote_work_time_pct;

  std::unique_ptr<TransactionType> transaction;
  std::vector<std::unique_ptr<Message>> messages;
  std::vector<
      std::function<void(MessagePiece, Message &, ITable &, TransactionType *)>>
      messageHandlers;

  std::vector<
      std::function<void(MessagePiece, Message &, DatabaseType &, ShareQueue<simpleTransaction>*, std::deque<int>* )>>
      controlMessageHandlers;

  ShareQueue<simpleTransaction> router_transactions_queue;           // router
  std::deque<int> router_stop_queue;
  std::deque<std::unique_ptr<TransactionType>> r_transactions_queue; // to transaction

  std::vector<std::size_t> message_stats, message_sizes;
  LockfreeQueue<Message *, 100860> in_queue, out_queue, out_transaction_queue;

  // Percentile<int64_t> time_router;
  // Percentile<int64_t> time_scheuler;
  // Percentile<int64_t> time_local_locks;
  // Percentile<int64_t> time_remote_locks;
  // Percentile<int64_t> time_execute;
  // Percentile<int64_t> time_commit;
  // Percentile<int64_t> time_wait4serivce;
  // Percentile<int64_t> time_other_module;

  // Percentile<int64_t> time_total;
  WALLogger * logger = nullptr;

  void record_txn_breakdown_stats(TransactionType & txn) {
    if (txn.is_single_partition()) {
      local_txn_stall_time_pct.add(txn.get_stall_time());
      local_txn_commit_work_time_pct.add(txn.get_commit_work_time());
      local_txn_commit_write_back_time_pct.add(txn.get_commit_write_back_time());
      local_txn_commit_unlock_time_pct.add(txn.get_commit_unlock_time());
      local_txn_local_work_time_pct.add(txn.get_local_work_time());
      local_txn_remote_work_time_pct.add(txn.get_remote_work_time());
      local_txn_commit_persistence_time_pct.add(txn.get_commit_persistence_time());
      local_txn_commit_prepare_time_pct.add(txn.get_commit_prepare_time());
      local_txn_commit_replication_time_pct.add(txn.get_commit_replication_time());
    } else {
      dist_txn_stall_time_pct.add(txn.get_stall_time());
      dist_txn_commit_work_time_pct.add(txn.get_commit_work_time());
      dist_txn_commit_write_back_time_pct.add(txn.get_commit_write_back_time());
      dist_txn_commit_unlock_time_pct.add(txn.get_commit_unlock_time());
      dist_txn_local_work_time_pct.add(txn.get_local_work_time());
      dist_txn_remote_work_time_pct.add(txn.get_remote_work_time());
      dist_txn_commit_persistence_time_pct.add(txn.get_commit_persistence_time());
      dist_txn_commit_prepare_time_pct.add(txn.get_commit_prepare_time());
      dist_txn_commit_replication_time_pct.add(txn.get_commit_replication_time());
    }
  }
};
} // namespace star