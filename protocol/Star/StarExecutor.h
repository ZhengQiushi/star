//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include "core/Partitioner.h"

#include "common/BufferedFileWriter.h"
#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Star/Star.h"
#include "protocol/Star/StarQueryNum.h"

#include <chrono>
#include <deque>

namespace star {

template <class Workload> class StarExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;
  using TransactionType = typename WorkloadType::TransactionType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = Star<DatabaseType>;

  using MessageType = StarMessage;
  using MessageFactoryType = StarMessageFactory;
  using MessageHandlerType = StarMessageHandler<DatabaseType>;

  StarExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
               const ContextType &context, uint32_t &batch_size,
               std::atomic<uint32_t> &worker_status,
               std::atomic<uint32_t> &n_complete_workers,
               std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        batch_size(batch_size),
        s_partitioner(std::make_unique<StarSPartitioner>(
            coordinator_id, context.coordinator_num)),
        c_partitioner(std::make_unique<StarCPartitioner>(
            coordinator_id, context.coordinator_num)),
        random(reinterpret_cast<uint64_t>(this)), worker_status(worker_status),
        n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)) {

    for (auto i = 0u; i <= context.coordinator_num; i++) {
      sync_messages.emplace_back(std::make_unique<Message>());
      init_message(sync_messages[i].get(), i);

      async_messages.emplace_back(std::make_unique<Message>());
      init_message(async_messages[i].get(), i);

      record_messages.emplace_back(std::make_unique<Message>());
      init_message(record_messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
    controlMessageHandlers = ControlMessageHandler<DatabaseType>::get_message_handlers();

    if (context.log_path != "") {
      std::string filename =
          context.log_path + "_" + std::to_string(id) + ".txt";
      logger = std::make_unique<BufferedFileWriter>(filename.c_str());
    }

    s_context = context.get_single_partition_context();
    c_context = context.get_cross_partition_context();

    // sync responds that need to be received 
    async_message_num.store(0);
    async_message_respond_num.store(0);

    router_transaction_done.store(0);
    router_transactions_send.store(0);
  }
  void trace_txn(){
    if(coordinator_id == 0 && id == 0){
      std::ofstream outfile_excel;
      outfile_excel.open("/Users/lion/project/01_star/star/result_tnx.xls", std::ios::trunc); // ios::trunc

      outfile_excel << "cross_partition_txn" << "\t";
      for(size_t i = 0 ; i < res.size(); i ++ ){
        std::pair<size_t, size_t> cur = res[i];
        outfile_excel << cur.first << "\t";
      }
      outfile_excel << "\n";
      outfile_excel << "single_partition_txn" << "\t";
      for(size_t i = 0 ; i < res.size(); i ++ ){
        std::pair<size_t, size_t> cur = res[i];
        outfile_excel << cur.second << "\t";
      }
      outfile_excel.close();
    }
  }

  void replication_fence(ExecutorStatus status){
    while(async_message_num.load() != async_message_respond_num.load()){
      int a = async_message_num.load();
      int b = async_message_respond_num.load();

      process_request();
      std::this_thread::yield();
    }
    int a = async_message_num.load();
    int b = async_message_respond_num.load();

    async_message_num.store(0);
    async_message_respond_num.store(0);
  }

  void router_fence(){

    while(router_transaction_done.load() != router_transactions_send.load()){
      process_request(); 
    }
    router_transaction_done.store(0);
    router_transactions_send.store(0);
  }

  void unpack_route_transaction(WorkloadType& c_workload, WorkloadType& s_workload, StorageType& storage){
    while(!router_transactions_queue.empty()){
      simpleTransaction simple_txn = router_transactions_queue.front();
      router_transactions_queue.pop_front();
      
      n_network_size.fetch_add(simple_txn.size);

      if(simple_txn.is_distributed){
        auto p = c_workload.unpack_transaction(context, 0, storage, simple_txn);
        c_transactions_queue.push_back(std::move(p));
        c_source_coordinator_ids.push_back(static_cast<uint64_t>(simple_txn.op));
      } else {
        auto p = s_workload.unpack_transaction(context, 0, storage, simple_txn);
        s_transactions_queue.push_back(std::move(p));
        s_source_coordinator_ids.push_back(static_cast<uint64_t>(simple_txn.op));
      }

    }
  }
  
  bool is_router_stopped(){
    bool ret = false;
    if(router_stop_queue.size() < context.coordinator_num){
      ret = false;
    } else {
      //
      int i = context.coordinator_num;
      while(i > 0){
        i --;
        DCHECK(router_stop_queue.size() > 0);
        router_stop_queue.pop_front();
      }
      ret = true;
    }
    return ret;
  }

  void start() override {

    LOG(INFO) << "Executor " << id << " starts.";

    // C-Phase to S-Phase, to C-phase ...

    int times = 0;

    for (;;) {
      auto begin = std::chrono::steady_clock::now();

      ExecutorStatus status;
      times ++ ;

      do {
        status = static_cast<ExecutorStatus>(worker_status.load());
        process_request();
        if (status == ExecutorStatus::EXIT) {
          // commit transaction in s_phase;
          commit_transactions();
          LOG(INFO) << "Executor " << id << " exits.";
          VLOG_IF(DEBUG_V, id==0) << "TIMES : " << times; 
          return;
        }
      } while (status != ExecutorStatus::C_PHASE);

      // commit transaction in s_phase;
      commit_transactions();

      VLOG_IF(DEBUG_V, id==0) << "worker " << id << " prepare_transactions_to_run";

      WorkloadType c_workload = WorkloadType (coordinator_id, db, random, *c_partitioner.get(), start_time);
      WorkloadType s_workload = WorkloadType (coordinator_id, db, random, *s_partitioner.get(), start_time);
      StorageType storage;
      auto now = std::chrono::steady_clock::now();

      VLOG_IF(DEBUG_V, id==0) << "prepare_transactions_to_run "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - now)
                     .count()
              << " milliseconds.";
      now = std::chrono::steady_clock::now();

      // 准备transaction
      while(!is_router_stopped()){ //  && router_transactions_queue.size() < context.batch_size 
        process_request();
        std::this_thread::sleep_for(std::chrono::microseconds(5));
      }

      VLOG_IF(DEBUG_V, id==0) << "prepare_transactions_to_run "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - now)
                     .count()
              << " milliseconds.";
      now = std::chrono::steady_clock::now();

      unpack_route_transaction(c_workload, s_workload, storage); // 

      VLOG_IF(DEBUG_V, id==0) << c_transactions_queue.size() << " " << s_transactions_queue.size();
      VLOG_IF(DEBUG_V, id==0) << "prepare_transactions_to_run "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - now)
                     .count()
              << " milliseconds.";
      now = std::chrono::steady_clock::now();

      // c_phase
      VLOG_IF(DEBUG_V, id==0) << "worker " << id << " c_phase";
      if (coordinator_id == 0) {
        VLOG_IF(DEBUG_V, id==0) << "worker " << id << " ready to run_transaction";
        n_started_workers.fetch_add(1);

        size_t r_size = c_transactions_queue.size();
        // LOG(INFO) << "c_transactions_queue.size() : " <<  r_size;
        run_transaction(ExecutorStatus::C_PHASE, &c_transactions_queue ,async_message_num);
        for(size_t r = 0; r < r_size; r ++ ){
          // 发回原地...
          size_t generator_id = context.coordinator_num;
          // LOG(INFO) << static_cast<uint32_t>(ControlMessage::ROUTER_TRANSACTION_RESPONSE) << " -> " << generator_id;
          ControlMessageFactory::router_transaction_response_message(*(async_messages[generator_id]));
          flush_messages(async_messages);
        }

        n_complete_workers.fetch_add(1);
        VLOG_IF(DEBUG_V, id==0) << "worker " << id << " finish run_transaction";
      } else {
        
        n_started_workers.fetch_add(1);

        VLOG_IF(DEBUG_V, id==0) << "worker " << id << " ready to process_request";

        while (static_cast<ExecutorStatus>(worker_status.load()) ==
               ExecutorStatus::C_PHASE) {
          process_request();
        }
        // process replication request after all workers stop.
        process_request();
        n_complete_workers.fetch_add(1);
        
        VLOG_IF(DEBUG_V, id==0) << "worker " << id << " finish to process_request";
      }


      VLOG_IF(DEBUG_V, id==0) << "C_phase - local "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - now)
                     .count()
              << " milliseconds.";
      now = std::chrono::steady_clock::now();
      // wait to s_phase
      VLOG_IF(DEBUG_V, id==0) << "worker " << id << " wait to s_phase";
      
      while (static_cast<ExecutorStatus>(worker_status.load()) !=
             ExecutorStatus::S_PHASE) {
        process_request(); 
      }

      replication_fence(ExecutorStatus::C_PHASE);

      // commit transaction in c_phase;
      commit_transactions();
      VLOG_IF(DEBUG_V, id==0) << "C_phase router done "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - now)
                     .count()
              << " milliseconds.";
      now = std::chrono::steady_clock::now();

      // s_phase

      n_started_workers.fetch_add(1);
      VLOG_IF(DEBUG_V, id==0) << "worker " << id << " ready to run_transaction";

      size_t r_size = s_transactions_queue.size();
      // LOG(INFO) << "s_transactions_queue.size() : " <<  r_size;
      run_transaction(ExecutorStatus::S_PHASE, &s_transactions_queue, async_message_num);
      for(size_t r = 0; r < r_size; r ++ ){
        // 发回原地...
        size_t generator_id = context.coordinator_num;
        // LOG(INFO) << static_cast<uint32_t>(ControlMessage::ROUTER_TRANSACTION_RESPONSE) << " -> " << generator_id;
        ControlMessageFactory::router_transaction_response_message(*(async_messages[generator_id]));
        flush_messages(async_messages);
      }

      VLOG_IF(DEBUG_V, id==0) << "worker " << id << " ready to replication_fence";

      VLOG_IF(DEBUG_V, id==0) << "S_phase done "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - now)
                     .count()
              << " milliseconds.";
      now = std::chrono::steady_clock::now();
      
      replication_fence(ExecutorStatus::S_PHASE);
      n_complete_workers.fetch_add(1);
      VLOG_IF(DEBUG_V, id==0) << "S_phase fence "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - now)
                     .count()
              << " milliseconds.";
      now = std::chrono::steady_clock::now();

      // once all workers are stop, we need to process the replication
      // requests

      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::S_PHASE) {
        process_request();
      }

      VLOG_IF(DEBUG_V, id==0) << "wait back "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - now)
                     .count()
              << " milliseconds.";
      now = std::chrono::steady_clock::now();


      // n_complete_workers has been cleared
      process_request();
      n_complete_workers.fetch_add(1);



      VLOG_IF(DEBUG_V, id==0) << "whole batch "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";
    }
    VLOG_IF(DEBUG_V, id==0) << "TIMES : " << times; 


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

  std::size_t get_partition_id(ExecutorStatus status) {

    std::size_t partition_id;

    if (status == ExecutorStatus::C_PHASE) {
      // 从当前线程管的分区里随机选一个
      // CHECK(coordinator_id == 0);
      // CHECK(context.partition_num % context.worker_num == 0);
      auto partition_num_per_thread =
          context.partition_num / context.worker_num;
      partition_id = id * partition_num_per_thread +
                     random.uniform_dist(0, partition_num_per_thread - 1);

    } else if (status == ExecutorStatus::S_PHASE) {
      partition_id = id * context.coordinator_num + coordinator_id;
    } else {
      CHECK(false);
    }

    return partition_id;
  }

  void run_transaction(ExecutorStatus status, 
                       std::deque<std::unique_ptr<TransactionType>>* cur_transactions_queue, 
                       std::atomic<uint32_t>& async_message_num) {
    /**
     * @brief 
     * @note modified by truth 22-01-24
     *       
    */
    // std::size_t query_num = 0;

    Partitioner *partitioner = nullptr;

    ContextType phase_context; //  = c_context;

    if(id == 0 && status == ExecutorStatus::S_PHASE){
      // LOG(INFO) << "hi, i'm thread 0";
    }
    if (status == ExecutorStatus::C_PHASE) {
      partitioner = c_partitioner.get();
      // query_num =
      //     StarQueryNum<ContextType>::get_c_phase_query_num(context, batch_size);
      phase_context = context.get_cross_partition_context(); //  c_context; // 


    } else if (status == ExecutorStatus::S_PHASE) {
      partitioner = s_partitioner.get();
      // query_num =
      //     StarQueryNum<ContextType>::get_s_phase_query_num(context, batch_size);
      phase_context = context.get_single_partition_context(); // s_context;// 

    } else {
      CHECK(false);
    }
    
    int time1 = 0;
    int time2 = 0;
    int time3 = 0;
    int time4 = 0;

    ProtocolType protocol(db, phase_context, *partitioner, id);
    WorkloadType workload(coordinator_id, db, random, *partitioner, start_time);

    // StorageType storage;

    uint64_t last_seed = 0;

    auto i = 0u;
    size_t cur_queue_size = cur_transactions_queue->size();
    
    if(id == 0){
      // // LOG(INFO) << "debug";
    }
    // while(!cur_transactions_queue->empty()){ // 为什么不能这样？ 不是太懂
    for (auto i = 0u; i < cur_queue_size; i++) {
      if(cur_transactions_queue->empty()){
        break;
      }
      bool retry_transaction = false;

      transaction =
              std::move(cur_transactions_queue->front());
      transaction->startTime = std::chrono::steady_clock::now();;

      do {
        // // LOG(INFO) << "StarExecutor: "<< id << " " << "process_request" << i;
        process_request();
        last_seed = random.get_seed();

        if (retry_transaction) {
          transaction->reset();
        } else {
          std::size_t partition_id = get_partition_id(status);
          setupHandlers(*transaction, protocol);
        }
        // // LOG(INFO) << "StarExecutor: "<< id << " " << "transaction->execute" << i;

        // auto result = transaction->execute(id);

 auto now = std::chrono::steady_clock::now();

          // auto result = transaction->execute(id);
          transaction->prepare_read_execute(id);

          time4 += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
          now = std::chrono::steady_clock::now();
          

          time1 += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
          now = std::chrono::steady_clock::now();
          
          auto result = transaction->read_execute(id, ReadMethods::REMOTE_READ_WITH_TRANSFER);
          if(result != TransactionResult::READY_TO_COMMIT){
            retry_transaction = false;
            protocol.abort(*transaction);
            n_abort_no_retry.fetch_add(1);
            continue;
          } else {
            result = transaction->prepare_update_execute(id);
          }
          // auto result = transaction->execute(id);
          time2 += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
          now = std::chrono::steady_clock::now();

        if (result == TransactionResult::READY_TO_COMMIT) {
          // // LOG(INFO) << "StarExecutor: "<< id << " " << "commit" << i;

          bool commit =
              protocol.commit(*transaction, sync_messages, async_messages, record_messages, 
                              async_message_num);
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
            retry_transaction = true;
          }
        } else {
          n_abort_no_retry.fetch_add(1);
        }

        time3 += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
        now = std::chrono::steady_clock::now();

      } while (retry_transaction);

      cur_transactions_queue->pop_front();

      flush_sync_messages();
      
      if (i % phase_context.batch_flush == 0) {
        flush_async_messages(); 
        flush_record_messages();
      }
    }
    flush_async_messages();
    flush_record_messages();
    flush_sync_messages();

    if(cur_queue_size > 0)
      VLOG_IF(DEBUG_V4, id == 0) << "prepare: " << time4 / cur_queue_size << "  execute: " << time2 / cur_queue_size << "  commit: " << time3 / cur_queue_size << "  router : " << time1 / cur_queue_size; 

  }

  void onExit() override {
    LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50)
              << " us (50%) " << percentile.nth(75) << " us (75%) "
              << percentile.nth(95) << " us (95%) " << percentile.nth(99)
              << " us (99%).";

    if (logger != nullptr) {
      logger->close();
    }
  }

  void push_message(Message *message) override { 

    // message will only be of type signal, COUNT
    // MessagePiece messagePiece = *(message->begin());

    // auto message_type =
    // static_cast<int>(messagePiece.get_message_type());

      // sync_queue.push(message);
      // // LOG(INFO) << "sync_queue: " << sync_queue.read_available(); 
      for (auto it = message->begin(); it != message->end(); it++) {
        auto messagePiece = *it;
        auto message_type = messagePiece.get_message_type();

        if(message_type == static_cast<int>(StarMessage::SYNC_VALUE_REPLICATION_RESPONSE)){
          auto message_length = messagePiece.get_message_length();
          static int total_async = 0;
          
          // // LOG(INFO) << "recv : " << ++total_async;
          // async_message_num.fetch_sub(1);
          async_message_respond_num.fetch_add(1);
        } else if (message_type == static_cast<int>(StarMessage::ROUTER_TRANSACTION_RESPONSE)){
          static int router_done = 0;
          
          // // LOG(INFO) << "recv : " << ++router_done;
          router_transaction_done.fetch_add(1);
        } else if (message_type == static_cast<int>(StarMessage::ROUTER_TRANSACTION_REQUEST)){
           static int router_recv = 0;
          // // LOG(INFO) << "recv ROUTER_TRANSACTION_REQUEST : " << ++ router_recv;
        }
      }

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

private:
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
                                *sync_messages[message->get_source_node_id()], db,
                                transaction.get(),
                                &router_transactions_queue);
        }

        if (logger) {
          logger->write(messagePiece.toStringPiece().data(),
                        messagePiece.get_message_length());
        }
      }

      size += message->get_message_count();
      flush_sync_messages();
    }
    return size;
  }

  void setupHandlers(TransactionType &txn, ProtocolType &protocol) {
    txn.readRequestHandler =
        [&protocol](std::size_t table_id, std::size_t partition_id,
                    uint32_t key_offset, const void *key, void *value,
                    bool local_index_read) -> uint64_t {
      return protocol.search(table_id, partition_id, key, value);
    };

    txn.remote_request_handler = [this]() { return this->process_request(); };
    txn.message_flusher = [this]() { this->flush_sync_messages(); };
  }

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

  void flush_record_messages() { flush_messages(record_messages); }

  void init_message(Message *message, std::size_t dest_node_id) {
    message->set_source_node_id(coordinator_id);
    message->set_dest_node_id(dest_node_id);
    message->set_worker_id(id);
  }

private:
  DatabaseType &db;
  const ContextType &context;
  uint32_t &batch_size;
  std::unique_ptr<Partitioner> s_partitioner, c_partitioner;
  RandomType random;
  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> async_message_num;

  std::atomic<uint32_t> router_transactions_send, router_transaction_done;

  std::atomic<uint32_t> async_message_respond_num;

  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  std::unique_ptr<Delay> delay;
  std::unique_ptr<BufferedFileWriter> logger;
  Percentile<int64_t> percentile;
  std::unique_ptr<TransactionType> transaction;
  // transaction only commit in a single group
  std::queue<std::unique_ptr<TransactionType>> q;
  std::vector<std::unique_ptr<Message>> sync_messages, async_messages, record_messages;
  std::vector<std::function<void(MessagePiece, Message &, DatabaseType &,
                                 TransactionType *, std::deque<simpleTransaction>*)>>
      messageHandlers;
  LockfreeQueue<Message *> in_queue, out_queue, 
                           sync_queue; // for value sync when phase switching occurs

  std::deque<simpleTransaction> router_transactions_queue;
  std::deque<int> router_stop_queue;

  std::vector<
      std::function<void(MessagePiece, Message &, DatabaseType &, std::deque<simpleTransaction>* ,std::deque<int>* )>>
      controlMessageHandlers;
  // std::unique_ptr<WorkloadType> s_workload, c_workload;

  ContextType s_context, c_context;

  std::deque<std::unique_ptr<TransactionType>> s_transactions_queue, c_transactions_queue, 
                                               r_transactions_queue;
  std::deque<uint64_t> s_source_coordinator_ids, c_source_coordinator_ids;

  std::vector<std::pair<size_t, size_t> > res; // record tnx

};
} // namespace star