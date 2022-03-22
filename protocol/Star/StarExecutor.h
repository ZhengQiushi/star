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
#include <queue>

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

    for (auto i = 0u; i < context.coordinator_num; i++) {
      sync_messages.emplace_back(std::make_unique<Message>());
      init_message(sync_messages[i].get(), i);

      async_messages.emplace_back(std::make_unique<Message>());
      init_message(async_messages[i].get(), i);

      record_messages.emplace_back(std::make_unique<Message>());
      init_message(record_messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();

    if (context.log_path != "") {
      std::string filename =
          context.log_path + "_" + std::to_string(id) + ".txt";
      logger = std::make_unique<BufferedFileWriter>(filename.c_str());
    }

    s_context = context.get_single_partition_context();
    c_context = context.get_cross_partition_context();

    // sync responds that need to be received 
    async_message_num.store(0);
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
      if(status == ExecutorStatus::S_PHASE){
        process_request();
      }
      std::this_thread::yield();
    }
    async_message_num.store(0);
    async_message_respond_num.store(0);
  }
  void start() override {

    LOG(INFO) << "Executor " << id << " starts.";

    // C-Phase to S-Phase, to C-phase ...

    for (;;) {

      ExecutorStatus status;

      do {
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          // commit transaction in s_phase;
          commit_transactions();
          LOG(WARNING) << "Executor " << id << " exits.";
          // debug
          trace_txn();

          return;
        }
      } while (status != ExecutorStatus::C_PHASE);

      // commit transaction in s_phase;
      commit_transactions();

      // LOG(WARNING) << "worker " << id << " prepare_transactions_to_run";

      WorkloadType c_workload = WorkloadType (coordinator_id, db, random, *c_partitioner.get());
      WorkloadType s_workload = WorkloadType (coordinator_id, db, random, *s_partitioner.get());
      StorageType storage;

      // 准备transaction
      prepare_transactions_to_run(c_workload, s_workload, storage);

      // c_phase
      LOG(WARNING) << "worker " << id << " c_phase";
      if (coordinator_id == 0) {
        LOG(WARNING) << "worker " << id << " ready to run_transaction";
        n_started_workers.fetch_add(1);
        run_transaction(ExecutorStatus::C_PHASE, async_message_num);
        
        replication_fence(ExecutorStatus::C_PHASE);
        n_complete_workers.fetch_add(1);
        LOG(WARNING) << "worker " << id << " finish run_transaction";

      } else {
        
        n_started_workers.fetch_add(1);
         LOG(WARNING) << "worker " << id << " ready to process_request";

        while (static_cast<ExecutorStatus>(worker_status.load()) ==
               ExecutorStatus::C_PHASE) {
          process_request();
        }
         LOG(WARNING) << "worker " << id << " finish to process_request";
        // LOG(WARNING) << "worker " << id << " ready to process_request for replication";
        // process replication request after all workers stop.
        process_request();
        n_complete_workers.fetch_add(1);
         LOG(WARNING) << "worker " << id << " finish process_request for replication";
      }

      // wait to s_phase
      LOG(WARNING) << "worker " << id << " wait to s_phase";
      
      while (static_cast<ExecutorStatus>(worker_status.load()) !=
             ExecutorStatus::S_PHASE) {
        std::this_thread::yield();
      }
       LOG(WARNING) << "worker " << id << " s_phase";

      // commit transaction in c_phase;
      commit_transactions();

      // s_phase

      n_started_workers.fetch_add(1);
       LOG(WARNING) << "worker " << id << " ready to run_transaction";
      if(id == 0){
        // LOG(INFO) << "debug";
      }
      run_transaction(ExecutorStatus::S_PHASE, async_message_num);
      
       LOG(WARNING) << "worker " << id << " ready to replication_fence";

      replication_fence(ExecutorStatus::S_PHASE);
      n_complete_workers.fetch_add(1);

       LOG(WARNING) << "worker " << id << " finish run_transaction";

      // once all workers are stop, we need to process the replication
      // requests
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::S_PHASE) {
        process_request();
      }

       LOG(WARNING) << "worker " << id << " finish process_request";

      // n_complete_workers has been cleared
      process_request();
      n_complete_workers.fetch_add(1);
       LOG(WARNING) << "worker " << id << " finish process_request for replication";

    //   if(id == 0){
    //   LOG(INFO) << id << " over prepare_transactions_to_run " << c_transactions_queue.size() << " " << 
    //     s_transactions_queue.size();
    // }
    }


  }

  // bool check_cross_txn(std::unique_ptr<TransactionType>& cur_transaction, bool& success){
  //   /**
  //    * @brief 判断是不是跨分区事务
  //    * @return true/false
  //    */
  //       auto query_keys = cur_transaction->get_query();

  //       int32_t first_key;
  //       size_t first_key_partition_id;
  //       bool is_cross_txn = false;
  //       for (size_t j = 0 ; j < query_keys.size(); j ++ ){
  //         // judge if is cross txn
  //         if(j == 0){
  //           first_key = query_keys[j];
  //           first_key_partition_id = db.getPartitionID(context, first_key);
  //           if(first_key_partition_id == context.partition_num){
  //             success = false;
  //             break;
  //           }
  //         } else {
  //           auto cur_key = query_keys[j];
  //           auto cur_key_partition_id = db.getPartitionID(context, cur_key);
  //           if(cur_key_partition_id == context.partition_num) {
  //             success = false;
  //             break;
  //           }
  //           if(cur_key_partition_id != first_key_partition_id){
  //             is_cross_txn = true;
  //             break;
  //           }
  //         }
  //       }
  //   return is_cross_txn;
  // }

  void prepare_transactions_to_run(WorkloadType& c_workload, WorkloadType& s_workload, StorageType& storage){
    /** 
     * @brief 准备需要的txns
     * @note add by truth 22-01-24
     */
    std::size_t query_num = 0;
    Partitioner *partitioner = nullptr;
    ContextType phase_context; 

    std::vector<ExecutorStatus> cur_status;
    cur_status.push_back(ExecutorStatus::C_PHASE);
    cur_status.push_back(ExecutorStatus::S_PHASE);

    for(int round = 0; round < 2 ; round ++ ){
      // 当前状态, 依次遍历两个phase
      auto status = cur_status[round];

      if (status == ExecutorStatus::C_PHASE) {
        partitioner = c_partitioner.get();
        query_num =
             StarQueryNum<ContextType>::get_c_phase_query_num(context, batch_size);
        phase_context = context.get_cross_partition_context(); 
        if(id == 0){
          // LOG(INFO) << "debug";
        }
      } else if (status == ExecutorStatus::S_PHASE) {
        partitioner = s_partitioner.get();
        query_num =
            StarQueryNum<ContextType>::get_s_phase_query_num(context, batch_size);
        phase_context = context.get_single_partition_context(); 
        if(id == 0){
          // LOG(INFO) << "debug";
        }
      } else {
        CHECK(false);
      }   

      
      LOG(INFO) << sizeof(storage);

      uint64_t last_seed = 0;

      for (auto i = 0u; i < query_num; i++) {
        std::unique_ptr<TransactionType> cur_transaction;

        std::size_t partition_id = get_partition_id(status);
        if (status == ExecutorStatus::C_PHASE) {
          cur_transaction = c_workload.next_transaction(c_context, partition_id, storage);
        } else {
          cur_transaction = s_workload.next_transaction(s_context, partition_id, storage);
        }
        // 甄别一下？
        bool is_success = true;

        bool is_cross_txn = cur_transaction->check_cross_txn(is_success);
        if(is_success){
          if(is_cross_txn && status == ExecutorStatus::S_PHASE){
            LOG(INFO) << "what?";
            bool is_cross_txn = cur_transaction->check_cross_txn(is_success);
          }
        }
        if(is_success){
          if(is_cross_txn){ //cur_status == ExecutorStatus::C_PHASE){
            if (coordinator_id == 0 && status == ExecutorStatus::C_PHASE) {
              // TODO: 暂时不考虑部分副本处理跨分区事务...
              c_transactions_queue.push(std::move(cur_transaction));
            }
          } else {
            s_transactions_queue.push(std::move(cur_transaction));
          }
        }

      } // END FOR
    }
    if(id == 0){
      res.push_back(std::make_pair(c_transactions_queue.size(), s_transactions_queue.size()));
      LOG(INFO) << id << " prepare_transactions_to_run " << c_transactions_queue.size() << " " << 
        s_transactions_queue.size();
    }
    return;
  }
  
  void commit_transactions() {
    /**
     * @brief 
     * 
     */

    while (!q.empty()) {
      auto &ptr = q.front();
      auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - ptr->startTime)
                         .count();
      // if (context.star_sync_in_single_master_phase){
      //   Star<DatabaseType>::sync_messages(*ptr);
      // }
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
      // 
      partition_id = id * context.coordinator_num + coordinator_id;
    } else {
      CHECK(false);
    }

    if(partition_id > 10 && status == ExecutorStatus::S_PHASE){
      LOG(INFO) << "TEST";
    }
    return partition_id;
  }

  void run_transaction(ExecutorStatus status, std::atomic<uint32_t>& async_message_num) {
    /**
     * @brief 
     * @note modified by truth 22-01-24
     *       
    */
    std::queue<std::unique_ptr<TransactionType>>* cur_transactions_queue = nullptr;
    std::size_t query_num = 0;

    Partitioner *partitioner = nullptr;

    ContextType phase_context; //  = c_context;

    if(id == 0 && status == ExecutorStatus::S_PHASE){
      LOG(INFO) << "hi, i'm thread 0";
    }
    if (status == ExecutorStatus::C_PHASE) {
      partitioner = c_partitioner.get();
      query_num =
          StarQueryNum<ContextType>::get_c_phase_query_num(context, batch_size);
      phase_context = context.get_cross_partition_context(); //  c_context; // 

      cur_transactions_queue = &c_transactions_queue;

    } else if (status == ExecutorStatus::S_PHASE) {
      partitioner = s_partitioner.get();
      query_num =
          StarQueryNum<ContextType>::get_s_phase_query_num(context, batch_size);
      phase_context = context.get_single_partition_context(); // s_context;// 

      cur_transactions_queue = &s_transactions_queue;
    } else {
      CHECK(false);
    }

    ProtocolType protocol(db, phase_context, *partitioner, id);
    WorkloadType workload(coordinator_id, db, random, *partitioner);

    // StorageType storage;

    uint64_t last_seed = 0;

    auto i = 0u;
    size_t cur_queue_size = cur_transactions_queue->size();
    
    if(id == 0){
      // LOG(INFO) << "debug";
    }
    // while(!cur_transactions_queue->empty()){ // 为什么不能这样？ 不是太懂
    for (auto i = 0u; i < cur_queue_size; i++) {
      if(cur_transactions_queue->empty()){
        break;
      }
      bool retry_transaction = false;

      transaction =
              std::move(cur_transactions_queue->front());
      do {
        // LOG(INFO) << "StarExecutor: "<< id << " " << "process_request" << i;
        process_request();
        last_seed = random.get_seed();

        if (retry_transaction) {
          transaction->reset();
        } else {
          std::size_t partition_id = get_partition_id(status);
          setupHandlers(*transaction, protocol);
        }
        // LOG(INFO) << "StarExecutor: "<< id << " " << "transaction->execute" << i;

        auto result = transaction->execute(id);

        if (result == TransactionResult::READY_TO_COMMIT) {
          // LOG(INFO) << "StarExecutor: "<< id << " " << "commit" << i;

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
      } while (retry_transaction);

      cur_transactions_queue->pop();

      if (i % phase_context.batch_flush == 0) {
        flush_async_messages(); 
        flush_sync_messages();
        flush_record_messages();
        
      }
    }
    flush_async_messages();
    flush_record_messages();
    flush_sync_messages();
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
      // LOG(INFO) << "sync_queue: " << sync_queue.read_available(); 
      for (auto it = message->begin(); it != message->end(); it++) {
        auto messagePiece = *it;
        auto message_type = messagePiece.get_message_type();

        if(message_type == static_cast<int>(StarMessage::SYNC_VALUE_REPLICATION_RESPONSE)){
          auto message_length = messagePiece.get_message_length();
          static int total_async = 0;
          
          // LOG(INFO) << "recv : " << ++total_async;
          // async_message_num.fetch_sub(1);
          async_message_respond_num.fetch_add(1);
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

        messageHandlers[type](messagePiece,
                              *sync_messages[message->get_source_node_id()], db,
                              transaction.get());
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
                                 TransactionType *)>>
      messageHandlers;
  LockfreeQueue<Message *> in_queue, out_queue, 
                           sync_queue; // for value sync when phase switching occurs

  // std::unique_ptr<WorkloadType> s_workload, c_workload;

  ContextType s_context, c_context;

  std::queue<std::unique_ptr<TransactionType>> s_transactions_queue, c_transactions_queue;

  std::vector<std::pair<size_t, size_t> > res; // record tnx

};
} // namespace star