//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include "core/Partitioner.h"

#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Calvin/Calvin.h"
#include "protocol/Calvin/CalvinHelper.h"
#include "protocol/Calvin/CalvinMessage.h"

#include <chrono>
#include <thread>

namespace star {

template <class Workload> class CalvinExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = CalvinTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");

  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = Calvin<DatabaseType>;

  using MessageType = CalvinMessage;
  using MessageFactoryType = CalvinMessageFactory;
  using MessageHandlerType = CalvinMessageHandler;

  CalvinExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
                 const ContextType &context,
                 std::vector<std::unique_ptr<TransactionType>> &transactions,
                 std::vector<StorageType> &storages,
                 std::atomic<uint32_t> &lock_manager_status,
                 std::atomic<uint32_t> &worker_status,
                 std::atomic<uint32_t> &n_complete_workers,
                 std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        transactions(transactions), storages(storages),
        lock_manager_status(lock_manager_status), worker_status(worker_status),
        n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        partitioner(coordinator_id, context.coordinator_num,
                    CalvinHelper::string_to_vint(context.replica_group)),
        workload(coordinator_id, worker_status, db, random, partitioner, start_time),
        n_lock_manager(CalvinHelper::n_lock_manager(
            partitioner.replica_group_id, id,
            CalvinHelper::string_to_vint(context.lock_manager))),
        n_workers(context.worker_num - n_lock_manager),
        lock_manager_id(CalvinHelper::worker_id_to_lock_manager_id(
            id, n_lock_manager, n_workers)),
        init_transaction(false),
        random(id), // make sure each worker has a different seed.
        protocol(db, context, partitioner),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)) {

    for (auto i = 0u; i <= context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
    controlMessageHandlers = ControlMessageHandler<DatabaseType>::get_message_handlers();

    CHECK(n_workers > 0 && n_workers % n_lock_manager == 0);
  }

  ~CalvinExecutor() = default;

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
    LOG(INFO) << "CalvinExecutor " << id << " started. ";

    for (;;) {
      auto begin = std::chrono::steady_clock::now();
      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "CalvinExecutor " << id << " exits. ";
          LOG(INFO) << " router : " << router_percentile.nth(50) << " " <<
          router_percentile.nth(80) << " " << router_percentile.nth(95) << " " << 
          router_percentile.nth(99);

          LOG(INFO) << " analysis : " << analyze_percentile.nth(50) << " " <<
          analyze_percentile.nth(80) << " " << analyze_percentile.nth(95) << " " << 
          analyze_percentile.nth(99);

          LOG(INFO) << " execution : " << execute_latency.nth(50) << " " <<
          execute_latency.nth(80) << " " << execute_latency.nth(95) << " " << 
          execute_latency.nth(99);

          LOG(INFO) <<  " n_lock_manager : " << n_lock_manager;
          return;
        }
      } while (status != ExecutorStatus::Analysis);

      need_remote_read_num = 0;

      n_started_workers.fetch_add(1);
      if (id < n_lock_manager) {
        // my_generate_transactions(); // active // 感觉只要id == 0 进行操作就行了... 
      router_recv_txn_num = 0;
      // 准备transaction
      while(!is_router_stopped(router_recv_txn_num)){ //  && router_transactions_queue.size() < context.batch_size 
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

      VLOG_IF(DEBUG_V, id==0) << "cur_time : " << "  unpack_route_transaction : " << transactions.size();
      
      }
      n_complete_workers.fetch_add(1);

      // wait to Execute

      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Analysis) {
        std::this_thread::yield();
      }
      
      auto analysis_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count();

      VLOG_IF(DEBUG_V, id==0) << "Analysis time: "
              << analysis_time
              << " milliseconds.";

        analyze_percentile.add(analysis_time);

      n_started_workers.fetch_add(1);
      // work as lock manager
      if (id < n_lock_manager) {
        // schedule transactions
        schedule_transactions();

      } else {
        // work as executor
        run_transactions();
        VLOG(DEBUG_V) << "Run time: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - begin)
                      .count()
                << " milliseconds.";
      }

      auto execution_schedule_time =                std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - begin)
                      .count();
        VLOG(DEBUG_V) << "Schedule time: "
                << execution_schedule_time
                << " milliseconds.";

        execute_latency.add(execution_schedule_time);

      LOG(INFO) << "done, send: " << router_recv_txn_num << " " << need_remote_read_num;

      n_complete_workers.fetch_add(1);

      // wait to Analysis

      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Execute) {
        status = static_cast<ExecutorStatus>(worker_status.load());
        if (status == ExecutorStatus::EXIT) {
          break;
        }
        process_request();
      }

    }
  }

  void onExit() override {}

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

  void unpack_route_transaction(){
    int s = router_transactions_queue.size();
    DCHECK(s == router_recv_txn_num);
    transactions.clear();

    while(s -- > 0){
      simpleTransaction simple_txn = router_transactions_queue.front();
      router_transactions_queue.pop_front();

      n_network_size.fetch_add(simple_txn.size);
      size_t t_id = transactions.size();
      auto p = workload.unpack_transaction(context, 0, storages[t_id], simple_txn);
      p->set_id(t_id);
      prepare_transaction(*p);
      // LOG(INFO) << *(int*) p->readSet[0].get_key() << " " << *(int*) p->readSet[1].get_key();
      transactions.push_back(std::move(p));
    }
  }

  void my_generate_transactions() {
      router_recv_txn_num = 0;
      // 准备transaction
      while(!is_router_stopped(router_recv_txn_num)){ //  && router_transactions_queue.size() < context.batch_size 
        process_request();
        std::this_thread::sleep_for(std::chrono::microseconds(5));
      }

      // VLOG_IF(DEBUG_V, id==0) << "prepare_transactions_to_run "
      //         << std::chrono::duration_cast<std::chrono::milliseconds>(
      //                std::chrono::steady_clock::now() - begin)
      //                .count()
      //         << " milliseconds.";
      // now = std::chrono::steady_clock::now();

      unpack_route_transaction(); // 

      VLOG_IF(DEBUG_V, id==0) << "unpack_route_transaction : " << transactions.size();

  }


  void prepare_transaction(TransactionType &txn) {
    /**
     * @brief Set the up prepare handlers object
     * @ active_coordinator
     */
    setup_prepare_handlers(txn);
    // run execute to prepare read/write set
    auto result = txn.execute(id);
    if (result == TransactionResult::ABORT_NORETRY) {
      txn.abort_no_retry = true;
    }

    if (context.calvin_same_batch) {
      txn.save_read_count();
    }

    analyze_active_coordinator(txn);

    // setup handlers for execution
    setup_execute_handlers(txn);
    txn.execution_phase = true;
  }

  void analyze_active_coordinator(TransactionType &transaction) {
    /**
     * @brief 确定某txn 涉及到了些coordinator
     * 
     */
    // assuming no blind write
    auto &readSet = transaction.readSet;
    auto &active_coordinators = transaction.active_coordinators;
    active_coordinators =
        std::vector<bool>(partitioner.total_coordinators(), false);

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readkey = readSet[i];
      if (readkey.get_local_index_read_bit()) {
        continue;
      }
      auto partitionID = readkey.get_partition_id();
      if (readkey.get_write_lock_bit()) {
        active_coordinators[partitioner.master_coordinator(partitionID)] = true;
        active_coordinators[partitioner.secondary_coordinator(partitionID)] = true;
      }
    }
  }

  void schedule_transactions() {

    // grant locks, once all locks are acquired, assign the transaction to
    // a worker thread in a round-robin manner.

    std::size_t request_id = 0;
    int skip_num = 0;
    int real_num = 0;

    int write_lock_num = 0;
    int read_lock_num = 0;
    
    int time_locking = 0;
    int last = 0;
    auto begin = std::chrono::steady_clock::now();

    for (auto i = 0u; i < transactions.size(); i++) {
      // do not grant locks to abort no retry transaction
      if (!transactions[i]->abort_no_retry) {
        bool grant_lock = false;
        auto &readSet = transactions[i]->readSet;
        for (auto k = 0u; k < readSet.size(); k++) {
          auto &readKey = readSet[k];
          auto tableId = readKey.get_table_id();
          auto partitionId = readKey.get_partition_id();

          if (!partitioner.is_partition_replicated_on(partitionId, coordinator_id)) {
            continue;
          }

          auto table = db.find_table(tableId, partitionId);
          auto key = readKey.get_key();

          if (readKey.get_local_index_read_bit()) {
            continue;
          }

          if (CalvinHelper::partition_id_to_lock_manager_id(
                  readKey.get_partition_id(), n_lock_manager,
                  partitioner.replica_group_size) 
              != 
                  lock_manager_id) {
            continue;
          }
          auto now = std::chrono::steady_clock::now();
          grant_lock = true;
          std::atomic<uint64_t> &tid = table->search_metadata(key);
          if (readKey.get_write_lock_bit()) {
            CalvinHelper::write_lock(tid);
            write_lock_num += 1;
            // LOG(INFO) << "TXN[" << transactions[i]->id << "] wLOCK : " << *(int*)key;
          } else if (readKey.get_read_lock_bit()) {
            CalvinHelper::read_lock(tid);
            read_lock_num += 1;
          } else {
            CHECK(false);
          }
          time_locking += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
        }
        if (grant_lock) {
          auto worker = get_available_worker(request_id++);
          all_executors[worker]->transaction_queue.push(transactions[i].get());
          real_num += 1;
        } else {
          skip_num += 1;
        }
        // only count once
        if (i % n_lock_manager == id) {
          n_commit.fetch_add(1);
        }
      } else {
        // only count once
        if (i % n_lock_manager == id) {
          n_abort_no_retry.fetch_add(1);
        }
      }
    }
    last += std::chrono::duration_cast<std::chrono::microseconds>(
                                                            std::chrono::steady_clock::now() - begin)
                                                            .count();

    // LOG(INFO) << " transaction recv : " << transactions.size() << " " << real_num << " " << skip_num;
    LOG(INFO) << " transaction recv : " << transactions.size() << " " 
              << real_num << " " << skip_num << " "
              << time_locking * 1.0 / real_num << " "
              << last * 1.0 / real_num << " "
              << write_lock_num * 1.0 / real_num << " "
              << read_lock_num * 1.0 / real_num;

    set_lock_manager_bit(id);
  }

  void run_transactions() {
    size_t generator_id = context.coordinator_num;

    auto begin = std::chrono::steady_clock::now();

    int idle_time = 0;
    int execution_time = 0;
    int commit_time = 0;
    int cnt = 0;
    while (!get_lock_manager_bit(lock_manager_id) ||
           !transaction_queue.empty()) {

      if (transaction_queue.empty()) {
        process_request();
        continue;
      }

      auto idle = std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count();
      idle_time += idle;
      begin = std::chrono::steady_clock::now();

      TransactionType *transaction = transaction_queue.front();
      bool ok = transaction_queue.pop();
      DCHECK(ok);

      auto result = transaction->execute(id);

      auto execution = std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count();
      execution_time += execution;
      begin = std::chrono::steady_clock::now();

      n_network_size.fetch_add(transaction->network_size.load());
      if (result == TransactionResult::READY_TO_COMMIT) {
        protocol.commit(*transaction, lock_manager_id, n_lock_manager,
                        partitioner.replica_group_size);
      } else if (result == TransactionResult::ABORT) {
        // non-active transactions, release lock
        protocol.abort(*transaction, lock_manager_id, n_lock_manager,
                       partitioner.replica_group_size);
      } else {
        CHECK(false) << "abort no retry transaction should not be scheduled.";
      }
      auto commit = std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count();
      commit_time += commit;
      begin = std::chrono::steady_clock::now();
      cnt += 1;
      // // LOG(INFO) << static_cast<uint32_t>(ControlMessage::ROUTER_TRANSACTION_RESPONSE) << " -> " << generator_id;
      // ControlMessageFactory::router_transaction_response_message(*(messages[generator_id]));
      // // LOG(INFO) << "router_transaction_response_message :" << *(int*)transaction->readSet[0].get_key() << " " << *(int*)transaction->readSet[1].get_key();
      // flush_messages();
    }
    LOG(INFO) << "total: " << cnt << " " 
              << idle_time / cnt  << " "
              << execution_time / cnt << " "
              << commit_time / cnt;

  }

  void setup_execute_handlers(TransactionType &txn) {
    /**
     * @brief 
     * 
     */

    txn.read_handler = [this, &txn](std::size_t worker_id, std::size_t table_id,
                                    std::size_t partition_id, std::size_t id,
                                    uint32_t key_offset, const void *key,
                                    void *value) {
      /**
       * @brief 
       * 
       */
      auto *worker = this->all_executors[worker_id];
      if (worker->partitioner.is_partition_replicated_on(partition_id, coordinator_id)) {
        ITable *table = worker->db.find_table(table_id, partition_id);
        CalvinHelper::read(table->search(key), value, table->value_size());

        auto &active_coordinators = txn.active_coordinators;
        for (auto i = 0u; i < active_coordinators.size(); i++) {
          // 发送到涉及到的且非本地coordinator
          // if (i == worker->coordinator_id || !active_coordinators[i])
          if (i == worker->coordinator_id)
            continue;
          if(!worker->partitioner.is_partition_replicated_on(partition_id, i) && active_coordinators[i]){
            auto sz = MessageFactoryType::new_read_message(
                *worker->messages[i], *table, id, key_offset, value);
            txn.network_size.fetch_add(sz);
            txn.distributed_transaction = true;
            need_remote_read_num += 1;
          }
        }
        txn.local_read.fetch_add(-1);
      }
    };

    txn.setup_process_requests_in_execution_phase(
        n_lock_manager, n_workers, partitioner.replica_group_size);

    txn.remote_request_handler = [this](std::size_t worker_id) {
      auto *worker = this->all_executors[worker_id];
      return worker->process_request();
    };
    txn.message_flusher = [this](std::size_t worker_id) {
      auto *worker = this->all_executors[worker_id];
      worker->flush_messages();
    };
  };

  void setup_prepare_handlers(TransactionType &txn) {
    /**
     * @brief
    */

    txn.local_index_read_handler = [this](std::size_t table_id,
                                          std::size_t partition_id,
                                          const void *key, void *value) {
      ITable *table = this->db.find_table(table_id, partition_id);
      CalvinHelper::read(table->search(key), value, table->value_size());
    };
    txn.setup_process_requests_in_prepare_phase();
  };

  void set_all_executors(const std::vector<CalvinExecutor *> &executors) {
    all_executors = executors;
  }

  std::size_t get_available_worker(std::size_t request_id) {
    // assume there are n lock managers and m workers
    // 0, 1, .. n-1 are lock managers
    // n, n + 1, .., n + m -1 are workers

    // the first lock managers assign transactions to n, .. , n + m/n - 1

    auto start_worker_id = n_lock_manager + n_workers / n_lock_manager * id;
    auto len = n_workers / n_lock_manager; 
    return request_id % len + start_worker_id;
  }

  void set_lock_manager_bit(int id) {
    uint32_t old_value, new_value;
    do {
      old_value = lock_manager_status.load();
      DCHECK(((old_value >> id) & 1) == 0);
      new_value = old_value | (1 << id);
    } while (!lock_manager_status.compare_exchange_weak(old_value, new_value));
  }

  bool get_lock_manager_bit(int id) {
    return (lock_manager_status.load() >> id) & 1;
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
                                transactions);
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
  std::vector<std::unique_ptr<TransactionType>>& transactions;
  std::vector<StorageType> &storages;
  std::atomic<uint32_t> &lock_manager_status, &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  CalvinPartitioner partitioner;
  WorkloadType workload;
  std::size_t n_lock_manager, n_workers;
  std::size_t lock_manager_id;
  bool init_transaction;
  RandomType random;
  ProtocolType protocol;
  std::unique_ptr<Delay> delay;
  std::vector<std::unique_ptr<Message>> messages;
  std::vector<
      std::function<void(MessagePiece, Message &, ITable &,
                         std::vector<std::unique_ptr<TransactionType>> &)>>
      messageHandlers;
  std::vector<
      std::function<void(MessagePiece, Message &, DatabaseType &, std::deque<simpleTransaction>* ,std::deque<int>* )>>
      controlMessageHandlers;
  
  LockfreeQueue<Message *, 10086> in_queue, out_queue;
  LockfreeQueue<TransactionType *, 10086> transaction_queue;
  std::vector<CalvinExecutor *> all_executors;

  std::deque<simpleTransaction> router_transactions_queue;
  std::deque<int> router_stop_queue;

  int router_recv_txn_num = 0; // generator router from 
  Percentile<int64_t> router_percentile, analyze_percentile, execute_latency;
  int need_remote_read_num = 0;
};
} // namespace star