//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include "core/Partitioner.h"

#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Hermes/Hermes.h"
#include "protocol/Hermes/HermesHelper.h"
#include "protocol/Hermes/HermesMessage.h"

#include <chrono>
#include <thread>

namespace star {

template <class Workload> class HermesExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = HermesTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");

  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = Hermes<DatabaseType>;

  using MessageType = HermesMessage;
  using MessageFactoryType = HermesMessageFactory<DatabaseType>;
  using MessageHandlerType = HermesMessageHandler<DatabaseType>;

  HermesExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
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
                    HermesHelper::string_to_vint(context.replica_group), db),
        workload(coordinator_id, worker_status, db, random, partitioner, start_time),
        n_lock_manager(HermesHelper::n_lock_manager(
            partitioner.replica_group_id, id,
            HermesHelper::string_to_vint(context.lock_manager))),
        n_workers(context.worker_num - n_lock_manager),
        lock_manager_id(HermesHelper::worker_id_to_lock_manager_id(
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

  ~HermesExecutor() = default;

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
    LOG(INFO) << "HermesExecutor " << id << " started. ";

    for (;;) {
      auto begin = std::chrono::steady_clock::now();
      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "HermesExecutor " << id << " exits. ";
          return;
        }
      } while (status != ExecutorStatus::Analysis);

      n_started_workers.fetch_add(1);
      if (id < n_lock_manager) {

        // my_generate_transactions(); // active // 感觉只要id == 0 进行操作就行了... 
      router_recv_txn_num = 0;
      // 准备transaction
      while(!is_router_stopped(router_recv_txn_num)){ //  && router_transactions_queue.size() < context.batch_size 
        process_request();
        std::this_thread::sleep_for(std::chrono::microseconds(5));
      }
      VLOG_IF(DEBUG_V, id==0) << "Unpack start time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";
      unpack_route_transaction(); // 

      VLOG_IF(DEBUG_V, id==0) << "unpack_route_transaction : " << transactions.size();

        
      VLOG_IF(DEBUG_V, id==0) << "Unpack end time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";
        // router_planning();
      }
      n_complete_workers.fetch_add(1);

      // wait to Execute

      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Analysis) {
        std::this_thread::yield();
      }

      VLOG_IF(DEBUG_V, id==0) << "Analysis time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";


      n_started_workers.fetch_add(1);
      // work as lock manager
      if (id < n_lock_manager) {
        // schedule transactions
        schedule_transactions();

        VLOG(DEBUG_V) << "Schedule time: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - begin)
                      .count()
                << " milliseconds.";

        for(int r = 0; r < router_recv_txn_num; r ++ ){
          // 发回原地...
          size_t generator_id = context.coordinator_num;
          // LOG(INFO) << static_cast<uint32_t>(ControlMessage::ROUTER_TRANSACTION_RESPONSE) << " -> " << generator_id;
          ControlMessageFactory::router_transaction_response_message(*(messages[generator_id]));
          flush_messages();
        }
        VLOG(DEBUG_V) << "Route back time: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - begin)
                      .count()
                << " milliseconds.";
      } else {
        // work as executor
        run_transactions();
        VLOG(DEBUG_V) << "Run time: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - begin)
                      .count()
                << " milliseconds.";
      }
      LOG(INFO) << "done, send: " << router_recv_txn_num << " " << 
                                     need_transfer_num   << " " << need_remote_read_num;

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
      // txn_replica_involved(&simple_txn, context.coordinator_id);
      router_transactions_queue.pop_front();

      n_network_size.fetch_add(simple_txn.size);
      // router_planning(&simple_txn);
      size_t t_id = transactions.size();
      auto p = workload.unpack_transaction(context, 0, storages[t_id], simple_txn);
      p->set_id(t_id);
      p->on_replica_id = simple_txn.on_replica_id;      
      p->router_coordinator_id = simple_txn.destination_coordinator;

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

    // analyze_active_coordinator(txn);

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
    
    int replica_id = transaction.on_replica_id;

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readKey = readSet[i];

      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto key = readKey.get_key();
      
      auto master_coordinator = db.get_dynamic_coordinator_id(context.coordinator_num, tableId, key, replica_id);
    //   auto secondary_coordinator = partitioner.secondary_coordinator(tableId, partitionId, key);
      readKey.set_master_coordinator_id(master_coordinator);
    //   readKey.set_secondary_coordinator_id(secondary_coordinator);

      if (readKey.get_local_index_read_bit()) {
        continue;
      }
      //

      if (readKey.get_write_lock_bit()) {
        // 有写且写到主副本...从副本也需要吧
        active_coordinators[readKey.get_master_coordinator_id()] = true;
        // active_coordinators[readKey.get_secondary_coordinator_id()] = true;
      }
    }
  }



  void transfer_request_messages(TransactionType &txn){

    // std::size_t target_coordi_id = (std::size_t)txn.router_coordinator_id;
    auto& readSet = txn.readSet;
    txn.pendingResponses = 0; // 初始化一下?
    int replica_id = txn.on_replica_id;

    auto *worker = this->all_executors[id]; // current-lock-manager

    for (auto k = 0u; k < readSet.size(); k++) {
      auto& readKey = readSet[k];
      int key_offset = k;
       // readKey.get_master_coordinator_id();
      auto table_id = readKey.get_table_id();
      auto partition_id = readKey.get_partition_id();
      auto key = readKey.get_key();
      ITable *table = db.find_table(table_id, partition_id, replica_id);

      auto coordinatorID = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key, replica_id);
      readKey.set_master_coordinator_id(coordinatorID);
      // 
      if(coordinatorID == coordinator_id){
        continue;
      }
      need_transfer_num += 1;
      for(size_t i = 0; i <= context.coordinator_num; i ++ ){ 
        // also send to generator to update the router-table
        if(i == coordinator_id){
          continue; // local
        }
        if(i == coordinatorID){
          // target
          txn.network_size += MessageFactoryType::new_async_search_message(
              *(this->messages[i]), *table, key, txn.id, key_offset, replica_id);
          VLOG(DEBUG_V8) << "ASYNC MIGRATE " << table_id << " ASK " << i << " " << *(int*)key << " " << txn.readSet.size();
        } else {
          // others, only change the router
          txn.network_size += MessageFactoryType::transfer_request_router_only_message(
              *worker->messages[i], 
              *table, txn, key_offset);
          VLOG(DEBUG_V8) << "ASYNC ROUTER " << table_id << " ASK " << i << " " << *(int*)key << " " << txn.readSet.size();
        } 
        txn.pendingResponses++;

        // txn.asyncPendingResponses++;
        // this->async_pend_num.fetch_add(1);
      }


    // //   readKey.set_secondary_coordinator_id(secondary_coordi_id);
    //   VLOG(DEBUG_V12) << *(int*) key << " " << master_coordi_id; 
    

    

    //   if(coordinator_id == target_coordi_id){
    //     // local, then receive all info away
    //     if(master_coordi_id == target_coordi_id){
    //       // key already at local, pass...
    //     } else {
    //       // mastership of the key is not at local, wait for transfer/remaster from other nodes
    //       txn.pendingResponses ++; // wait for transfer
    //     }

    //     // wait for router
    //     for(std::size_t c = 0; c < context.coordinator_num; c ++ ){
    //       if(c == target_coordi_id || c == master_coordi_id){
    //         continue;
    //       }
    //       int32_t sz = MessageFactoryType::transfer_request_router_only_message(*worker->messages[c], 
    //                                     *table, txn, k, target_coordi_id);
    //       // txn.pendingResponses ++ ;
    //       txn.network_size.fetch_add(sz);
    //     }

    //   } else {
    //     // remote, then send all info you need 
    //     if(coordinator_id == master_coordi_id){
    //       // has mastership of relational key, send key to the target
    //       bool success = true;

    //       int32_t sz = MessageFactoryType::transfer_request_message(*worker->messages[target_coordi_id], 
    //                                       db, context, &partitioner,
    //                                       *table, k, txn, success);
    //       txn.pendingResponses ++ ;
    //       txn.network_size.fetch_add(sz);
    //     } else if(master_coordi_id != target_coordi_id){
    //       // 不是主副本，但是主副本发生改变 wait master's only router-message to update local router
    //       // txn.pendingResponses ++ ;
    //     }
    //   }

    }

    txn.message_flusher(id);

    // if(coordinator_id == target_coordi_id){
    // spin on local & remote read
    // VLOG(DEBUG_V8) << "WAIT remote" << txn.id << " " << txn.pendingResponses << " " << txn.local_read.load() << " " << txn.remote_read.load();
    while (txn.pendingResponses > 0) {
      // process remote reads for other workers
      txn.remote_request_handler(id);
    }
    // VLOG(DEBUG_V8) << "WAIT remote" << txn.id << " " << txn.pendingResponses << " " << txn.local_read.load() << " " << txn.remote_read.load();

    // }

  }

  void schedule_transactions() {

    // grant locks, once all locks are acquired, assign the transaction to
    // a worker thread in a round-robin manner.

    std::size_t request_id = 0;
    int skip_num = 0;
    int real_num = 0;
    need_transfer_num = 0;
    need_remote_read_num = 0;

    int time_transfer_read = 0;
    int time_locking = 0;
    int last = 0;
    
    auto begin = std::chrono::steady_clock::now();

    for (auto i = 0u; i < transactions.size(); i++) {
      // do not grant locks to abort no retry transaction
      auto& txn = transactions[i];
      // LOG(INFO) << i << " : " << *(int*)txn->readSet[0].get_key() << " " << *(int*)txn->readSet[1].get_key() 
      //           << " " << txn->on_replica_id << " " << txn->router_coordinator_id;
      // target replica is not at local
      if(txn->on_replica_id == -1){
        continue;
      }
      // router is not at local
      if((int)context.coordinator_id != txn->router_coordinator_id){
        continue;
      }
      auto now = std::chrono::steady_clock::now();
      // do transactions remote
      transfer_request_messages(*txn);

      time_transfer_read += std::chrono::duration_cast<std::chrono::microseconds>(
                                                            std::chrono::steady_clock::now() - now)
          .count();
      now = std::chrono::steady_clock::now();
          

      if (!txn->abort_no_retry) {
        bool grant_lock = false;
        auto &readSet = txn->readSet;
        bool is_with_lock_manager = false;
        int replica_id = txn->on_replica_id;

        for (auto k = 0u; k < readSet.size(); k++) {
          auto &readKey = readSet[k];
          auto tableId = readKey.get_table_id();
          auto partitionId = readKey.get_partition_id();
          auto key = readKey.get_key();

          auto master_coordinator_id = readKey.get_master_coordinator_id();

          // 本地只要有副本就锁住          
          if (master_coordinator_id == coordinator_id) {
            txn->local_read.fetch_add(1);
          } else {
            txn->remote_read.fetch_add(1);
          }
          VLOG(DEBUG_V12) << " @@@ " << master_coordinator_id << " " <<  coordinator_id << " " <<
                             *(int*)key << " " << 
                             txn->local_read.load() << " " << txn->remote_read.load();

          auto table = db.find_table(tableId, partitionId, replica_id);

          if (readKey.get_local_index_read_bit()) {
            continue;
          }

          if (HermesHelper::partition_id_to_lock_manager_id(
                  readKey.get_partition_id(), n_lock_manager,
                  partitioner.replica_group_size) 
              != 
                  lock_manager_id) {
            continue;
          } else {
            is_with_lock_manager = true;
          }

          grant_lock = true;
          std::atomic<uint64_t> &tid = table->search_metadata(key);
          if (readKey.get_write_lock_bit()) {
            VLOG(DEBUG_V12) << "      " << *(int*)key << " LOCK";
            HermesHelper::write_lock(tid);
          } else if (readKey.get_read_lock_bit()) {
            HermesHelper::read_lock(tid);
          } else {
            CHECK(false);
          }
        }
        if (grant_lock) {
          auto worker = get_available_worker(request_id++);
          all_executors[worker]->transaction_queue.push(txn.get());
          real_num += 1;
        } else {
          skip_num += 1;
          if(is_with_lock_manager == true){
            VLOG(DEBUG_V12) << txn->id << " not at C" << context.coordinator_id;
          } else {
            VLOG(DEBUG_V12) << txn->id << " not for lock manager " << id << " at C" << context.coordinator_id;
          }
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

      time_locking += std::chrono::duration_cast<std::chrono::microseconds>(
                                                            std::chrono::steady_clock::now() - now)
          .count();
      now = std::chrono::steady_clock::now(); 
    }

    last += std::chrono::duration_cast<std::chrono::microseconds>(
                                                            std::chrono::steady_clock::now() - begin)
                                                            .count();

    LOG(INFO) << " transaction recv : " << transactions.size() << " " 
              << real_num << " " << skip_num << " "
              << time_transfer_read / 1000 << " " << time_locking / 1000 << " "
              << last / 1000;

    set_lock_manager_bit(id);
  }

  void run_transactions() { 
    size_t generator_id = context.coordinator_num;

    while (!get_lock_manager_bit(lock_manager_id) ||
           !transaction_queue.empty()) {

      if (transaction_queue.empty()) {
        process_request();
        continue;
      }

      TransactionType *transaction = transaction_queue.front();
      bool ok = transaction_queue.pop();
      DCHECK(ok);
      // analyze_active_coordinator(*transaction);

      auto result = transaction->execute(id);

      VLOG(DEBUG_V12) << id << " exeute " << transaction->id;

      n_network_size.fetch_add(transaction->network_size.load());
      if (result == TransactionResult::READY_TO_COMMIT) {
        protocol.commit(*transaction, lock_manager_id, n_lock_manager,
                        partitioner.replica_group_size);
        VLOG(DEBUG_V12) << id << " commit " << transaction->id;

      } else if (result == TransactionResult::ABORT) {
        // non-active transactions, release lock
        protocol.abort(*transaction, lock_manager_id, n_lock_manager,
                       partitioner.replica_group_size);
        VLOG(DEBUG_V12) << id << " abort " << transaction->id;

        // auto tmp = transaction->get_query();
        // // LOG(INFO) << "ABORT " << *(int*)& tmp[0] << " " << *(int*)& tmp[1] << " " << transaction->active_coordinators[0] << " " << transaction->active_coordinators[1];
      } else {
        CHECK(false) << "abort no retry transaction should not be scheduled.";
      }
    }
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
       * @brief if master coordinator, read local and spread to all active replica
       * @param id = index of current txn
       */
      auto *worker = this->all_executors[worker_id];
      auto& readKey = txn.readSet[key_offset];
      int replica_id = txn.on_replica_id;
      VLOG(DEBUG_V12) << " read_handler : " << id << " " << *(int*)readKey.get_key() << " " << 
                         "master_coordi : " << readKey.get_master_coordinator_id() << 
                         " sec: " << readKey.get_secondary_coordinator_id() << " " << 
                          txn.local_read.load() << " " << txn.remote_read.load();

      if (readKey.get_master_coordinator_id() == coordinator_id) {
        ITable *table = worker->db.find_table(table_id, partition_id, replica_id);
        HermesHelper::read(table->search(key), value, table->value_size());

        // auto &active_coordinators = txn.active_coordinators;
        // for (auto i = 0u; i < active_coordinators.size(); i ++ ){// active_coordinators.size(); i++) {
        //   // 发送到涉及到的且非本地coordinator
        //   if (i == worker->coordinator_id)// !active_coordinators[i])
        //     continue;
        //   if(i != readKey.get_secondary_coordinator_id() && i != readKey.get_master_coordinator_id()){
        //     continue;
        //   }
        //   auto sz = MessageFactoryType::new_read_message(
        //       *worker->messages[i], *table, id, key_offset, key, value);
        //   need_remote_read_num += 1;
        //   txn.network_size.fetch_add(sz);
        //   txn.distributed_transaction = true;
        // }
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
                                          int replica_id,
                                          const void *key, void *value) {
      ITable *table = this->db.find_table(table_id, partition_id, replica_id);
      HermesHelper::read(table->search(key), value, table->value_size());
    };
    txn.setup_process_requests_in_prepare_phase();
  };

  void set_all_executors(const std::vector<HermesExecutor *> &executors) {
    all_executors = executors;
  }

  std::size_t get_available_worker(std::size_t request_id) {
    // assume there are n lock managers and m workers
    // 0, 1, .. n-1 are lock managers
    // n, n + 1, .., n + m -1 are workers

    // the first lock managers assign transactions to n, .. , n + m/n - 1
    // e.g. 2 locker
    //      4 worker
    //   n_workers = n_thread - n_lock_manager 
    //   id = [0,1] (lock_manager)
    //   start_worker_id = 2 + 4 / 2 * id = 
    //   len = 2 
    //   request_id % len = [0,1] + start_worker_id => [2,3,4,5]
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
        // ITable *table = db.find_table(messagePiece.get_table_id(),
        //                               messagePiece.get_partition_id());
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
                                *messages[message->get_source_node_id()], 
                                db, context, &partitioner,
                                // *table,
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
  HermesPartitioner<Workload> partitioner;
  WorkloadType workload;
  std::size_t n_lock_manager, n_workers;
  std::size_t lock_manager_id;
  bool init_transaction;
  RandomType random;
  ProtocolType protocol;
  std::unique_ptr<Delay> delay;
  std::vector<std::unique_ptr<Message>> messages;
  std::vector<
      std::function<void(MessagePiece, Message &, 
                         DatabaseType &, const ContextType &, Partitioner *,
                        //  ITable &,
                         std::vector<std::unique_ptr<TransactionType>> &)>>
      messageHandlers;
  std::vector<
      std::function<void(MessagePiece, Message &, DatabaseType &, std::deque<simpleTransaction>* ,std::deque<int>* )>>
      controlMessageHandlers;

  LockfreeQueue<Message *, 10086> in_queue, out_queue;
  LockfreeQueue<TransactionType *> transaction_queue;
  std::vector<HermesExecutor *> all_executors;

  std::deque<simpleTransaction> router_transactions_queue;
  std::deque<int> router_stop_queue;

  int need_transfer_num = 0;
  int need_remote_read_num = 0;

  int router_recv_txn_num = 0; // generator router from 
};
} // namespace star