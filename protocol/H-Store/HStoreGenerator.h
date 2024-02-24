//
// Created by Yi Lu on 9/10/18.
//

#pragma once

#include "common/Percentile.h"
#include "common/ShareQueue.h"

#include "core/ControlMessage.h"
#include "core/Defs.h"
#include "core/Delay.h"
#include "core/Partitioner.h"
#include "core/Worker.h"
#include "glog/logging.h"
#include <chrono>

#include "HStoreManager.h"
#include "core/Coordinator.h"
#include <mutex>          // std::mutex, std::lock_guard

namespace star {

template <class Workload, class Protocol> class HStoreGenerator : public Worker {
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

  HStoreGenerator(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
           const ContextType &context, std::atomic<uint32_t> &worker_status,
           std::atomic<uint32_t> &n_complete_workers,
           std::atomic<uint32_t> &n_started_workers,
           hstore::ScheduleMeta &schedule_meta)
      : Worker(coordinator_id, id), db(db), context(context),
        worker_status(worker_status), 
        n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        schedule_meta(schedule_meta),
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

      messages_mutex.emplace_back(std::make_unique<std::mutex>());
    }
    
    is_replica_worker = coordinator_id < this->partitioner->num_coordinator_for_one_replica() ? false: true;

    messageHandlers = MessageHandlerType::get_message_handlers();
    controlMessageHandlers = ControlMessageHandler<DatabaseType>::get_message_handlers();

    message_stats.resize(messageHandlers.size(), 0);
    message_sizes.resize(messageHandlers.size(), 0);

    router_transaction_done.store(0);
    router_transactions_send.store(0);

    for(int i = 0 ; i < 20 ; i ++ ){
      is_full_signal_self[i].store(0);
    }
    DCHECK(id < context.worker_num);
    LOG(INFO) << id;
    generator_num = 1;

    generator_core_id.resize(context.coordinator_num);
    dispatcher_core_id.resize(context.coordinator_num);

    pin_thread_id_ = 3 + 2 + context.worker_num;

    for(size_t i = 0 ; i < generator_num; i ++ ){
      generator_core_id[i] = pin_thread_id_ ++ ;
    }

    for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      dispatcher_core_id[i] = pin_thread_id_ + id * context.coordinator_num + i;
    }
    dispatcher_num = context.worker_num * context.coordinator_num;
    cur_txn_num = context.batch_size / dispatcher_num ; // * context.coordinator_num


    for (auto n = 0u; n < context.coordinator_num; n++) {
      // 处理
      dispatcher.emplace_back([&](int n, int worker_id) {
        ExecutorStatus status = static_cast<ExecutorStatus>(worker_status.load());
        int dispatcher_id  = worker_id * context.coordinator_num + n;
        LOG(INFO) << n << " " << worker_id * context.coordinator_num;
        while(is_full_signal_self[dispatcher_id].load() == false){
            bool success = prepare_transactions_to_run(workload, storages[dispatcher_id],
                                  transactions_queue_self[dispatcher_id]);

            if(!success){ // full
                is_full_signal_self[dispatcher_id].store(true);
            } 
        }

        do {
          status = static_cast<ExecutorStatus>(worker_status.load());
          std::this_thread::sleep_for(std::chrono::microseconds(5));
        } while (status != ExecutorStatus::START);  
        
        while(status != ExecutorStatus::EXIT && status != ExecutorStatus::CLEANUP){
          status = static_cast<ExecutorStatus>(worker_status.load());
          if(is_full_signal_self[dispatcher_id].load() == true){
            std::this_thread::sleep_for(std::chrono::microseconds(5));
            continue;
          }

          bool success = prepare_transactions_to_run(workload, storages[dispatcher_id],
            transactions_queue_self[dispatcher_id]);
          if(!success){ // full
            is_full_signal_self[dispatcher_id].store(true);
          }                    
        }
      }, n, this->id);

      if (context.cpu_affinity) {
      LOG(INFO) << "dispatcher_core_id[n]: " << dispatcher_core_id[n] 
                << " work_id" << this->id;
        ControlMessageFactory::pin_thread_to_core(context, dispatcher[n], dispatcher_core_id[n]);
      }
    }     
  
  }

  ~HStoreGenerator(){
    // for (auto i = 0u; i < dispatcher.size(); i++) {
    //   dispatcher[i].join();
    // }
  }
  // std::unique_ptr<TransactionType> get_next_transaction() {
  //   if (is_replica_worker) {
  //     // Sleep for a while to save cpu
  //     //std::this_thread::sleep_for(std::chrono::microseconds(10));
  //     return nullptr;
  //   } else {
  //     auto partition_id = managed_partitions[this->random.next() % managed_partitions.size()];
  //     auto granule_id = this->random.next() % this->context.granules_per_partition;
  //     auto txn = this->workload.next_transaction(this->context, partition_id, this->dispatcher_id, granule_id);
  //     txn->context = &this->context;
  //     auto total_batch_size = this->partitioner->num_coordinator_for_one_replica() * this->context.batch_size;
  //     if (this->context.stragglers_per_batch) {
  //       auto v = this->random.uniform_dist(1, total_batch_size);
  //       if (v <= (uint64_t)this->context.stragglers_per_batch) {
  //         txn->straggler_wait_time = this->context.stragglers_total_wait_time / this->context.stragglers_per_batch;
  //       }
  //     }
  //     if (this->context.straggler_zipf_factor > 0) {
  //       int length_type = star::Zipf::globalZipfForStraggler().value(this->random.next_double());
  //       txn->straggler_wait_time = transaction_lengths[length_type];
  //       transaction_lengths_count[length_type]++;
  //     }
  //     return txn;
  //   }
  // }


  bool prepare_transactions_to_run(WorkloadType& workload, StorageType& storage,
      ShareQueue<simpleTransaction*, 40960>& transactions_queue_self_){
    /** 
     * @brief 准备需要的txns
     * @note add by truth 22-01-24
     */
      std::size_t hot_area_size = context.partition_num / context.coordinator_num;

      if(WorkloadType::which_workload == myTestSet::YCSB){

      } else {
        hot_area_size = context.coordinator_num;
      }
      std::size_t partition_id = random.uniform_dist(0, context.partition_num - 1); // get_random_partition_id(n, context.coordinator_num);
      // 
      size_t skew_factor = random.uniform_dist(1, 100);
      if (context.skew_factor >= skew_factor) {
        // 0 >= 50 
        if(WorkloadType::which_workload == myTestSet::YCSB){
          partition_id = 0;
        } else {
          partition_id = (0 + skew_factor * context.coordinator_num) % context.partition_num;
        }
      } else {
        // 0 < 50
        //正常
      }
      // 
      std::size_t partition_id_;
      if(WorkloadType::which_workload == myTestSet::YCSB){
        partition_id_ = partition_id / hot_area_size * hot_area_size + 
                                  partition_id / hot_area_size % context.coordinator_num;
      } else {
        if(context.skew_factor >= skew_factor) {
          partition_id_ = partition_id / hot_area_size * hot_area_size;

        } else {
          partition_id_ = partition_id / hot_area_size * hot_area_size + 
                                  partition_id / hot_area_size % context.coordinator_num;;
        }
      }
      partition_id_ %= context.partition_num;

      // 
      std::unique_ptr<TransactionType> cur_transaction = workload.next_transaction(context, partition_id_, this->id, storage);
      
      simpleTransaction* txn = new simpleTransaction();
      txn->keys = cur_transaction->get_query();
      txn->update = cur_transaction->get_query_update();
      txn->partition_id = cur_transaction->get_partition_id();
      // 
      bool is_cross_txn_static  = cur_transaction->check_cross_node_txn(false);
      if(is_cross_txn_static){
        txn->is_distributed = true;
      } else {
        DCHECK(txn->is_distributed == false);
      }
    

    return transactions_queue_self_.push_no_wait(txn); // txn->partition_id % context.coordinator_num [0]
  }


  void router_request(std::vector<int>& router_send_txn_cnt, std::shared_ptr<simpleTransaction> txn){
    // router transaction to coordinators
    size_t coordinator_id_dst = txn->destination_coordinator;

    size_t router_size = ControlMessageFactory::new_router_transaction_message(
        *async_messages[coordinator_id_dst], 0, *txn, 
        RouterTxnOps::TRANSFER);
    flush_message(async_messages, coordinator_id_dst);

    // router_send_txn_cnt[coordinator_id_dst]++;
    n_network_size.fetch_add(router_size);
    // router_transactions_send.fetch_add(1);
    coordinator_send[txn->destination_coordinator] ++ ;
  };

  void scheduler_transactions(int dispatcher_num, int dispatcher_id){    

    if(transactions_queue_self[dispatcher_id].size() < (size_t)cur_txn_num * dispatcher_num){
      DCHECK(false);
    }
    // int cur_txn_num = context.batch_size * context.coordinator_num / dispatcher_num;
    int idx_offset = dispatcher_id * cur_txn_num;

    auto & txns            = schedule_meta.node_txns;
    auto & busy_           = schedule_meta.node_busy;
    auto & txns_coord_cost = schedule_meta.txns_coord_cost;
    
    auto staart = std::chrono::steady_clock::now();

    double cur_timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
                 std::chrono::steady_clock::now() - start_time)
                 .count() * 1.0 / 1000 / 1000;
    int workload_type = ((int)cur_timestamp / context.workload_time) + 1;// which_workload_(crossPartition, (int)cur_timestamp);
    // find minimal cost routing 
    LOG(INFO) << "txn_id.load() = " << schedule_meta.txn_id.load() << " " << cur_txn_num;
    
    std::vector<int> busy_local(context.coordinator_num, 0);
    int real_distribute_num = 0;
    for(int i = 0; i < cur_txn_num; i ++ ){
      bool success = false;
      std::shared_ptr<simpleTransaction> new_txn(transactions_queue_self[dispatcher_id].pop_no_wait(success)); 
      int idx = i + idx_offset;

      txns[idx] = std::move(new_txn);
      // if(i < 2){
      //   LOG(INFO) << i << " " << txns[idx]->is_distributed;
      // }
      auto& txn = txns[idx];
      txn->idx_ = idx;      

      DCHECK(success == true);

      if(WorkloadType::which_workload == myTestSet::YCSB){
        txn_nodes_involved(txn.get(), txns_coord_cost);
      } else {
        txn_nodes_involved_tpcc(txn.get(), txns_coord_cost);
      }


      busy_local[txn->destination_coordinator] += 1;
    }

    schedule_meta.txn_id.fetch_add(1);
    {
      std::lock_guard<std::mutex> l(schedule_meta.l);
      for(size_t i = 0; i < context.coordinator_num; i ++ ){
          busy_[i] += busy_local[i];
      }
    }

    double cur_timestamp__ = std::chrono::duration_cast<std::chrono::microseconds>(
                 std::chrono::steady_clock::now() - staart)
                 .count() * 1.0 / 1000 ;


              
    if(real_distribute_num > 0){
      LOG(INFO) << "real_distribute_num = " << real_distribute_num;
    }

    LOG(INFO) << "scheduler : " << cur_timestamp__ << " " << schedule_meta.txn_id.load();
    while((int)schedule_meta.txn_id.load() < dispatcher_num){
      auto i = schedule_meta.txn_id.load();
      std::this_thread::sleep_for(std::chrono::microseconds(5));
    }
  }


  void router_fence(){

    while(router_transaction_done.load() != router_transactions_send.load()){
      int a = router_transaction_done.load();
      int b = router_transactions_send.load();
      process_request(); 
    }
    router_transaction_done.store(0);//router_transaction_done.fetch_sub(router_transactions_send.load());
    router_transactions_send.store(0);
  }

  int pin_thread_id_ = 3;




  int get_dynamic_coordinator_id(MoveRecord<WorkloadType>& record){
    
    size_t coordinator_id;
    int32_t table_id = record.table_id;
    switch (table_id)
    {
    case tpcc::warehouse::tableID:
        coordinator_id = 
        db.get_dynamic_coordinator_id(context.coordinator_num, 
                                      record.table_id, 
                                      (void*)& record.key.w_key);
        break;
    case tpcc::district::tableID:
        coordinator_id = 
        db.get_dynamic_coordinator_id(context.coordinator_num, 
                                      record.table_id, 
                                      (void*)& record.key.d_key);
        break;
    case tpcc::customer::tableID:
        coordinator_id = 
        db.get_dynamic_coordinator_id(context.coordinator_num, 
                                      record.table_id, 
                                      (void*)& record.key.c_key);
        break;
    case tpcc::stock::tableID:
        coordinator_id = 
        db.get_dynamic_coordinator_id(context.coordinator_num, 
                                      record.table_id, 
                                      (void*)& record.key.s_key);
        break;
    default:
        DCHECK(false);
        break;
    }
    return coordinator_id;
    
  }

  int partition_owner_cluster_coordinator(int partition_id, std::size_t ith_replica) {
    auto ret = this->partitioner->get_ith_replica_coordinator(partition_id, ith_replica);
    // if (is_replica_worker == false && ith_replica == 0) {
    //   DCHECK(ret == this->hash_partitioner->get_ith_replica_coordinator(partition_id, ith_replica));
    // }

    return ret;
  }

  void txn_nodes_involved(simpleTransaction* t, 
                          std::vector<std::vector<int>>& txns_coord_cost) {
    

      int max_cnt = INT_MIN;
      int max_node = -1;

      max_node = partition_owner_cluster_coordinator(t->partition_id, 0);
      
      t->destination_coordinator = max_node;
     return;
   }

  void txn_nodes_involved_tpcc(simpleTransaction* t, 
                          std::vector<std::vector<int>>& txns_coord_cost_) {
    

      int max_node = -1;
      // if(context.random_router > 0){
      //   // 
      //   // size_t random_value = random.uniform_dist(0, 9);
      //   size_t coordinator_id = (keys.W_ID - 1) % context.coordinator_num;
      //   max_node = coordinator_id; // query_keys[0];
        
      // } 
      max_node = partition_owner_cluster_coordinator(t->partition_id, 0);

      t->destination_coordinator = max_node;
     return;
   }

  void start() override {

    LOG(INFO) << "SiloGenerator " << id << " starts.";
    start_time = std::chrono::steady_clock::now();
    workload.start_time = start_time;
    
    StorageType storage;
    uint64_t last_seed = 0;

    // transaction only commit in a single group

    std::queue<std::unique_ptr<TransactionType>> q;
    std::size_t count = 0;

    // main loop
    for (;;) {
      ExecutorStatus status;

      while ((status = static_cast<ExecutorStatus>(worker_status.load())) !=
            ExecutorStatus::START) {
        if(status == ExecutorStatus::CLEANUP){
          for(auto& n: dispatcher){
            n.join();
          }
          return;
        }
        std::this_thread::yield();
      }

      n_started_workers.fetch_add(1);

      auto t = std::chrono::steady_clock::now();
      std::vector<int> send_num(context.coordinator_num, 0);

      do {
        process_request();
        for(int i = 0 ; i < context.coordinator_num; i ++ ){
          int cur = schedule_meta.router_transaction_done[i].load();
          int b_sz = context.batch_size;
          if(cur >= b_sz){
            continue;
          }
          // consumed one transaction and route one
          int dispatcher_id  = this->id * context.coordinator_num + i;

          auto & busy_           = schedule_meta.node_busy;
          auto & txns_coord_cost = schedule_meta.txns_coord_cost;
          
          // get one transaction from queue
          bool success = false;
          std::shared_ptr<simpleTransaction> new_txn(transactions_queue_self[dispatcher_id].pop_no_wait(success)); 

          DCHECK(success == true);
          // determine its ideal destination
          if(WorkloadType::which_workload == myTestSet::YCSB){
            txn_nodes_involved(new_txn.get(), txns_coord_cost);
          } else {
            txn_nodes_involved_tpcc(new_txn.get(), txns_coord_cost);
          }

          // router the transaction
          router_request(router_send_txn_cnt, new_txn);   
          send_num[new_txn->destination_coordinator] += 1;

          // inform generator to create new transactions
          is_full_signal_self[dispatcher_id].store(false);
          schedule_meta.router_transaction_done[i].fetch_add(1);
        }
        auto now = std::chrono::steady_clock::now();
        if(std::chrono::duration_cast<std::chrono::seconds>(now - t).count() > 3){
          t = now;
          LOG(INFO) << "SEND OUT";
          for(int i = 0 ; i < context.coordinator_num; i ++ ){
            LOG(INFO) << " coord[" << i << "]" << send_num[i];
            send_num[i] = 0;
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
    }
    // not end here!
  }

  // void start() override {
  //   // last_mp_arrival = std::chrono::steady_clock::now();
  //   // LOG(INFO) << "Executor " << (is_replica_worker ? "Replica" : "") << this->id << " starts with thread id" << gettid();

  //   // last_commit = std::chrono::steady_clock::now();
  //   uint64_t last_seed = 0;
  //   // active_mp_limit = batch_per_worker /3;
  //   ExecutorStatus status;

  //   while ((status = static_cast<ExecutorStatus>(this->worker_status.load())) !=
  //          ExecutorStatus::START) {
  //     std::this_thread::yield();
  //   }

  //   this->n_started_workers.fetch_add(1);
    
  //   int cnt = 0;
    
  //   // worker_commit = 0;
  //   int try_times = 0;
  //   // executor_start_time = std::chrono::steady_clock::now();
  //   bool retry_transaction = false;
  //   int partition_id;
  //   bool is_sp = false;
  //   do {
  //     // drive_event_loop();
  //     process_request();
  //     status = static_cast<ExecutorStatus>(this->worker_status.load());
  //   } while (status != ExecutorStatus::STOP);
    
  //   // LOG(INFO) << "stats memory usage:\n" << memory_usage_from_stats();
  //   this->n_complete_workers.fetch_add(1);

  //   // once all workers are stopped, we need to process the replication
  //   // requests

  //   while (static_cast<ExecutorStatus>(this->worker_status.load()) !=
  //          ExecutorStatus::CLEANUP) {
  //     // drive_event_loop(false);
  //     process_request();
  //   }

  //   // drive_event_loop(false);
  //   process_request();
  //   this->n_complete_workers.fetch_add(1);
  //   LOG(INFO) << "Executor " << this->id << " exits.";
  // }


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
    // CHECK(partitioner->has_master_partition(partition_id));
    return partition_id;
  }


  void push_message(Message *message) override { 

    for (auto it = message->begin(); it != message->end(); it++) {
      auto messagePiece = *it;
      auto message_type = messagePiece.get_message_type();

      if (message_type == static_cast<int>(ControlMessage::ROUTER_TRANSACTION_RESPONSE)){
        size_t from = message->get_source_node_id();
        schedule_meta.router_transaction_done[from].fetch_sub(1);
        // LOG(INFO) << schedule_meta.router_transaction_done[from].load();
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

        // messageHandlers[type](messagePiece,
        //                       *sync_messages[message->get_source_node_id()],
        //                       *table, transaction.get());
        // LOG(INFO) << 
        if(type < controlMessageHandlers.size()){
          // transaction router from HStoreGenerator
          controlMessageHandlers[type](
            messagePiece,
            *sync_messages[message->get_source_node_id()], db,
            &router_transactions_queue,
            &router_stop_queue
          );
        } else {
          messageHandlers[type](messagePiece,
                              *sync_messages[message->get_source_node_id()], 
                              *table, transaction.get());
        }

        message_stats[type]++;
        message_sizes[type] += messagePiece.get_message_length();
      }

      size += message->get_message_count();
      flush_sync_messages();
    }
    return size;
  }

  void setupHandlers(TransactionType &txn){
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

  void flush_message(std::vector<std::unique_ptr<Message>> &messages, int i) {
    //for (auto i = 0u; i < messages.size(); i++) {
      if (i == (int)coordinator_id) {
        return;
      }

      if (messages[i]->get_message_count() == 0) {
        return;
      }

      auto message = messages[i].release();
      
      mm.lock();
      out_queue.push(message); // 单入单出的...
      mm.unlock();

      messages[i] = std::make_unique<Message>();
      init_message(messages[i].get(), i);
    // }
  }
  void flush_sync_messages() { flush_messages(sync_messages); }

  void flush_async_messages() { flush_messages(async_messages); }

  void init_message(Message *message, std::size_t dest_node_id) {
    message->set_source_node_id(coordinator_id);
    message->set_dest_node_id(dest_node_id);
    message->set_worker_id(id);
  }

protected:
  size_t generator_num;
  std::vector<int> generator_core_id;
  std::vector<int> dispatcher_core_id;

  std::vector<std::thread> dispatcher;

  std::vector<int> router_send_txn_cnt;

  std::mutex mm;
  std::atomic<uint32_t> router_transactions_send, router_transaction_done;

  DatabaseType &db;
  const ContextType &context;
  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;

  hstore::ScheduleMeta &schedule_meta;

  ShareQueue<simpleTransaction*, 40960> transactions_queue_self[MAX_COORDINATOR_NUM];
  StorageType storages[MAX_COORDINATOR_NUM];
  std::atomic<uint32_t> is_full_signal_self[MAX_COORDINATOR_NUM];
  std::atomic<int> coordinator_send[MAX_COORDINATOR_NUM];

  std::unique_ptr<Partitioner> partitioner;
  RandomType random;
  ProtocolType protocol;
  WorkloadType workload;
  std::unique_ptr<Delay> delay;
  Percentile<int64_t> commit_latency, write_latency;
  Percentile<int64_t> dist_latency, local_latency;
  std::unique_ptr<TransactionType> transaction;
  std::vector<std::unique_ptr<Message>> sync_messages, async_messages;
  std::vector<std::unique_ptr<std::mutex>> messages_mutex;

  ShareQueue<simpleTransaction> router_transactions_queue;
  std::deque<int> router_stop_queue;

  std::vector<
      std::function<void(MessagePiece, Message &, ITable &, TransactionType *)>>
      messageHandlers;

  std::vector<
    std::function<void(MessagePiece, Message &, DatabaseType &, ShareQueue<simpleTransaction>*, std::deque<int>*)>>
    controlMessageHandlers;    

  std::vector<std::size_t> message_stats, message_sizes;
  LockfreeQueue<Message *, 500860> in_queue, out_queue;


  int dispatcher_num;
  int cur_txn_num;

  bool is_replica_worker = false;
  int cluster_worker_num = -1;
  int active_replica_worker_num_end = -1;
  // int dispatcher_id;
  int replica_cluster_worker_id = -1;
};

} // namespace star