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

#include "core/Coordinator.h"
#include <mutex>          // std::mutex, std::lock_guard

namespace star {
namespace group_commit {


template <class Workload, class Protocol> class HermesGenerator : public Worker {
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

  HermesGenerator(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
           const ContextType &context, std::atomic<uint32_t> &worker_status,
           std::atomic<uint32_t> &n_complete_workers,
           std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        // partitioner(PartitionerFactory::create_partitioner(
        //     context.partitioner, coordinator_id, context.coordinator_num)),
        partitioner(coordinator_id, context.coordinator_num,
                    HermesHelper::string_to_vint(context.replica_group), db),
        random(reinterpret_cast<uint64_t>(this)),
        protocol(db, context, partitioner),
        workload(coordinator_id, worker_status, db, random, partitioner, start_time),
        n_lock_manager(HermesHelper::n_lock_manager(
            partitioner.replica_group_id, id,
            HermesHelper::string_to_vint(context.lock_manager))),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)) {

    for (auto i = 0u; i <= context.coordinator_num; i++) {
      sync_messages.emplace_back(std::make_unique<Message>());
      init_message(sync_messages[i].get(), i);

      async_messages.emplace_back(std::make_unique<Message>());
      init_message(async_messages[i].get(), i);

      messages_mutex.emplace_back(std::make_unique<std::mutex>());
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
    controlMessageHandlers = ControlMessageHandler<DatabaseType>::get_message_handlers();

    message_stats.resize(messageHandlers.size(), 0);
    message_sizes.resize(messageHandlers.size(), 0);

    router_transaction_done.store(0);
    router_transactions_send.store(0);

    is_full_signal.store(0);
    generator_core_id.resize(context.coordinator_num);

    generator_num = 1;
    txns_coord_cost.resize(context.batch_size, std::vector<int>(context.coordinator_num, 0));

    replica_num = partitioner.replica_num();

    ycsbTableID = ycsb::ycsb::tableID;
    // for(int i = 0; i < replica_num; i ++ ){
    //   router_table_vec.push_back(db.find_router_table(ycsbTableID, i));
    // }
  }

  bool prepare_transactions_to_run(WorkloadType& workload, StorageType& storage){
    /** 
     * @brief 准备需要的txns
     * @note add by truth 22-01-24
     */
      std::size_t hot_area_size = context.partition_num / context.coordinator_num;

      std::size_t partition_id = random.uniform_dist(0, context.partition_num - 1); // get_random_partition_id(n, context.coordinator_num);

      size_t skew_factor = random.uniform_dist(1, 100);
      if (context.skew_factor >= skew_factor) {
        // 0 >= 50 
        partition_id = 0;
      } else {
        // 0 < 50
        //正常
      }
      
      std::size_t partition_id_ = partition_id / hot_area_size * hot_area_size + 
                                  partition_id / hot_area_size % context.coordinator_num; // get_partition_id();
      std::unique_ptr<TransactionType> cur_transaction = workload.next_transaction(context, partition_id_, storage);
      
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
    
    // VLOG_IF(DEBUG_V6, id == 0) << "transactions_queue: " << transactions_queue.size();
    return transactions_queue.push_no_wait(txn);
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

  void txn_replica_involved(simpleTransaction* t, int replica_id) {
    auto query_keys = t->keys;
    // 
    int replica_master_coordinator[2] = {0};
  
    int max_cnt = 0;
    int replica_destination = -1;
    
    // check master num at this replica on each node
    size_t from_nodes_id[20] = {0};
    size_t master_max_cnt = 0;
    for (size_t j = 0 ; j < query_keys.size(); j ++ ){
      // LOG(INFO) << "query_keys[j] : " << query_keys[j];
      // look-up the dynamic router to find-out where
      size_t cur_c_id = db.get_dynamic_coordinator_id(context.coordinator_num, 
                                            ycsbTableID, 
                                            (void*)& query_keys[j], replica_id);

      from_nodes_id[cur_c_id] += 1;

      if(from_nodes_id[cur_c_id] > master_max_cnt){
        master_max_cnt = from_nodes_id[cur_c_id];
        replica_destination = cur_c_id;
      }
    }

    t->on_replica_id = replica_id;
    t->destination_coordinator = replica_destination;
    if(master_max_cnt == query_keys.size()){
      t->is_real_distributed = false;
    } else {
      t->is_real_distributed = true;
    }
    // LOG(INFO) << t->idx_ << " " << t->keys[0] << " " << t->keys[1] << " " << replica_id;
    return;
   }

  // void txn_nodes_involved(simpleTransaction* txn) {
  //     int from_nodes_id[20] = {0};
  //     size_t ycsbTableID = ycsb::ycsb::tableID;
  //     auto query_keys = txn->keys;
  //     int max_cnt = 0;
  //     txn->destination_coordinator = -1;
  //     for (size_t j = 0 ; j < query_keys.size(); j ++ ){
  //       // LOG(INFO) << "query_keys[j] : " << query_keys[j];
  //       // look-up the dynamic router to find-out where
  //       size_t cur_c_id = db.get_dynamic_coordinator_id(context.coordinator_num, 
  //                                             ycsbTableID, 
  //                                             (void*)& query_keys[j], 
  //                                             txn->on_replica_id);

  //       from_nodes_id[cur_c_id] += 1;

  //       if(from_nodes_id[cur_c_id] > max_cnt){
  //         max_cnt = from_nodes_id[cur_c_id];
  //         txn->destination_coordinator = cur_c_id;
  //       }
  //     }
  //    return;
  //  }

  /// @brief pick the ideal destination for migration
  // void router_planning(simpleTransaction* txn){
  //   // for (size_t i = 0; i < transactions.size(); i ++) {
  //     // generate transaction
  //     // auto& txn = transactions[i];
  //     auto all_coords = txn_nodes_involved(txn);
  //     DCHECK(all_coords.size() > 0);
  //     // simply choose one
  //     //!TODO planning  
  //     DCHECK(0 <= txn->destination_coordinator && 
  //                 txn->destination_coordinator < context.coordinator_num);
  //   // }
  // }

  int pin_thread_id_ = 3;
  void router_request(std::vector<int>& router_send_txn_cnt, simpleTransaction* txn) {
  // auto router_request = [&]( std::shared_ptr<simpleTransaction> txn) {
    for(size_t r = 0 ; r < replica_num; r ++ ){
    
      txn_replica_involved(txn, r);

      t_1 += std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::steady_clock::now() - t_start)
                           .count();
      t_start = std::chrono::steady_clock::now();

      int i = txn->destination_coordinator;
      // router transaction to coordinators
      messages_mutex[i]->lock();
      size_t router_size = ControlMessageFactory::new_router_transaction_message(
          *async_messages[i].get(), 0, *txn, 
          context.coordinator_id);
      flush_message(async_messages, i);
    messages_mutex[i]->unlock();
      // LOG(INFO) << "TXN : " << txn->keys[0] << " " << txn->keys[1] << " -> " << coordinator_id_dst;
      router_send_txn_cnt[i]++;
      n_network_size.fetch_add(router_size);
      router_transactions_send.fetch_add(1);

      t_3 += std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::steady_clock::now() - t_start)
                           .count();
      t_start = std::chrono::steady_clock::now();
    }
  };





  void start() override {

    LOG(INFO) << "HermesGenerator " << id << " starts.";
    start_time = std::chrono::steady_clock::now();
    workload.start_time = start_time;
    
    StorageType storage;
    uint64_t last_seed = 0;

    // transaction only commit in a single group

    std::queue<std::unique_ptr<simpleTransaction>> q;
    std::size_t count = 0;


    // generators
    std::vector<std::thread> generators;
    for (auto n = 0u; n < generator_num; n++) {
      generators.emplace_back([&](int n) {
        ExecutorStatus status = static_cast<ExecutorStatus>(worker_status.load());
        while(status != ExecutorStatus::EXIT){
          // 
          bool is_not_full = prepare_transactions_to_run(workload, storage);
          if(!is_not_full){
            is_full_signal.store(1);
            while(is_full_signal.load() == 1 && status != ExecutorStatus::EXIT){
              std::this_thread::sleep_for(std::chrono::microseconds(5));
              status = static_cast<ExecutorStatus>(worker_status.load());
            }
          }
          status = static_cast<ExecutorStatus>(worker_status.load());
        }
        LOG(INFO) << "generators " << n << " exits.";
      }, n);

      if (context.cpu_affinity) {
        generator_core_id[n] = ControlMessageFactory::pin_thread_to_core(context, generators[n]);
      }
    }


    
    // for(size_t n = 0 ; n < context.coordinator_num; n ++ ){
      while(is_full_signal.load() == 0){
        std::this_thread::sleep_for(std::chrono::microseconds(5));
      }
    // }
    // main loop
    for (;;) {
      ExecutorStatus status;
      do {
        // exit 
        status = static_cast<ExecutorStatus>(worker_status.load());
        process_request();
        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "Executor " << id << " exits.";
            
          for (auto &t : generators) {
            t.join();
          }
  
          return;
        }
      } while (status != ExecutorStatus::Analysis);

      while (!q.empty()) {
        auto &ptr = q.front();
        // auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
        //                    std::chrono::steady_clock::now() - ptr->startTime)
        //                    .count();
        // commit_latency.add(latency);
        n_commit.fetch_add(1);
        q.pop();
      }

      n_started_workers.fetch_add(1);
      
      auto test = std::chrono::steady_clock::now();

      
      // // prepare_transactions_to_run(workload, storage);
      // LOG(INFO) << "prepare_transactions_to_run: " << std::chrono::duration_cast<std::chrono::microseconds>(
      //                      std::chrono::steady_clock::now() - test)
      //                      .count();
      // test = std::chrono::steady_clock::now();


      VLOG_IF(DEBUG_V, id==0) << "worker " << id << " ready to process_request";

      // std::string res = "";
      // for(size_t i  = 0 ; i < context.coordinator_num; i ++ ){
      //   res += std::to_string(transactions_queue[i].size()) + " ";
      // }
      // LOG(INFO) << res;

      if (id < n_lock_manager) {
            std::vector<int> router_send_txn_cnt(context.coordinator_num, 0);
            size_t batch_size = (size_t)transactions_queue.size() < (size_t)context.batch_size ? (size_t)transactions_queue.size(): (size_t)context.batch_size;

            t_start = std::chrono::steady_clock::now();
            t_1 = 0;
            t_2 = 0;
            t_3 = 0;
            for(size_t i = 0; i < batch_size; i ++ ){
              bool success = false;
              std::unique_ptr<simpleTransaction> txn(transactions_queue.pop_no_wait(success));
              DCHECK(success == true);
              // txn->destination_coordinator = coordinator_id_dst;
              router_request(router_send_txn_cnt, txn.get());            
              q.push(std::move(txn));
            }

            LOG(INFO) << 1.0 * t_1 / 1000 / 1000 << " " << 1.0 * t_2 / 1000 / 1000 << " " << 1.0 * t_3 / 1000 / 1000;
            is_full_signal.store(0);
            // after router all txns, send the stop-SIGNAL
            for (auto l = 0u; l < context.coordinator_num; l++){
              if(l == context.coordinator_id){
                continue;
              }
              // LOG(INFO) << "SEND ROUTER_STOP " << n << " -> " << l;
              messages_mutex[l]->lock();
              ControlMessageFactory::router_stop_message(*async_messages[l].get(), router_send_txn_cnt[l]);
              flush_message(async_messages, l);
              messages_mutex[l]->unlock();
            }
      }

      LOG(INFO) << "router_transaction_to_coordinator: " << std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - test)
                           .count();
      test = std::chrono::steady_clock::now();
      
      // process replication request after all workers stop.
      process_request();

      flush_async_messages();

      n_complete_workers.fetch_add(1);

      // once all workers are stop, we need to process the replication
      // requests
      LOG(INFO) << "Analysis done, wait Execute";
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Analysis) {
        std::this_thread::yield();
      }


      LOG(INFO) << "Start Execute";
      n_started_workers.fetch_add(1);

      // while (static_cast<ExecutorStatus>(worker_status.load()) ==
      //        ExecutorStatus::Execute) {
      //   process_request();
      // }

      // router_fence(); // wait for coordinator to response

      LOG(INFO) << "wait for coordinator to response: " << std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - test)
                           .count();
      test = std::chrono::steady_clock::now();

      process_request();
      n_complete_workers.fetch_add(1);
    }
    // not end here!
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
    CHECK(partitioner.has_master_partition(partition_id));
    return partition_id;
  }


  void push_message(Message *message) override { 

    for (auto it = message->begin(); it != message->end(); it++) {
      auto messagePiece = *it;
      auto message_type = messagePiece.get_message_type();

      if (message_type == static_cast<int>(ControlMessage::ROUTER_TRANSACTION_RESPONSE)){
        router_transaction_done.fetch_add(1);
        // LOG(INFO) << "router_transaction_done: " << router_transaction_done.load() << " from " << message->get_source_node_id() << " " << message->get_dest_node_id();
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
        // ITable *table = db.find_table(messagePiece.get_table_id(),
        //                               messagePiece.get_partition_id());

        // messageHandlers[type](messagePiece,
        //                       *sync_messages[message->get_source_node_id()],
        //                       *table, transaction.get());
        // LOG(INFO) << 
        if(type < controlMessageHandlers.size()){
          // transaction router from HermesGenerator
          controlMessageHandlers[type](
            messagePiece,
            *sync_messages[message->get_source_node_id()], db,
            &router_transactions_queue,
            &router_stop_queue
          );
        } else {
            messageHandlers[type](messagePiece,
                                *sync_messages[message->get_source_node_id()], 
                                db, context, &partitioner,
                                // *table,
                                transactions);
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
  std::chrono::steady_clock::time_point t_start;
  int t_1;
  int t_2;
  int t_3;

  size_t replica_num;
  size_t ycsbTableID;

  // vector<ITable *> router_table_vec;

  ShareQueue<simpleTransaction*, 54096> transactions_queue;// [20];
  size_t generator_num;
  std::atomic<uint32_t> is_full_signal;// [20];

  std::vector<int> generator_core_id;
  std::mutex mm;
  std::atomic<uint32_t> router_transactions_send, router_transaction_done;

  DatabaseType &db;
  const ContextType &context;
  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  HermesPartitioner<WorkloadType> partitioner;
  RandomType random;
  ProtocolType protocol;
  WorkloadType workload;
  std::size_t n_lock_manager;
  std::unique_ptr<Delay> delay;
  Percentile<int64_t> commit_latency, write_latency;
  Percentile<int64_t> dist_latency, local_latency;
  std::unique_ptr<TransactionType> transaction;
  std::vector<std::unique_ptr<Message>> sync_messages, async_messages;
  std::vector<std::unique_ptr<std::mutex>> messages_mutex;

  std::deque<simpleTransaction> router_transactions_queue;
  std::deque<int> router_stop_queue;

  std::vector<
      std::function<void(MessagePiece, Message &, 
                         DatabaseType &, const ContextType &, Partitioner *,
                        //  ITable &,
                         std::vector<std::unique_ptr<TransactionType>> &)>>
      messageHandlers;

  std::vector<
    std::function<void(MessagePiece, Message &, DatabaseType &, std::deque<simpleTransaction>*, std::deque<int>*)>>
    controlMessageHandlers;    

  std::vector<std::size_t> message_stats, message_sizes;
  LockfreeQueue<Message *, 100860> in_queue, out_queue;
  std::vector<std::vector<int>> txns_coord_cost;
  std::vector<std::unique_ptr<TransactionType>> transactions;
};
} // namespace group_commit

} // namespace star