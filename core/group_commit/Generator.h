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


template <class Workload, class Protocol> class Generator : public Worker {
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

  Generator(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
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
  }

  bool prepare_transactions_to_run(WorkloadType& workload, StorageType& storage){
    /** 
     * @brief 准备需要的txns
     * @note add by truth 22-01-24
     */
    /* partition_num = 6, coordinator_num = 2, thread_num = 1
     ----
     Hotarea_0   
        0 <- partition_id
          1
        2 
     ----
     Hotarea_1
        4 <-
          5
        6
    */

    /* partition_num = 8, coordinator_num = 2, thread_num = 2
     ----
     Hotarea_0   
        0 <- partition_id
          1
        2 
          3
     ----
     Hotarea_1
        4 <-
          5
        6
          7
    */
      // first key locate at the first partition of every hot area
      std::size_t hot_area_size = context.partition_num / context.coordinator_num;

      DCHECK(context.partition_num % context.worker_num == 0);
      std::size_t partition_per_thread =  context.partition_num / context.worker_num;
      

      std::size_t partition_id = random.uniform_dist(0, context.partition_num - 1); 
                                // id * partition_per_thread + 
                                //  random.uniform_dist(0, partition_per_thread - 1); 
                                 // get_random_partition_id(n, context.coordinator_num);

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

  int pin_thread_id_ = 3;
  void router_request(std::vector<int>& router_send_txn_cnt, simpleTransaction* txn) {
  // auto router_request = [&]( std::shared_ptr<simpleTransaction> txn) {
    size_t coordinator_id_dst = txn->destination_coordinator;
    // router transaction to coordinators
    messages_mutex[coordinator_id_dst]->lock();
    size_t router_size = ControlMessageFactory::new_router_transaction_message(
        *async_messages[coordinator_id_dst].get(), 0, *txn, 
        context.coordinator_id);
    flush_message(async_messages, coordinator_id_dst);
    messages_mutex[coordinator_id_dst]->unlock();
    // LOG(INFO) << "TXN : " << txn->keys[0] << " " << txn->keys[1] << " -> " << coordinator_id_dst;
    router_send_txn_cnt[coordinator_id_dst]++;
    n_network_size.fetch_add(router_size);
    router_transactions_send.fetch_add(1);
  };


  void txn_nodes_involved(simpleTransaction* t, bool is_dynamic) {
    
      std::unordered_map<int, int> from_nodes_id;           // dynamic replica nums
      std::unordered_map<int, int> from_nodes_id_secondary; // secondary replica nums
      // std::unordered_map<int, int> nodes_cost;              // cost on each node
      std::vector<int> coordi_nums_;

      
      size_t ycsbTableID = ycsb::ycsb::tableID;
      auto query_keys = t->keys;


      for (size_t j = 0 ; j < query_keys.size(); j ++ ){
        // LOG(INFO) << "query_keys[j] : " << query_keys[j];
        // judge if is cross txn
        size_t cur_c_id = -1;
        size_t secondary_c_ids;
        if(is_dynamic){
          // look-up the dynamic router to find-out where
          auto router_table = db.find_router_table(ycsbTableID);// , master_coordinator_id);
          auto tab = static_cast<RouterValue*>(router_table->search_value((void*) &query_keys[j]));

          cur_c_id = tab->get_dynamic_coordinator_id();
          secondary_c_ids = tab->get_secondary_coordinator_id();
        } else {
          // cal the partition to figure out the coordinator-id
          cur_c_id = query_keys[j] / context.keysPerPartition % context.coordinator_num;
        }
        if(!from_nodes_id.count(cur_c_id)){
          from_nodes_id[cur_c_id] = 1;
          // 
          coordi_nums_.push_back(cur_c_id);
        } else {
          from_nodes_id[cur_c_id] += 1;
        }

        // key on which node
        for(size_t i = 0; i <= context.coordinator_num; i ++ ){
            if(secondary_c_ids & 1 && i != cur_c_id){
                from_nodes_id_secondary[i] += 1;
            }
            secondary_c_ids = secondary_c_ids >> 1;
        }
      }

      int max_cnt = INT_MIN;
      int max_node = -1;

      for(size_t cur_c_id = 0 ; cur_c_id < context.coordinator_num; cur_c_id ++ ){
        int cur_score = 0;
        size_t cnt_master = from_nodes_id[cur_c_id];
        size_t cnt_secondary = from_nodes_id_secondary[cur_c_id];
        if(cnt_master == query_keys.size()){
          cur_score = 100 * (int)query_keys.size();
        // } else if(cnt_secondary + cnt_master == query_keys.size()){
        //   cur_score = 50 * cnt_master;// + 25 * cnt_secondary;
        } else {
          cur_score = 25 * cnt_master;// + 15 * cnt_secondary;
        }
        if(cur_score > max_cnt){
          max_node = cur_c_id;
          max_cnt = cur_score;
        }
        txns_coord_cost[t->idx_][cur_c_id] = 10 * (int)query_keys.size() - cur_score;
      }


      if(context.random_router > 0){
        // 
        int coords_num = (int)coordi_nums_.size();
        size_t random_value = random.uniform_dist(0, 100);
        if(random_value > context.random_router){
          size_t random_coord_id = random.uniform_dist(0, coords_num - 1);
          if(random_coord_id > context.coordinator_num){
            VLOG(DEBUG_V8) << "bad  " << t->keys[0] << " " << t->keys[1] << " router to -> " << max_node << " " << from_nodes_id[max_node] << " " << coordi_nums_[random_coord_id] << " " << from_nodes_id[coordi_nums_[random_coord_id]];
          }
          max_node = coordi_nums_[random_coord_id];
        }
      } 


      t->destination_coordinator = max_node;
      t->execution_cost = 10 * (int)query_keys.size() - max_cnt;

     return;
   }


  void start() override {

    LOG(INFO) << "Generator " << id << " starts.";
    start_time = std::chrono::steady_clock::now();
    workload.start_time = start_time;
    
    StorageType storage;
    uint64_t last_seed = 0;

    // transaction only commit in a single group

    std::queue<std::unique_ptr<TransactionType>> q;
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

        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "Executor " << id << " exits.";
            
          for (auto &t : generators) {
            t.join();
          }
  
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

      // thread to router the transaction generated by Generator
      std::vector<std::thread> threads;
      // for (auto n = 0u; n < context.coordinator_num; n++) {
      //   threads.emplace_back([&](int n) {
          std::vector<int> router_send_txn_cnt(context.coordinator_num, 0);
          size_t batch_size = (size_t)transactions_queue.size() < (size_t)context.batch_size ? (size_t)transactions_queue.size(): (size_t)context.batch_size;

          // / context.coordinator_num
          // if(id == 0)
            for(size_t i = 0; i < batch_size / context.worker_num ; i ++ ){
              bool success = false;
              std::unique_ptr<simpleTransaction> txn(transactions_queue.pop_no_wait(success));
              DCHECK(success == true);
              txn_nodes_involved(txn.get(), false);
              // txn->destination_coordinator = coordinator_id_dst;
              router_request(router_send_txn_cnt, txn.get());            

            }
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
        // }, n);

        // if (context.cpu_affinity) {
        //   ControlMessageFactory::pin_thread_to_core(context, threads[n], pin_thread_id_);
        //   pin_thread_id_ ++;
        // }
      // }

      for (auto &t : threads) {
        t.join();
      }

      LOG(INFO) << "router_transaction_to_coordinator: " << std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - test)
                           .count();
      test = std::chrono::steady_clock::now();
      
      // router_fence(); // wait for coordinator to response

      LOG(INFO) << "wait for coordinator to response: " << std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - test)
                           .count();
      test = std::chrono::steady_clock::now();

      // process replication request after all workers stop.
      process_request();

      flush_async_messages();

      n_complete_workers.fetch_add(1);

      // once all workers are stop, we need to process the replication
      // requests

      while (static_cast<ExecutorStatus>(worker_status.load()) !=
             ExecutorStatus::CLEANUP) {
        process_request();
      }

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
    CHECK(partitioner->has_master_partition(partition_id));
    return partition_id;
  }


  void push_message(Message *message) override { 

    for (auto it = message->begin(); it != message->end(); it++) {
      auto messagePiece = *it;
      auto message_type = messagePiece.get_message_type();

      if (message_type == static_cast<int>(ControlMessage::ROUTER_TRANSACTION_RESPONSE)){
        router_transaction_done.fetch_add(1);
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
          // transaction router from Generator
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
  ShareQueue<simpleTransaction*, 140960> transactions_queue;// [20];
  size_t generator_num;
  std::atomic<uint32_t> is_full_signal;// [20];

  std::vector<int> generator_core_id;
  std::mutex mm;
  std::atomic<uint32_t> router_transactions_send, router_transaction_done;

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
  LockfreeQueue<Message *, 100860> in_queue, out_queue;
  std::vector<std::vector<int>> txns_coord_cost;
};
} // namespace group_commit

} // namespace star