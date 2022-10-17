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

template <class Workload, class Protocol> class MyClayGenerator : public Worker {
public:
  using WorkloadType = Workload;
  using ProtocolType = Protocol;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using TransactionType = typename WorkloadType::TransactionType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using MessageType = typename ProtocolType::MessageType;
  using MessageFactoryType = MyClayMessageFactory;
  using MessageHandlerType = MyClayMessageHandler<DatabaseType>;

  using StorageType = typename WorkloadType::StorageType;

  MyClayGenerator(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
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
        workload(coordinator_id, db, random, *partitioner, start_time),
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
  }

  bool prepare_transactions_to_run(WorkloadType& workload, StorageType& storage){
    /** 
     * @brief 准备需要的txns
     * @note add by truth 22-01-24
     */
      std::size_t hot_area_size = context.partition_num / context.coordinator_num;

      std::size_t partition_id = random.uniform_dist(0, context.partition_num - 1); // get_random_partition_id(n, context.coordinator_num);
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

  std::unordered_map<int, int> txn_nodes_involved(simpleTransaction* t, int& max_node, bool is_dynamic) {
      std::unordered_map<int, int> from_nodes_id;
      size_t ycsbTableID = ycsb::ycsb::tableID;
      auto query_keys = t->keys;
      int max_cnt = 0;

      for (size_t j = 0 ; j < query_keys.size(); j ++ ){
        // LOG(INFO) << "query_keys[j] : " << query_keys[j];
        // judge if is cross txn
        size_t cur_c_id = -1;
        if(is_dynamic){
          // look-up the dynamic router to find-out where
          cur_c_id = db.get_dynamic_coordinator_id(context.coordinator_num, ycsbTableID, (void*)& query_keys[j]);
        } else {
          // cal the partition to figure out the coordinator-id
          cur_c_id = query_keys[j] / context.keysPerPartition % context.coordinator_num;
        }
        if(!from_nodes_id.count(cur_c_id)){
          from_nodes_id[cur_c_id] = 1;
        } else {
          from_nodes_id[cur_c_id] += 1;
        }
        if(from_nodes_id[cur_c_id] > max_cnt){
          max_cnt = from_nodes_id[cur_c_id];
          max_node = cur_c_id;
        }
      }
     return from_nodes_id;
   }

  int select_best_node(simpleTransaction* t) {
    
    int max_node = -1;
    // if(t->is_distributed){
      std::unordered_map<int, int> result;
      result = txn_nodes_involved(t, max_node, true);
    // } else {
    //   max_node = t->partition_id % context.coordinator_num;
    // }

    DCHECK(max_node != -1);
    return max_node;
  }
  
  int router_transmit_request(group_commit::ShareQueue<std::shared_ptr<myMove<WorkloadType>>>& move_plans){
    // transmit_request_queue
    auto new_transmit_generate = [&](int n){
      simpleTransaction* s = new simpleTransaction();
      s->is_transmit_request = true;
      s->partition_id = n;
      return s;
    };
    // pull request
    std::vector<simpleTransaction*> transmit_requests(context.coordinator_num);
    for(size_t i = 0 ; i < transmit_requests.size(); i ++ ){
      transmit_requests[i] = new_transmit_generate(i);
    }

    const int transmit_block_size = 100;

    int cur_move_size = move_plans.size();
    int move_size = 0;
    // pack up move-steps to transmit request
    for(int i = 0 ; i < cur_move_size; i ++ ){
      bool success = false;
      std::shared_ptr<myMove<WorkloadType>> cur_move = move_plans.pop_no_wait(success);
      DCHECK(success == true);

      for(auto move_record: cur_move->records){
        if(move_record.src_coordinator_id != cur_move->dest_coordinator_id){
          //
          if(move_size == 0){
            move_size = 1;
          }
          transmit_requests[cur_move->dest_coordinator_id]->keys.push_back(move_record.record_key_);
          transmit_requests[cur_move->dest_coordinator_id]->update.push_back(true);

          if(transmit_requests[cur_move->dest_coordinator_id]->keys.size() > transmit_block_size){
            // added to the router
            transmit_request_queue.push_no_wait(transmit_requests[cur_move->dest_coordinator_id]);
            transmit_requests[cur_move->dest_coordinator_id] = new_transmit_generate(cur_move->dest_coordinator_id);
            move_size ++ ;
          }
        }
      }
    }

    for(size_t i = 0 ; i < transmit_requests.size(); i ++ ){
      if(transmit_requests[i]->keys.size() > 0){
        transmit_request_queue.push_no_wait(transmit_requests[i]);
      }
    }
    return move_size;
  }

  void start() override {

    LOG(INFO) << "MyClayGenerator " << id << " starts.";

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
    // wait 
    while(is_full_signal.load() == 0){
      std::this_thread::sleep_for(std::chrono::microseconds(5));
    }

    // clay: do the clustering 
    std::vector<std::thread> clay;
    my_clay = std::make_unique<Clay<WorkloadType>>(context, db, worker_status);
    clay.emplace_back(&Clay<WorkloadType>::start, my_clay.get());

    // transmiter: do the transfer for the clay and whole system
    std::vector<std::thread> transmiter;
    transmiter.emplace_back([&]() {
      ExecutorStatus status = static_cast<ExecutorStatus>(worker_status.load());
      while(status != ExecutorStatus::EXIT){
        // 
        bool is_movable = my_clay->movable_flag.load();
        if(!is_movable){
          while(my_clay->movable_flag.load() == false && status != ExecutorStatus::EXIT){
            std::this_thread::sleep_for(std::chrono::microseconds(5));
            status = static_cast<ExecutorStatus>(worker_status.load());
          }
        } 
        
        int num = router_transmit_request(my_clay->move_plans);
        if(num > 0){
          LOG(INFO) << "router transmit request " << num; 
        }
        my_clay->movable_flag.store(false);
        status = static_cast<ExecutorStatus>(worker_status.load());
      }
      LOG(INFO) << "transmiter " << " exits.";
    });


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
          for (auto &t : clay) {
            t.join();
          }
          for (auto &t : transmiter) {
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

      // thread to router the transaction generated by Generator
      std::vector<std::thread> threads;
      for (auto n = 0u; n < context.coordinator_num; n++) {
        threads.emplace_back([&](int n) {
          // 
          std::vector<int> router_send_txn(context.coordinator_num, 0);

          auto router_request = [&](size_t coordinator_id_dst, std::shared_ptr<simpleTransaction> txn) {
            messages_mutex[coordinator_id_dst]->lock();
            size_t router_size = ControlMessageFactory::new_router_transaction_message(
                *async_messages[coordinator_id_dst].get(), 0, *txn, 
                context.coordinator_id);
            flush_message(async_messages, coordinator_id_dst);
            messages_mutex[coordinator_id_dst]->unlock();

            router_send_txn[coordinator_id_dst]++;
            n_network_size.fetch_add(router_size);
            router_transactions_send.fetch_add(1);
          };
          int cnt1 = 0;
          int cnt2 = 0;
          size_t batch_size = (size_t)transactions_queue.size() < (size_t)context.batch_size ? (size_t)transactions_queue.size(): (size_t)context.batch_size;
          for(size_t i = 0; i < batch_size / context.coordinator_num; i ++ ){
            bool success = false;
            std::shared_ptr<simpleTransaction> txn(transactions_queue.pop_no_wait(success));
            DCHECK(success == true);
            // 
            if(txn->is_distributed)
              my_clay->push_txn(txn);
            // 
            size_t coordinator_id_dst = select_best_node(txn.get()); // txn->partition_id % context.coordinator_num;
            cnt1 ++ ;
            router_request(coordinator_id_dst, txn);
          }

          // transfer clay move
          size_t transmit_batch_size = (size_t)transmit_request_queue.size();
          for(size_t i = 0; i < transmit_batch_size / context.coordinator_num; i ++ ){
            bool success = false;
            std::shared_ptr<simpleTransaction> txn(transmit_request_queue.pop_no_wait(success));
            if(success == false){
              break;
            }
            size_t coordinator_id_dst = txn->partition_id % context.coordinator_num;
            if(txn->is_transmit_request){
              VLOG(DEBUG_V14) << " TRANSMIT " << txn->keys[0] << " " << txn->keys[1] << " -> " << coordinator_id_dst;
            }
            cnt2 ++ ;
            router_request(coordinator_id_dst, txn);
          }
          is_full_signal.store(0);
          VLOG(DEBUG_V14) << "Generator " << n << " send router " << router_send_txn[0] << " " << router_send_txn[1];
          // after router all txns, send the stop-SIGNAL
          for (auto l = 0u; l < context.coordinator_num; l++){
            if(l == context.coordinator_id){
              continue;
            }
            VLOG(DEBUG_V14) << "SEND ROUTER_STOP " << n << " -> " << l;
            messages_mutex[l]->lock();
            ControlMessageFactory::router_stop_message(*async_messages[l].get(), router_send_txn[l]);
            flush_message(async_messages, l);
            messages_mutex[l]->unlock();
          }
        }, n);

        if (context.cpu_affinity) {
          ControlMessageFactory::pin_thread_to_core(context, threads[n], generator_core_id[n]);
        }
      }

      for (auto &t : threads) {
        t.join();
      }

      LOG(INFO) << "router_transaction_to_coordinator: " << std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - test)
                           .count();
      test = std::chrono::steady_clock::now();
      
      router_fence(); // wait for coordinator to response

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
      } else if (message_type == static_cast<int>(ControlMessage::TRANSMIT)){
        transmit_request_response.fetch_sub(1);
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

        if(type < controlMessageHandlers.size()){
          // transaction router from Generator
          controlMessageHandlers[type](
            messagePiece,
            *sync_messages[message->get_source_node_id()], db,
            &router_transactions_queue,
            &router_stop_queue
          );
        } else {
          // messageHandlers[type](messagePiece,
          //                     *sync_messages[message->get_source_node_id()], 
          //                     *table, transaction.get());
          messageHandlers[type](messagePiece,
                              *sync_messages[message->get_source_node_id()], 
                              db, context, partitioner.get(),
                              transaction.get());
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
  std::unique_ptr<Clay<WorkloadType>> my_clay;
  std::atomic<uint32_t> transmit_request_response;

  ShareQueue<simpleTransaction*, 4096> transactions_queue;
  ShareQueue<simpleTransaction*, 4096> transmit_request_queue;

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

  std::deque<simpleTransaction> router_transactions_queue;
  // std::deque<simpleTransaction> transmit_request_queue;

  std::deque<int> router_stop_queue;

  std::vector<
      std::function<void(MessagePiece, Message &, DatabaseType &, const ContextType &,  Partitioner *, TransactionType *)>>
      messageHandlers;

  std::vector<
    std::function<void(MessagePiece, Message &, DatabaseType &, std::deque<simpleTransaction>*, std::deque<int>*)>>
    controlMessageHandlers;    

  std::vector<std::size_t> message_stats, message_sizes;
  LockfreeQueue<Message *, 10086> in_queue, out_queue;
};
} // namespace group_commit

} // namespace star