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

#include "core/Coordinator.h"
#include <mutex>          // std::mutex, std::lock_guard

namespace star {
namespace group_commit {

#define MAX_COORDINATOR_NUM 20


template <class Workload, class Protocol> class LionMetisGenerator : public Worker {
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

  int pin_thread_id_ = 3;

  LionMetisGenerator(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
           const ContextType &context, std::atomic<uint32_t> &worker_status,
           std::atomic<uint32_t> &n_complete_workers,
           std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        partitioner(std::make_unique<LionDynamicPartitioner<WorkloadType>>(
            coordinator_id, context.coordinator_num, db)),
        random(reinterpret_cast<uint64_t>(this)),
        // protocol(db, context, *partitioner),
        workload(coordinator_id, worker_status, db, random, *partitioner, start_time),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)) {
    // 
    for (auto i = 0u; i <= context.coordinator_num; i++) {
      sync_messages.emplace_back(std::make_unique<Message>());
      init_message(sync_messages[i].get(), i);

      // async_messages.emplace_back(std::make_unique<Message>());
      // init_message(async_messages[i].get(), i);

      metis_async_messages.emplace_back(std::make_unique<Message>());
      init_message(metis_async_messages[i].get(), i);

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

    
    outfile_excel.open("/home/star/data/metis_router.xls", std::ios::trunc); // ios::trunc
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
      static std::unordered_map<int, int> busy_;

      std::vector<int> coordi_nums_;
      size_t ycsbTableID = ycsb::ycsb::tableID;
      auto query_keys = t->keys;
      int max_cnt = INT_MIN;

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
          // 
          coordi_nums_.push_back(cur_c_id);
        } else {
          from_nodes_id[cur_c_id] += 1;
        }
        // if(!busy_.count(cur_c_id)){
        //   busy_[cur_c_id] = 0;
        // }
        // if(from_nodes_id[cur_c_id] + busy_[cur_c_id] > max_cnt){
        //   max_cnt = from_nodes_id[cur_c_id] + busy_[cur_c_id];
        //   max_node = cur_c_id;
        // }
        if(from_nodes_id[cur_c_id] > max_cnt){
          max_cnt = from_nodes_id[cur_c_id];
          max_node = cur_c_id;
        }
      }

      if(context.random_router){
        // 
        int coords_num = (int)from_nodes_id.size();
        int random_coord_id = random.uniform_dist(0, coords_num - 1);
        max_node = coordi_nums_[random_coord_id];
      }

      // busy_[max_node] -= 10;

     return from_nodes_id;
   }

  int select_best_node(simpleTransaction* t) {
    
    int max_node = -1;
    if(t->is_distributed){
      std::unordered_map<int, int> result;
      result = txn_nodes_involved(t, max_node, true);
    } else {
      max_node = t->partition_id % context.coordinator_num;
    }

    DCHECK(max_node != -1);
    return max_node;
  }
  

  int router_transmit_request(group_commit::ShareQueue<std::shared_ptr<myMove<WorkloadType>>>& move_plans){
    // transmit_request_queue
    auto new_transmit_generate = [&](int idx){ // int n
      simpleTransaction* s = new simpleTransaction();
      s->idx_ = idx;
      s->is_transmit_request = true;
      s->is_distributed = true;
      // s->partition_id = n;
      return s;
    };
    // pull request
    std::vector<simpleTransaction*> transmit_requests;
    static int transmit_idx = 0; // split into sub-transactions
    static int metis_transmit_idx = 0;

    const int transmit_block_size = 10;

    int cur_move_size = my_clay->move_plans.size();
    // pack up move-steps to transmit request
    for(int i = 0 ; i < cur_move_size; i ++ ){
      bool success = false;
      std::shared_ptr<myMove<WorkloadType>> cur_move;
      
      success = my_clay->move_plans.pop_no_wait(cur_move);
      DCHECK(success == true);
      
      auto new_txn = new_transmit_generate(transmit_idx ++ );
      auto metis_new_txn = new_transmit_generate(metis_transmit_idx ++ );

      for(auto move_record: cur_move->records){
          metis_new_txn->keys.push_back(move_record.record_key_);
          metis_new_txn->update.push_back(true);
      }
      int64_t coordinator_id_dst = select_best_node(metis_new_txn);

      for(auto move_record: cur_move->records){
          new_txn->keys.push_back(move_record.record_key_);
          new_txn->update.push_back(true);
          new_txn->destination_coordinator = coordinator_id_dst;
          new_txn->metis_idx_ = metis_new_txn->idx_;

          if(new_txn->keys.size() > transmit_block_size){
            // added to the router
            transmit_requests.push_back(new_txn);
            new_txn = new_transmit_generate(transmit_idx ++ );
          }
      }

      if(new_txn->keys.size() > 0){
        transmit_requests.push_back(new_txn);
      }
    }

    std::vector<int> router_send_txn_cnt(context.coordinator_num, 0);
    for(size_t i = 0 ; i < transmit_requests.size(); i ++ ){ // 
      // transmit_request_queue.push_no_wait(transmit_requests[i]);
      // int64_t coordinator_id_dst = select_best_node(transmit_requests[i]);      
      outfile_excel << "Send Metis migration transaction ID(" << transmit_requests[i]->idx_ << " " << transmit_requests[i]->metis_idx_ << " " << transmit_requests[i]->keys[0] << " ) to " << transmit_requests[i]->destination_coordinator << "\n";

      metis_migration_router_request(router_send_txn_cnt, transmit_requests[i]);        
      // if(i > 5){ // debug
      //   break;
      // }
    }
    LOG(INFO) << "OMG transmit_requests.size() : " << transmit_requests.size();

    return cur_move_size;
  }


  // void router_request(std::vector<int>& router_send_txn_cnt, size_t coordinator_id_dst, std::shared_ptr<simpleTransaction> txn) {
  //   // router transaction to coordinators
  //   messages_mutex[coordinator_id_dst]->lock();
  //   size_t router_size = ControlMessageFactory::new_router_transaction_message(
  //       *async_messages[coordinator_id_dst].get(), 0, *txn, 
  //       context.coordinator_id);
  //   flush_message(async_messages, coordinator_id_dst);
  //   messages_mutex[coordinator_id_dst]->unlock();

  //   router_send_txn_cnt[coordinator_id_dst]++;
  //   n_network_size.fetch_add(router_size);
  //   router_transactions_send.fetch_add(1);
  // };

  void metis_migration_router_request(std::vector<int>& router_send_txn_cnt, simpleTransaction* txn) {
    // router transaction to coordinators
    uint64_t coordinator_id_dst = txn->destination_coordinator;
    messages_mutex[coordinator_id_dst]->lock();
    size_t router_size = LionMessageFactory::metis_migration_transaction_message(
        *metis_async_messages[coordinator_id_dst].get(), 0, *txn, 
        context.coordinator_id);
    flush_message(metis_async_messages, coordinator_id_dst);
    messages_mutex[coordinator_id_dst]->unlock();

    n_network_size.fetch_add(router_size);
  };

  void start() override {

    LOG(INFO) << "LionMetisGenerator " << id << " starts.";
    start_time = std::chrono::steady_clock::now();
    workload.start_time = start_time;
    
    StorageType storage;
    uint64_t last_seed = 0;

    // transaction only commit in a single group

    std::queue<std::unique_ptr<TransactionType>> q;
    std::size_t count = 0;

    // 
    auto trace_log = std::chrono::steady_clock::now();

    // transmiter: do the transfer for the clay and whole system
    // std::vector<std::thread> transmiter;
    // transmiter.emplace_back([&]() {
        my_clay = std::make_unique<Clay<WorkloadType>>(context, db, worker_status);

        ExecutorStatus status = static_cast<ExecutorStatus>(worker_status.load());

        int last_timestamp_int = 0;
        int workload_num = 3;
        int total_time = workload_num * context.workload_time;

        auto last_timestamp_ = start_time;
        int trigger_time_interval = context.workload_time * 1000; // unit sec.

        int start_offset = 10 * 1000;
        // 
        while(status != ExecutorStatus::EXIT){
          process_request();
          status = static_cast<ExecutorStatus>(worker_status.load());
          auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::steady_clock::now() - last_timestamp_)
                                  .count();
          if(latency > start_offset){
            break;
          }
        }
        // 
        last_timestamp_ = std::chrono::steady_clock::now();
        // 

        while(status != ExecutorStatus::EXIT){
          process_request();
          status = static_cast<ExecutorStatus>(worker_status.load());

          auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::steady_clock::now() - last_timestamp_)
                                  .count();
          if(last_timestamp_int != 0 && latency < trigger_time_interval){
            // std::this_thread::sleep_for(std::chrono::microseconds(5));
            continue;
          }
          // directly jump into first phase
          auto begin = std::chrono::steady_clock::now();
          
          // my_clay->init_with_history("/home/star/data/result_test.xls", last_timestamp_int, last_timestamp_int + trigger_time_interval);
          char file_name_[256] = {0};
          int time_begin = (last_timestamp_int + trigger_time_interval) / 1000 % total_time;
          int time_end   = (last_timestamp_int + trigger_time_interval) / 1000 % total_time + context.workload_time;

          sprintf(file_name_, "/home/star/data/resultss_partition_%d_%d.xls", time_begin, time_end);
          LOG(INFO) << "start read from file";
          my_clay->metis_partiion_read_from_file(file_name_);
          LOG(INFO) << "read from file done";

          latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::steady_clock::now() - begin)
                                  .count();
          LOG(INFO) << "lion loading file" << file_name_ << ". Used " << latency << " ms.";

          last_timestamp_ = begin;
          last_timestamp_int += trigger_time_interval;
          begin = std::chrono::steady_clock::now();
          // my_clay->metis_partition_graph();

          latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - begin)
                              .count();
          LOG(INFO) << "lion with metis graph initialization finished. Used " << latency << " ms.";
        
          // std::vector<simpleTransaction*> transmit_requests(context.coordinator_num);
          int num = router_transmit_request(my_clay->move_plans);
          if(num > 0){
            LOG(INFO) << "router transmit request " << num; 
          }
          // break; // debug
        }
        LOG(INFO) << "transmiter " << " exits.";
    // });
      while(status != ExecutorStatus::EXIT){
        process_request();
        status = static_cast<ExecutorStatus>(worker_status.load());
      }

    // ControlMessageFactory::pin_thread_to_core(context, transmiter[0], pin_thread_id_);
    // pin_thread_id_ ++ ;


    // // main loop
    // for (;;) {
    //   ExecutorStatus status;
    //   do {
    //     // exit 
    //     status = static_cast<ExecutorStatus>(worker_status.load());

    //     if (status == ExecutorStatus::EXIT) {
    //       LOG(INFO) << "Metis-Generator-Executor " << id << " exits.";

    //       // for (auto &t : transmiter) {
    //       //   t.join();
    //       // }
    //       return;
    //     }
    //   } while (status != ExecutorStatus::C_PHASE);

    //   n_started_workers.fetch_add(1);
      
    //   auto test = std::chrono::steady_clock::now();
    //   VLOG(DEBUG_V) << "Metis-Generator " << id << " ready to process_request";
    //   // 
    //     // while(status != ExecutorStatus::EXIT){
    //       status = static_cast<ExecutorStatus>(worker_status.load());

    //       auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
    //                               std::chrono::steady_clock::now() - last_timestamp_)
    //                               .count();
    //       if(last_timestamp_int != 0 && latency < trigger_time_interval){
    //         std::this_thread::sleep_for(std::chrono::microseconds(5));
    //       } else {
    //         // directly jump into first phase
    //         auto begin = std::chrono::steady_clock::now();
            
    //         // my_clay->init_with_history("/home/star/data/result_test.xls", last_timestamp_int, last_timestamp_int + trigger_time_interval);
    //         char file_name_[256] = {0};
    //         sprintf(file_name_, "/home/star/data/resultss_partition_%d_%d.xls", (last_timestamp_int + trigger_time_interval) / 1000 % 80, (last_timestamp_int + trigger_time_interval * 2) / 1000 % 80);
    //         LOG(INFO) << "start read from file";
    //         my_clay->metis_partiion_read_from_file(file_name_);
    //         LOG(INFO) << "read from file done";

    //         latency = std::chrono::duration_cast<std::chrono::milliseconds>(
    //                                 std::chrono::steady_clock::now() - begin)
    //                                 .count();
    //         LOG(INFO) << "lion loading file" << file_name_ << ". Used " << latency << " ms.";

    //         last_timestamp_ = begin;
    //         last_timestamp_int += trigger_time_interval;
    //         begin = std::chrono::steady_clock::now();
    //         // my_clay->metis_partition_graph();

    //         latency = std::chrono::duration_cast<std::chrono::milliseconds>(
    //                             std::chrono::steady_clock::now() - begin)
    //                             .count();
    //         LOG(INFO) << "lion with metis graph initialization finished. Used " << latency << " ms.";
          
    //         // std::vector<simpleTransaction*> transmit_requests(context.coordinator_num);
    //         int num = router_transmit_request(my_clay->move_plans);
    //         if(num > 0){
    //           LOG(INFO) << "router transmit request " << num; 
    //         }
    //       }

    //     //   break; // debug
    //     // }
    //     // LOG(INFO) << "transmiter " << " exits.";


    //   n_complete_workers.fetch_add(1);
    //   VLOG(DEBUG_V) << "Metis-Generator " << id << " finish C_PHASE";

    //   while (static_cast<ExecutorStatus>(worker_status.load()) !=
    //     ExecutorStatus::S_PHASE) {
    //     process_request();
    //   }
    //   process_request();

    //   // s-phase
    //   n_started_workers.fetch_add(1);
    //   n_complete_workers.fetch_add(1);

    //   VLOG(DEBUG_V) << "Metis-Generator " << id << " finish S_PHASE";

    //   // router_fence(); // wait for coordinator to response

    //   LOG(INFO) << "Metis-Generator Fence: wait for coordinator to response: " << std::chrono::duration_cast<std::chrono::microseconds>(
    //                        std::chrono::steady_clock::now() - test)
    //                        .count();
    //   test = std::chrono::steady_clock::now();

    //   flush_async_messages();

      
    //   while (static_cast<ExecutorStatus>(worker_status.load()) ==
    //          ExecutorStatus::S_PHASE) {
    //     process_request();
    //   }

    //   VLOG(DEBUG_V) << "wait back "
    //           << std::chrono::duration_cast<std::chrono::milliseconds>(
    //                  std::chrono::steady_clock::now() - test)
    //                  .count()
    //           << " milliseconds.";
    //   test = std::chrono::steady_clock::now();


    //   // n_complete_workers has been cleared
    //   process_request();
    //   n_complete_workers.fetch_add(1);
    // }
    // // not end here!






    // // main loop
    // for (;;) {
    //   ExecutorStatus status;
    //   do {
    //     // exit 
    //     status = static_cast<ExecutorStatus>(worker_status.load());

    //     if (status == ExecutorStatus::EXIT) {
    //       LOG(INFO) << "Executor " << id << " exits.";

    //       for (auto &t : transmiter) {
    //         t.join();
    //       }
    //       return;
    //     }
    //   } while (status != ExecutorStatus::C_PHASE);

    //   // while (!q.empty()) {
    //   //   auto &ptr = q.front();
    //   //   auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
    //   //                      std::chrono::steady_clock::now() - ptr->startTime)
    //   //                      .count();
    //   //   commit_latency.add(latency);
    //   //   q.pop();
    //   // }

    //   n_started_workers.fetch_add(1);
      
    //   auto test = std::chrono::steady_clock::now();
    //   VLOG(DEBUG_V) << "Generator " << id << " ready to process_request";

    //   // thread to router the transaction generated by LionMetisGenerator
    //   // std::atomic<int> coordinator_send[MAX_COORDINATOR_NUM];
    //   // for(int i = 0 ; i < MAX_COORDINATOR_NUM; i ++ ){
    //   //   coordinator_send[i] = 0;
    //   // }
    //   // std::vector<std::thread> threads;
    //   // for (auto n = 0u; n < context.coordinator_num; n++) {
    //   //   threads.emplace_back([&](int n) {
    //   //     std::vector<int> router_send_txn_cnt(context.coordinator_num, 0);
        
    //   //     size_t batch_size = (size_t)transactions_queue.size() < (size_t)context.batch_size ? (size_t)transactions_queue.size(): (size_t)context.batch_size;  
    //   //     // batch_size = 0; // debug
    //   //     LOG(INFO) << "batch_size: " << std::to_string(batch_size);
    //   //     for(size_t i = 0; i < batch_size / context.coordinator_num; i ++ ){

    //   //       bool success = false;
    //   //       std::shared_ptr<simpleTransaction> txn(transactions_queue.pop_no_wait(success));
    //   //       DCHECK(success == true);

    //   //       if(context.lion_with_trace_log){
    //   //         // 
    //   //         out_.lock();
    //   //         auto current_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
    //   //                   std::chrono::steady_clock::now() - trace_log)
    //   //                   .count() * 1.0 / 1000;
    //   //         outfile_excel << current_timestamp << "\t";
    //   //         for(auto item: txn->keys){
    //   //           outfile_excel << item << "\t";
    //   //         }
    //   //         outfile_excel << "\n";
    //   //         out_.unlock();
    //   //       }


    //   //       size_t coordinator_id_dst = txn->partition_id % context.coordinator_num;
    //   //       // router_transaction_to_coordinator(txn, coordinator_id_dst); // c_txn send to coordinator
    //   //       if(txn->is_distributed){ 
    //   //         // static-distribute
    //   //         coordinator_id_dst = select_best_node(txn.get());
    //   //       } else {
    //   //         DCHECK(txn->is_distributed == 0);
    //   //       }

    //   //       coordinator_send[coordinator_id_dst] ++ ;
    //   //       router_request(router_send_txn_cnt, coordinator_id_dst, txn);            
    //   //     }
    //   //     is_full_signal.store(0);
    //   //     // 

    //   //     for (auto l = 0u; l < context.coordinator_num; l++){
    //   //       if(l == context.coordinator_id){
    //   //         continue;
    //   //       }
            
    //   //       // LOG(INFO) << "SEND ROUTER_STOP " << n << " -> " << l;
    //   //       messages_mutex[l]->lock();
    //   //       ControlMessageFactory::router_stop_message(*async_messages[l].get(), router_send_txn_cnt[l]);
    //   //       flush_message(async_messages, l);
    //   //       messages_mutex[l]->unlock();
    //   //     }
          
    //   //   }, n);

    //   //   if (context.cpu_affinity) {
    //   //     ControlMessageFactory::pin_thread_to_core(context, threads[n], pin_thread_id_);// , generator_core_id[n
    //   //     pin_thread_id_ ++; 
    //   //   }
    //   // }

    //   // for (auto &t : threads) {
    //   //   t.join();
    //   // }

    //   // for(int i = 0 ; i < context.coordinator_num; i ++ ){
    //   //   LOG(INFO) << "Coord[" << i << "]: " << coordinator_send[i];
    //   // }

    //   // LOG(INFO) << "router_transaction_to_coordinator: " << std::chrono::duration_cast<std::chrono::microseconds>(
    //   //                      std::chrono::steady_clock::now() - test)
    //   //                      .count();
    //   // test = std::chrono::steady_clock::now();

    //   n_complete_workers.fetch_add(1);
    //   VLOG(DEBUG_V) << "Generator " << id << " finish C_PHASE";

    //   while (static_cast<ExecutorStatus>(worker_status.load()) !=
    //     ExecutorStatus::S_PHASE) {
    //     process_request();
    //   }
    //   process_request();

    //   // s-phase
    //   n_started_workers.fetch_add(1);
    //   n_complete_workers.fetch_add(1);

    //   VLOG(DEBUG_V) << "Generator " << id << " finish S_PHASE";

    //   router_fence(); // wait for coordinator to response

    //   LOG(INFO) << "Generator Fence: wait for coordinator to response: " << std::chrono::duration_cast<std::chrono::microseconds>(
    //                        std::chrono::steady_clock::now() - test)
    //                        .count();
    //   test = std::chrono::steady_clock::now();

    //   flush_async_messages();

      
    //   while (static_cast<ExecutorStatus>(worker_status.load()) ==
    //          ExecutorStatus::S_PHASE) {
    //     process_request();
    //   }

    //   VLOG_IF(DEBUG_V, id==0) << "wait back "
    //           << std::chrono::duration_cast<std::chrono::milliseconds>(
    //                  std::chrono::steady_clock::now() - test)
    //                  .count()
    //           << " milliseconds.";
    //   test = std::chrono::steady_clock::now();


    //   // n_complete_workers has been cleared
    //   process_request();
    //   n_complete_workers.fetch_add(1);
    // }
    // // not end here!
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

    outfile_excel.close();
    
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
      // LOG(INFO) <<   message_type << " " << (message_type == static_cast<int>(ControlMessage::ROUTER_TRANSACTION_RESPONSE)) << " " << router_transaction_done.load();
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
          // transaction router from LionMetisGenerator
          controlMessageHandlers[type](
            messagePiece,
            *sync_messages[message->get_source_node_id()], db,
            &router_transactions_queue,
            &router_stop_queue
          );
        } else if(type < messageHandlers.size()){
          // transaction from LionExecutor
          messageHandlers[type](messagePiece,
                                *sync_messages[message->get_source_node_id()], 
                                sync_messages,
                                db, context, partitioner.get(),
                                transaction.get(), 
                                &router_transactions_queue,
                                &migration_transactions_queue);
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

      mm.lock();
      out_queue.push(message);
      mm.unlock();

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
  
  ShareQueue<simpleTransaction*, 14096> transactions_queue;// [20];
  ShareQueue<simpleTransaction*, 14096> transmit_request_queue;

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
  // ProtocolType protocol;
  WorkloadType workload;
  std::unique_ptr<Delay> delay;
  Percentile<int64_t> commit_latency, write_latency;
  Percentile<int64_t> dist_latency, local_latency;
  std::unique_ptr<TransactionType> transaction;
  std::vector<std::unique_ptr<Message>> sync_messages, async_messages, metis_async_messages;
  std::vector<std::unique_ptr<std::mutex>> messages_mutex;

  std::deque<simpleTransaction> router_transactions_queue;
  group_commit::ShareQueue<simpleTransaction> migration_transactions_queue;

  std::deque<int> router_stop_queue;

  std::vector<std::function<void(MessagePiece, Message &, std::vector<std::unique_ptr<Message>>&, 
                                 DatabaseType &, const ContextType &, Partitioner *, // add partitioner
                                 TransactionType *, 
                                 std::deque<simpleTransaction>*,
                                 group_commit::ShareQueue<simpleTransaction>*)>>
      messageHandlers;

  std::vector<
    std::function<void(MessagePiece, Message &, DatabaseType &, std::deque<simpleTransaction>* ,std::deque<int>* )>>
    controlMessageHandlers;    

  std::vector<std::size_t> message_stats, message_sizes;
  LockfreeQueue<Message *, 10086> in_queue, out_queue;

  std::ofstream outfile_excel;
  std::mutex out_;
};
} // namespace group_commit

} // namespace star