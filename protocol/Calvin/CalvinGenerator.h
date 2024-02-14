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

#include "protocol/Calvin/CalvinMeta.h"

#include "core/Coordinator.h"
#include <mutex>          // std::mutex, std::lock_guard

namespace star {
namespace group_commit {


template <class Workload, class Protocol> class CalvinGenerator : public Worker {
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

  CalvinGenerator(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
           const ContextType &context, std::atomic<uint32_t> &worker_status,
           std::atomic<uint32_t> &n_complete_workers,
           std::atomic<uint32_t> &n_started_workers,
           calvin::ScheduleMeta &schedule_meta)
      : Worker(coordinator_id, id), db(db), context(context),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        schedule_meta(schedule_meta),
        partitioner(coordinator_id, context.coordinator_num,
                    CalvinHelper::string_to_vint(context.replica_group)),
        random(reinterpret_cast<uint64_t>(this)),
        protocol(db, context, partitioner),
        workload(coordinator_id, worker_status, db, random, partitioner, start_time),
        n_lock_manager(CalvinHelper::n_lock_manager(
            partitioner.replica_group_id, id,
            CalvinHelper::string_to_vint(context.lock_manager))),
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
          
            dispatcher.emplace_back([&](int n, int worker_id) {
              
              ExecutorStatus status = static_cast<ExecutorStatus>(worker_status.load());
              do {
                status = static_cast<ExecutorStatus>(worker_status.load());
                std::this_thread::sleep_for(std::chrono::microseconds(5));
              } while (status != ExecutorStatus::Analysis);  
            

            int dispatcher_id  = worker_id * context.coordinator_num + n;
            while(is_full_signal_self[dispatcher_id].load() == false){
                bool success = prepare_transactions_to_run(workload, storages[dispatcher_id],
                                      transactions_queue_self[dispatcher_id]
                                    );
                if(!success){ // full
                    is_full_signal_self[dispatcher_id].store(true);
                } 
            }
            
            while(status != ExecutorStatus::EXIT){
                  // wait for start
                  while(schedule_meta.start_schedule.load() == 0 && status != ExecutorStatus::EXIT){
                    status = static_cast<ExecutorStatus>(worker_status.load());
                    if(is_full_signal_self[dispatcher_id].load() == true){
                      std::this_thread::sleep_for(std::chrono::microseconds(5));
                      continue;
                    }

                    bool success = prepare_transactions_to_run(workload, storages[dispatcher_id],
                      transactions_queue_self[dispatcher_id]
                    );
                    if(!success){ // full
                      is_full_signal_self[dispatcher_id].store(true);
                    }                    
                  }
                  std::vector<std::shared_ptr<simpleTransaction>> &txns = schedule_meta.node_txns;

                  scheduler_transactions(dispatcher_num, dispatcher_id);

                  int idx_offset = dispatcher_id * cur_txn_num;

                  for(int j = 0; j < cur_txn_num; j ++ ){
                    int idx = idx_offset + j;
                    txns[idx]->idx_ = idx;

                    for(size_t k = 0 ; k < context.coordinator_num; k ++ ){
                      if(k == context.coordinator_id) continue;
                      txns[idx]->destination_coordinator = k;
                      coordinator_send[txns[idx]->destination_coordinator] ++ ;
                      router_request(router_send_txn_cnt, txns[idx]);   
                    }
                    if(j % context.batch_flush == 0){
                      for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
                        messages_mutex[i]->lock();
                        flush_message(async_messages, i);
                        messages_mutex[i]->unlock();
                      }
                    }
                  }
                  for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
                    messages_mutex[i]->lock();
                    flush_message(async_messages, i);
                    messages_mutex[i]->unlock();
                  }

                  schedule_meta.done_schedule.fetch_add(1);
                  status = static_cast<ExecutorStatus>(worker_status.load());

                  // wait for end
                  while(schedule_meta.done_schedule.load() < context.worker_num * context.coordinator_num && status != ExecutorStatus::EXIT){
                    std::this_thread::sleep_for(std::chrono::microseconds(5));
                    status = static_cast<ExecutorStatus>(worker_status.load());
                  }

                  is_full_signal_self[dispatcher_id].store(false);

                  schedule_meta.start_schedule.store(0);
                }
            }, n, this->id);

            if (context.cpu_affinity) {
            LOG(INFO) << "dispatcher_core_id[n]: " << dispatcher_core_id[n] 
                      << " work_id" << this->id;
              ControlMessageFactory::pin_thread_to_core(context, dispatcher[n], dispatcher_core_id[n]);
            }
        } 
  }

  void txn_nodes_involved_tpcc(simpleTransaction* t, 
                          std::vector<std::vector<int>>& txns_coord_cost) {
      int from_nodes_id[MAX_COORDINATOR_NUM] = {0};              // dynamic replica nums
      // std::unordered_map<int, int> from_nodes_id_secondary; // secondary replica nums
      // std::unordered_map<int, int> nodes_cost;              // cost on each node
      std::vector<int> query_keys;
      star::tpcc::NewOrderQuery keys;
      keys.unpack_transaction(*t);
      for(size_t i = 0 ; i < t->keys.size() - 3; i ++ ){
        size_t stock_coordinator_id = (keys.INFO[i].OL_SUPPLY_W_ID - 1) % context.coordinator_num;
        from_nodes_id[stock_coordinator_id] += 1;
        query_keys.push_back(stock_coordinator_id);
      }

      int max_cnt = INT_MIN;
      int max_node = -1;

      for(size_t cur_c_id = 0 ; cur_c_id < context.coordinator_num; cur_c_id ++ ){
        int cur_score = 0;
        size_t cnt_master = from_nodes_id[cur_c_id];
        // size_t cnt_secondary = from_nodes_id_secondary[cur_c_id];
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

      max_node = query_keys[0];


      // t->destination_coordinator = max_node;
      t->on_replica_id = max_node;
      t->execution_cost = 10 * (int)query_keys.size() - max_cnt;

     return;    
   }


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
    for(size_t i = 0; i < (size_t)cur_txn_num; i ++ ){
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

  bool prepare_transactions_to_run(WorkloadType& workload, StorageType& storage,
      ShareQueue<simpleTransaction*, 40960>& transactions_queue_self_){
    /** 
     * @brief 准备需要的txns
     * @note add by truth 22-01-24
     */
      std::size_t hot_area_size = context.coordinator_num;
      std::size_t partition_id = random.uniform_dist(0, context.partition_num - 1); // get_random_partition_id(n, context.coordinator_num);
      // 
      size_t skew_factor = random.uniform_dist(1, 100);
      if (context.skew_factor >= skew_factor) {
        // 0 >= 50 
        if(WorkloadType::which_workload == myTestSet::YCSB){
          partition_id = (0 + skew_factor * context.coordinator_num) % context.partition_num;
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

      // 
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
    

    return transactions_queue_self_.push_no_wait(txn); // txn->partition_id % context.coordinator_num [0]
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

  void router_request(std::vector<int>& router_send_txn_cnt, std::shared_ptr<simpleTransaction> txn) {
    // router transaction to coordinators
    size_t coordinator_id_dst = txn->destination_coordinator;

    messages_mutex[coordinator_id_dst]->lock();
    size_t router_size = ControlMessageFactory::new_router_transaction_message(
        *async_messages[coordinator_id_dst], 0, *txn, 
        RouterTxnOps::TRANSFER);
    // flush_message(async_messages, coordinator_id_dst);
    messages_mutex[coordinator_id_dst]->unlock();

    router_send_txn_cnt[coordinator_id_dst]++;
    n_network_size.fetch_add(router_size);
    router_transactions_send.fetch_add(1);
  };

  void txn_nodes_involved(simpleTransaction* t, 
                          std::vector<std::vector<int>>& txns_coord_cost) {
    
      int from_nodes_id[MAX_COORDINATOR_NUM] = {0};           // dynamic replica nums
      int from_nodes_id_secondary[MAX_COORDINATOR_NUM] = {0}; // secondary replica nums

      int max_cnt = INT_MIN;
      int max_node = -1;

      size_t ycsbTableID = ycsb::ycsb::tableID;
      auto query_keys = t->keys;


      for (size_t j = 0 ; j < query_keys.size(); j ++ ){
        // LOG(INFO) << "query_keys[j] : " << query_keys[j];
        // judge if is cross txn
        size_t cur_c_id = -1;
        size_t secondary_c_ids;

          // cal the partition to figure out the coordinator-id
        cur_c_id = query_keys[j] / context.keysPerPartition % context.coordinator_num;
        from_nodes_id[cur_c_id] += 1;


        // key on which node
        for(size_t i = 0; i <= context.coordinator_num; i ++ ){
            if(secondary_c_ids & 1 && i != cur_c_id){
                from_nodes_id_secondary[i] += 1;
            }
            secondary_c_ids = secondary_c_ids >> 1;
        }

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


      // if(context.random_router > 0){
        // 
        // int coords_num = (int)coordi_nums_.size();
        // size_t random_value = random.uniform_dist(0, 100);
        // if(random_value > context.random_router){
        //   size_t random_coord_id = random.uniform_dist(0, coords_num - 1);
        //   if(random_coord_id > context.coordinator_num){
        //     VLOG(DEBUG_V8) << "bad  " << t->keys[0] << " " << t->keys[1] << " router to -> " << max_node << " " << from_nodes_id[max_node] << " " << coordi_nums_[random_coord_id] << " " << from_nodes_id[coordi_nums_[random_coord_id]];
        //   }
        //   max_node = coordi_nums_[random_coord_id];
        // }
      // } 

      // max_node = query_keys[0] / context.keysPerPartition % context.coordinator_num;


      t->on_replica_id = max_node;
      t->execution_cost = 10 * (int)query_keys.size() - max_cnt;

     return;
   }

  void start() override {

    LOG(INFO) << "CalvinGenerator " << id << " starts.";
    start_time = std::chrono::steady_clock::now();
    workload.start_time = start_time;
    
    StorageType storage;
    uint64_t last_seed = 0;

    // transaction only commit in a single group

    std::queue<std::unique_ptr<simpleTransaction>> q;
    std::size_t count = 0;

    // main loop
    for (;;) {
      ExecutorStatus status;
      do {
        // exit 
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "Executor " << id << " exits.";
          
          for(auto& n: dispatcher){
            n.join();
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

      // thread to router the transaction generated by LionGenerator
      for(int i = 0 ; i < MAX_COORDINATOR_NUM; i ++ ){
        coordinator_send[i] = 0;
      }
      
      router_send_txn_cnt.resize(context.coordinator_num, 0);

      schedule_meta.start_schedule.store(1);
      // wait for end
      while(schedule_meta.done_schedule.load() < context.worker_num * context.coordinator_num && status != ExecutorStatus::EXIT){
        std::this_thread::sleep_for(std::chrono::microseconds(5));
        status = static_cast<ExecutorStatus>(worker_status.load());
      }

      auto cur_timestamp__ = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - test)
                  .count() * 1.0 / 1000;
      LOG(INFO) << "send : " << cur_timestamp__;

      // 
      for (auto l = 0u; l < context.coordinator_num; l++){
        if(l == context.coordinator_id){
          continue;
        }
        LOG(INFO) << "SEND ROUTER_STOP " << id << " -> " << l;
        messages_mutex[l]->lock();
        ControlMessageFactory::router_stop_message(*async_messages[l].get(), router_send_txn_cnt[l]);
        flush_message(async_messages, l);
        messages_mutex[l]->unlock();
      }

      for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
        LOG(INFO) << "Coord[" << i << "]: " << coordinator_send[i];
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
        std::this_thread::sleep_for(std::chrono::microseconds(5));
      }

      if(id == 0){
        schedule_meta.clear();
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
        ITable *table = db.find_table(messagePiece.get_table_id(),
                                      messagePiece.get_partition_id());

        // messageHandlers[type](messagePiece,
        //                       *sync_messages[message->get_source_node_id()],
        //                       *table, transaction.get());
        // LOG(INFO) << 
        if(type < controlMessageHandlers.size()){
          // transaction router from CalvinGenerator
          controlMessageHandlers[type](
            messagePiece,
            *sync_messages[message->get_source_node_id()], db,
            &router_transactions_queue,
            &router_stop_queue
          );
        } else {
          messageHandlers[type](messagePiece,
                                *sync_messages[message->get_source_node_id()], *table,
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
  calvin::ScheduleMeta &schedule_meta;

  ShareQueue<simpleTransaction*, 40960> transactions_queue_self[MAX_COORDINATOR_NUM];
  StorageType storages[MAX_COORDINATOR_NUM];
  std::atomic<uint32_t> is_full_signal_self[MAX_COORDINATOR_NUM];
  std::atomic<int> coordinator_send[MAX_COORDINATOR_NUM];


  CalvinPartitioner partitioner;
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

  ShareQueue<simpleTransaction> router_transactions_queue;
  std::deque<int> router_stop_queue;

  std::vector<
      std::function<void(MessagePiece, Message &, ITable &,
                         std::vector<std::unique_ptr<TransactionType>> &)>>
      messageHandlers;

  std::vector<
    std::function<void(MessagePiece, Message &, DatabaseType &, ShareQueue<simpleTransaction>*, std::deque<int>*)>>
    controlMessageHandlers;    

  std::vector<std::size_t> message_stats, message_sizes;
  LockfreeQueue<Message *, 100860> in_queue, out_queue;
  // std::vector<std::vector<int>> txns_coord_cost;
  std::vector<std::unique_ptr<TransactionType>> transactions;

  int dispatcher_num;
  int cur_txn_num;


};
} // namespace group_commit

} // namespace star