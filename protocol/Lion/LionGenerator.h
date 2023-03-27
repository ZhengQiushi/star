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


template <class Workload, class Protocol> class LionGenerator : public Worker {
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

  int pin_thread_id_ = 5;

  LionGenerator(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
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

    for (auto i = 0u; i <= context.coordinator_num; i++) {
      sync_messages.emplace_back(std::make_unique<Message>());
      init_message(sync_messages[i].get(), i);

      // for (auto j = 0u; j <= context.coordinator_num; j ++){
      //   async_messages[i].emplace_back(std::make_unique<Message>());
      //   init_message(async_messages[i][j].get(), j); // thread_id, coordinator_id
      // }
      async_messages.emplace_back(std::make_unique<Message>());
      init_message(async_messages[i].get(), i);

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
    generator_num = 1;

    generator_core_id.resize(context.coordinator_num);
    sender_core_id.resize(context.coordinator_num);

    for(int i = 0 ; i < generator_num; i ++ ){
      generator_core_id[i] = pin_thread_id_ ++ ;
    }
    for(int i = 0 ; i < context.coordinator_num; i ++ ){
      sender_core_id[i] = pin_thread_id_ ++ ;
    }

    for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      txns_coord_cost[i].resize(context.batch_size, std::vector<int>(context.coordinator_num, 0));
    }
    
    // for(int i = 0 ; i < txns_coord_cost.size(); i ++ ){
    //   txns_coord_cost[i] = std::make_unique<int[]>(context.coordinator_num);
    // }
    


    
    // if(context.lion_with_metis_init){
    //   LOG(INFO) << "lion with metis start to initialize the graph.";
    //   auto start_time = std::chrono::steady_clock::now(); 

    //   my_clay = std::make_unique<Clay<WorkloadType>>(context, db, worker_status);

    //   if(context.lion_with_metis_init == 1){
    //     my_clay->init_with_history("/home/star/data/result_test.xls");

    //     auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
    //                             std::chrono::steady_clock::now() - start_time)
    //                             .count();
    //     LOG(INFO) << "lion loading file. Used " << latency << " ms.";

    //     my_clay->metis_partition_graph();

    //     latency = std::chrono::duration_cast<std::chrono::milliseconds>(
    //                         std::chrono::steady_clock::now() - start_time)
    //                         .count();
    //     LOG(INFO) << "lion with metis graph initialization finished. Used " << latency << " ms.";
    //   } else {

    //     my_clay->metis_partiion_read_from_file();
    //     auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
    //                         std::chrono::steady_clock::now() - start_time)
    //                         .count();
    //     LOG(INFO) << "lion with metis graph initialization finished. Used " << latency << " ms.";

    //   }
    // }
  }

  bool prepare_transactions_to_run(WorkloadType& workload, StorageType& storage){
    /** 
     * @brief 准备需要的txns
     * @note add by truth 22-01-24
     */
      std::size_t hot_area_size = context.partition_num / context.coordinator_num;
      std::size_t partition_id = random.uniform_dist(0, context.partition_num - 1); // get_random_partition_id(n, context.coordinator_num);
      // 
      size_t skew_factor = random.uniform_dist(1, 100);
      if (context.skew_factor >= skew_factor) {
        // 0 >= 50 
        partition_id = 0;
      } else {
        // 0 < 50
        //正常
      }
      // 
      std::size_t partition_id_ = partition_id / hot_area_size * hot_area_size + 
                                  partition_id / hot_area_size % context.coordinator_num; // get_partition_id();

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
    

    // VLOG_IF(DEBUG_V6, id == 0) << "transactions_queue: " << transactions_queue.size();
    return transactions_queue.push_no_wait(txn); // txn->partition_id % context.coordinator_num [0]
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


  void txn_nodes_involved(simpleTransaction* t, bool is_dynamic, 
                          std::vector<std::vector<int>>& txns_coord_cost_) {
    
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
        if(context.migration_only){
          cur_score = 25 * cnt_master;
        } else {
          if(cnt_master == query_keys.size()){
            cur_score = 100 * (int)query_keys.size();
          } else if(cnt_secondary + cnt_master == query_keys.size()){
            cur_score = 50 * cnt_master + 25 * cnt_secondary;
          } else {
            cur_score = 25 * cnt_master + 15 * cnt_secondary;
          }
        }

        if(cur_score > max_cnt){
          max_node = cur_c_id;
          max_cnt = cur_score;
        }
        txns_coord_cost_[t->idx_][cur_c_id] = 10 * (int)query_keys.size() - cur_score;
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


      size_t cnt_master = from_nodes_id[max_node];
      size_t cnt_secondary = from_nodes_id_secondary[max_node];

      // if(cnt_secondary + cnt_master != query_keys.size()){
      //   distributed_outfile_excel << t->keys[0] << "\t" << t->keys[1] << "\t" << max_node << "\n";
      // }

     return;
   }


  void router_request(std::vector<int>& router_send_txn_cnt, std::shared_ptr<simpleTransaction> txn) {
            // router transaction to coordinators
            size_t coordinator_id_dst = txn->destination_coordinator;

            messages_mutex[coordinator_id_dst]->lock();
            size_t router_size = ControlMessageFactory::new_router_transaction_message(
                *async_messages[coordinator_id_dst].get(), 0, *txn, 
                context.coordinator_id);
            flush_message(async_messages, coordinator_id_dst);
            messages_mutex[coordinator_id_dst]->unlock();

            router_send_txn_cnt[coordinator_id_dst]++;
            n_network_size.fetch_add(router_size);
            router_transactions_send.fetch_add(1);
          };

  // void router_request(std::vector<int>& router_send_txn_cnt, std::shared_ptr<simpleTransaction> txn, int thread_id) {
  //   // router transaction to coordinators
  //   size_t coordinator_id_dst = txn->destination_coordinator;
  //   // messages_mutex[coordinator_id_dst]->lock();
  //   size_t router_size = ControlMessageFactory::new_router_transaction_message(
  //       *async_messages[thread_id][coordinator_id_dst].get(), 0, *txn, 
  //       context.coordinator_id);
  //   // messages_mutex[coordinator_id_dst]->unlock();

  //   router_send_txn_cnt[coordinator_id_dst]++;
  //   n_network_size.fetch_add(router_size);
  //   router_transactions_send.fetch_add(1);
  // };

  void metis_migration_router_request(std::vector<int>& router_send_txn_cnt, size_t coordinator_id_dst, simpleTransaction* txn) {
    // router transaction to coordinators
    // messages_mutex[coordinator_id_dst]->lock();
    size_t router_size = LionMessageFactory::metis_migration_transaction_message(
        *metis_async_messages[coordinator_id_dst].get(), 0, *txn, 
        context.coordinator_id);
    flush_message(metis_async_messages, coordinator_id_dst);
    // messages_mutex[coordinator_id_dst]->unlock();

    n_network_size.fetch_add(router_size);
  };

  struct cmp{
    bool operator()(const std::shared_ptr<simpleTransaction>& a, 
                    const std::shared_ptr<simpleTransaction>& b){
      return a->execution_cost < b->execution_cost;
    }
  };

  long long cal_load_distribute(int aver_val, 
                          std::unordered_map<size_t, int>& busy_){
    int cur_val = 0;
    for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      // 
      cur_val += (aver_val - busy_[i]) * (aver_val - busy_[i]);
    }
    cur_val /= context.coordinator_num;

    return cur_val;
  }
  

  int find_idle_node(const std::unordered_map<size_t, int>& idle_node,
                     std::vector<int>& q_costs,
                     bool is_init){
    int idle_coord_id = -1;                        
    int min_cost = INT_MAX;
    // int idle_coord_id = -1;
    for(auto& idle: idle_node){
      // 
      if(is_init){
        if(min_cost > q_costs[idle.first]){
          min_cost = q_costs[idle.first];
          idle_coord_id = idle.first;
        }
      } else {
        if(q_costs[idle.first] <= -150){
          idle_coord_id = idle.first;
        }
      }
    }
    return idle_coord_id;
  }
  void scheduler_transactions(std::vector<int>& router_send_txn_cnt, int thread_id){    
    size_t batch_size = std::min((size_t)transactions_queue.size(),  // [thread_id]
                            (size_t)context.batch_size);
    // batch_size = 0; // debug
    LOG(INFO) << "batch_size: " << std::to_string(batch_size);

    std::vector<std::shared_ptr<simpleTransaction>> &txns = node_txns[thread_id];
    txns.clear();

    std::unordered_map<size_t, int>& busy_ = node_busy_[thread_id];
    for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      busy_[i] = 0;
    }
    
    auto staart = std::chrono::steady_clock::now();

    size_t cur_thread_transaction_num = batch_size / context.coordinator_num;
    int aver_val = cur_thread_transaction_num / context.coordinator_num;


    double cur_timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
                 std::chrono::steady_clock::now() - start_time)
                 .count() * 1.0 / 1000 / 1000;
    int workload_type = ((int)cur_timestamp / context.workload_time) + 1;// which_workload_(crossPartition, (int)cur_timestamp);
    // find minimal cost routing 
    for(size_t i = 0; i < cur_thread_transaction_num; i ++ ){

      bool success = false;
      std::shared_ptr<simpleTransaction> txn(transactions_queue.pop_no_wait(success)); // [thread_id]
      txn->idx_ = i;

      DCHECK(success == true);

      if(context.lion_with_trace_log){
        // 
        out_.lock();
        auto current_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - trace_log)
                  .count() * 1.0 / 1000;
        outfile_excel << current_timestamp << "\t";
        for(auto item: txn->keys){
          outfile_excel << item << "\t";
        }
        outfile_excel << "\n";
        out_.unlock();
      }

      // router_transaction_to_coordinator(txn, coordinator_id_dst); // c_txn send to coordinator
      if(txn->is_distributed){ 
        // static-distribute
        // std::unordered_map<int, int> result;
        txn_nodes_involved(txn.get(), true, txns_coord_cost[thread_id]);
      } else {
        DCHECK(txn->is_distributed == 0);
        txn->destination_coordinator = txn->partition_id % context.coordinator_num;
      } 

      txns.push_back(txn);

      busy_[txn->destination_coordinator] ++;


      // if(((int)cur_timestamp / context.workload_time) == 0){
      //   test_distributed_outfile_excel[thread_id] << "\t" << txn->idx_ << "\t" << txn->keys[0] << "\t" << txn->keys[1] << "\t" << txn->destination_coordinator << "\t" << txn->execution_cost << 
      //   "\t" << txns_coord_cost[thread_id][txn->idx_][0] << "\t" << txns_coord_cost[thread_id][txn->idx_][1] << "\t" << txns_coord_cost[thread_id][txn->idx_][2] << "\t" << txns_coord_cost[thread_id][txn->idx_][3] << "\n" ;
      // }
    }
    // test_distributed_outfile_excel[thread_id] << "workload" << ((int)cur_timestamp / context.workload_time) << "\n";

// ((int)cur_timestamp / context.workload_time) == 0


    double cur_timestamp__ = std::chrono::duration_cast<std::chrono::microseconds>(
                 std::chrono::steady_clock::now() - staart)
                 .count() * 1.0 / 1000 / 1000;

    LOG(INFO) << "scheduler_transactions : " << cur_timestamp__;
    // LOG(INFO) << "BEFORE";
    // for(int i = 0; i < txns.size(); i ++ ){
    //   LOG(INFO) << "txns[i]->destination_coordinator : " << i << " " << txns[i]->destination_coordinator;  
    // }

    long long threshold = 100 / (context.coordinator_num / 2) * 100 / (context.coordinator_num / 2); // 2200 - 2800

    long long cur_val = cal_load_distribute(aver_val, busy_);

    // if(((int)cur_timestamp / context.workload_time) == 0){
    //   test_distributed_outfile_excel[thread_id] << "load: " << "\t" << threshold << "\n";
    //   for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
    //     test_distributed_outfile_excel[thread_id] << busy_[i] << "\t";
    //   }
    //   test_distributed_outfile_excel[thread_id] <<"\n";
    // }


    // && context.lion_with_metis_init == 0 && 
    
    if(cur_val > threshold && context.random_router == 0){ 
      std::priority_queue<std::shared_ptr<simpleTransaction>, 
                        std::vector<std::shared_ptr<simpleTransaction>>, 
                        cmp> q_;
      for(size_t i = 0 ; i < txns.size(); i ++ ){
        q_.push(txns[i]);
      }
      // start tradeoff for balancing
      int batch_num = 50; // 

      bool is_ok = true;
      do {
        
        for(int i = 0 ; i < batch_num && q_.size() > 0; i ++ ){

          std::unordered_map<size_t, int> overload_node;
          std::unordered_map<size_t, int> idle_node;

          for(size_t j = 0 ; j < context.coordinator_num; j ++ ){
            if((long long) (busy_[j] - aver_val) * (busy_[j] - aver_val) > threshold){
              if((busy_[j] - aver_val) > 0){
                overload_node[j] = busy_[j] - aver_val;
              } else {
                idle_node[j] = aver_val - busy_[j];
              } 
            }
          }
          if(overload_node.size() == 0 || idle_node.size() == 0){
            is_ok = false;
            break;
          }
          std::shared_ptr<simpleTransaction> t = q_.top();
          q_.pop();

          if(overload_node.count(t->destination_coordinator)){
            // find minial cost in idle_node
            int idle_coord_id = find_idle_node(idle_node, 
                                               txns_coord_cost[thread_id][t->idx_], 
                                               (workload_type == 1 || context.lion_with_metis_init == 0));
            if(idle_coord_id == -1){
              continue;
            }

            // minus busy
            busy_[t->destination_coordinator] -= 1;
            if(--overload_node[t->destination_coordinator] <= 0){
              overload_node.erase(t->destination_coordinator);
            }
            // add dest busy
            t->destination_coordinator = idle_coord_id;
            busy_[t->destination_coordinator] += 1;
            idle_node[idle_coord_id] -= 1;
            if(idle_node[idle_coord_id] <= 0){
              idle_node.erase(idle_coord_id);
            }

            // if(((int)cur_timestamp / context.workload_time) == 0){
            //   test_distributed_outfile_excel[thread_id] << "change :" <<  "\t" << t->idx_ << "\t"<< t->keys[0] << "\t" << t->keys[1] << "\t" << t->destination_coordinator << "\t" << t->execution_cost << 
            //       "\t" << txns_coord_cost[thread_id][t->idx_][0] << "\t" << txns_coord_cost[thread_id][t->idx_][1] << "\t" << txns_coord_cost[thread_id][t->idx_][2] << "\t" << txns_coord_cost[thread_id][t->idx_][3] << "\n" ;
            // }

          }

          if(overload_node.size() == 0 || idle_node.size() == 0){
            break;
          }

        }
        if(q_.empty()){
          is_ok = false;
        }
        
      } while(cal_load_distribute(aver_val, busy_) > threshold && is_ok);
    }




    // LOG(INFO) << "NEW";
    for(size_t i = 0; i < txns.size(); i ++ ){
      // LOG(INFO) << "txns[i]->destination_coordinator : " << i << " " << txns[i]->destination_coordinator << " " << txns[i]->keys[0] << " " << txns[i]->keys[1];
      coordinator_send[txns[i]->destination_coordinator] ++ ;
      router_request(router_send_txn_cnt, txns[i]);   
    }

    cur_timestamp__ = std::chrono::duration_cast<std::chrono::microseconds>(
                 std::chrono::steady_clock::now() - staart)
                 .count() * 1.0 / 1000 / 1000;
    LOG(INFO) << "scheduler_transactions + send : " << cur_timestamp__;
  }
  
  
  
  void start() override {

    LOG(INFO) << "LionGenerator " << id << " starts.";
    start_time = std::chrono::steady_clock::now();
    workload.start_time = start_time;
    
    StorageType storage;
    uint64_t last_seed = 0;

    // transaction only commit in a single group

    std::queue<std::unique_ptr<TransactionType>> q;
    std::size_t count = 0;


    // 
    trace_log = std::chrono::steady_clock::now();
    if(context.lion_with_trace_log){
      outfile_excel.open(context.data_src_path_dir + "resultss.xls", std::ios::trunc); // ios::trunc
      outfile_excel << "timestamp" << "\t" << "txn-items" << "\n";
    }

    // distributed_outfile_excel.open(context.data_src_path_dir + "resultss_generator.xls", std::ios::trunc); // ios::trunc
    // for(int i = 0 ; i < 4; i ++ ){
    //   test_distributed_outfile_excel[i].open(context.data_src_path_dir + "resultss_generator.xls." + std::to_string(i), std::ios::trunc); // ios::trunc
    // }
    


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
        ControlMessageFactory::pin_thread_to_core(context, generators[n], generator_core_id[n]);
      }
    }

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
          // for (auto &t : transmiter) {
          //   t.join();
          // }
          if(context.lion_with_trace_log){
            outfile_excel.close();
          }
          // distributed_outfile_excel.close();
          // for(int i = 0 ; i < 4; i ++ ){
          //   test_distributed_outfile_excel[i].close();
          // }

          return;
        }
      } while (status != ExecutorStatus::C_PHASE);

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
      VLOG(DEBUG_V) << "Generator " << id << " ready to process_request";

      // thread to router the transaction generated by LionGenerator
      
      for(int i = 0 ; i < MAX_COORDINATOR_NUM; i ++ ){
        coordinator_send[i] = 0;
      }
      std::vector<std::thread> threads;
      for (auto n = 0u; n < context.coordinator_num; n++) {  // context.coordinator_num
        threads.emplace_back([&](int n) {
          int thread_id = n;

          std::vector<int> router_send_txn_cnt(context.coordinator_num, 0);
          scheduler_transactions(router_send_txn_cnt, thread_id);

          is_full_signal.store(0);
          // 
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
          
        }, n);

        if (context.cpu_affinity) {
          ControlMessageFactory::pin_thread_to_core(context, threads[n], sender_core_id[n]);// , generator_core_id[n
        }
      }


      LOG(INFO) << "wait for thread join: " << std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - test)
                           .count() * 1.0 / 1000 / 1000;

      for (auto &t : threads) {
        t.join();
      }

      for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
        LOG(INFO) << "Coord[" << i << "]: " << coordinator_send[i];
      }

      LOG(INFO) << "router_transaction_to_coordinator: " << std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - test)
                           .count() * 1.0 / 1000 / 1000;
      // test = std::chrono::steady_clock::now();

      n_complete_workers.fetch_add(1);
      VLOG(DEBUG_V) << "Generator " << id << " finish C_PHASE";

      while (static_cast<ExecutorStatus>(worker_status.load()) !=
        ExecutorStatus::S_PHASE) {
        process_request();
      }
      process_request();

      LOG(INFO) << "after s_phase " << std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - test)
                           .count() * 1.0 / 1000 / 1000;
      // s-phase
      n_started_workers.fetch_add(1);
      n_complete_workers.fetch_add(1);

      VLOG(DEBUG_V) << "Generator " << id << " finish S_PHASE";

      router_fence(); // wait for coordinator to response

      LOG(INFO) << "Generator Fence: wait for coordinator to response: " << std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - test)
                           .count() * 1.0 / 1000 / 1000;
      // test = std::chrono::steady_clock::now();

      // flush_async_messages();
      // for(size_t thread_id = 0 ; thread_id < context.coordinator_num; thread_id ++ ){
      //   flush_messages(async_messages[thread_id]);
      // }
      flush_async_messages();


      
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::S_PHASE) {
        process_request();
      }

      VLOG_IF(DEBUG_V, id==0) << "wait back "
              << std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::steady_clock::now() - test)
                     .count() * 1.0 / 1000 / 1000
              << " microseconds.";
      // test = std::chrono::steady_clock::now();


      // n_complete_workers has been cleared
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
          // transaction router from LionGenerator
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
                                // &router_transactions_queue,
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

  // void flush_async_messages() { flush_messages(async_messages); }
  void flush_async_messages() { flush_messages(async_messages); }



  void init_message(Message *message, std::size_t dest_node_id) {
    message->set_source_node_id(coordinator_id);
    message->set_dest_node_id(dest_node_id);
    message->set_worker_id(id);
  }

protected:
  std::unique_ptr<Clay<WorkloadType>> my_clay;
  
  ShareQueue<simpleTransaction*, 54096> transactions_queue;// [20];// [20];

  size_t generator_num;
  std::atomic<uint32_t> is_full_signal;// [20];
  

  std::vector<int> generator_core_id;
  std::vector<int> sender_core_id;
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
  std::vector<std::unique_ptr<Message>> sync_messages, metis_async_messages;
  std::vector<std::unique_ptr<Message>> async_messages;// [20];

  std::vector<std::unique_ptr<std::mutex>> messages_mutex;

  std::deque<simpleTransaction> router_transactions_queue;
  ShareQueue<simpleTransaction> migration_transactions_queue;

  std::deque<int> router_stop_queue;

  std::vector<std::function<void(MessagePiece, Message &, std::vector<std::unique_ptr<Message>>&, 
                                 DatabaseType &, const ContextType &, Partitioner *, // add partitioner
                                 TransactionType *, 
                                //  ShareQueue<simpleTransaction>*,
                                 ShareQueue<simpleTransaction>*)>>
      messageHandlers;

  std::vector<
    std::function<void(MessagePiece, Message &, DatabaseType &, std::deque<simpleTransaction>* ,std::deque<int>* )>>
    controlMessageHandlers;    

  std::vector<std::size_t> message_stats, message_sizes;
  LockfreeQueue<Message *, 50086> in_queue, out_queue;

  std::ofstream outfile_excel;

  // std::ofstream distributed_outfile_excel;

  // std::ofstream test_distributed_outfile_excel[4]; // debug
  std::mutex out_;

  std::chrono::steady_clock::time_point trace_log; 
  std::atomic<int> coordinator_send[MAX_COORDINATOR_NUM];
  // 

  std::unordered_map<size_t, int> node_busy_[MAX_COORDINATOR_NUM];
  std::vector<std::shared_ptr<simpleTransaction>> node_txns[MAX_COORDINATOR_NUM];

  std::vector<std::vector<int>> txns_coord_cost[MAX_COORDINATOR_NUM];
};
} // namespace group_commit

} // namespace star