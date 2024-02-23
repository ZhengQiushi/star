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
#include "protocol/LionSS/LionSSMeta.h"

namespace star {
namespace group_commit {
#define LAG_NUM 1000
template <class Workload, class Protocol> class LionSSGenerator : public Worker {
public:
  using WorkloadType = Workload;
  using ProtocolType = Protocol;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using TransactionType = LionSSTransaction;
  
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using MessageType = typename ProtocolType::MessageType;
  using MessageFactoryType = LionSSMessageFactory;
  using MessageHandlerType = LionSSMessageHandler<DatabaseType>;

  using StorageType = typename WorkloadType::StorageType;
  int pin_thread_id_ = 3;
 
   
  struct Clump
  {
    std::chrono::_V2::steady_clock::time_point ts;
    std::unordered_set<uint64_t> keys;
    int coordinator_id;
  };

  class RouterClump {
  public:  
    std::chrono::_V2::steady_clock::time_point ts;
    const ContextType &context;
    int partition_num;
    RouterClump(const ContextType &context): context(context){
      ts = std::chrono::steady_clock::now();
      keyToClump.resize(context.partition_num, std::vector<int>(200000, -1));
    }

    std::vector<std::vector<int>> keyToClump;
    std::vector<Clump> clumps;
    std::queue<int> freeClumps;
    const int expire_duration = 30;
    void addToClump(simpleTransaction* t, Clump& c){
      for(auto& key: t->keys){
        c.keys.insert(key);
      }
      c.ts = std::chrono::steady_clock::now();
    }

    void MergeClump(Clump& c1, Clump& c2){
      for(auto& key: c2.keys){
        c1.keys.insert(key);
      }
      c1.ts = std::chrono::steady_clock::now();
    }
    void updateDest(simpleTransaction* t){
      for(auto& key: t->keys){
        int k = key % 200000;
        int p_id = key / 200000;
        int clump_id = keyToClump[p_id][k];
        Clump& c = clumps[clump_id];
        c.coordinator_id = t->destination_coordinator;
        break;
      }
    }
    void addTxn(simpleTransaction* t){
      // merges two clumps
      bool has_add = false;

      int clumps_ids[20] = {0};
      int clumps_cnt = 0;

      for(int kk = t->keys.size() - 1; kk >= 0 ; kk -- ){
        int key = t->keys[kk];
        int k = key % 200000;
        int p_id = key / 200000;
        if(keyToClump[p_id][k] == -1){
          continue;
        }

        int clump_id = keyToClump[p_id][k];
        Clump& c = clumps[clump_id];
        auto now = std::chrono::steady_clock::now();

        if(std::chrono::duration_cast<std::chrono::seconds>(now - c.ts).count() > expire_duration){
          // expired
          freeClumps.push(clump_id);
          // clean expired clump
          for(auto& key: c.keys){
            int k = key % 200000;
            keyToClump[p_id][k] = -1;
          }
          c.keys.clear();
        } else {
          bool is_find = false;
          for(int i = 0; i < clumps_cnt; i ++ ){
            if(clumps_ids[i] == clump_id){
              is_find = true;
            }
          }
          if(!is_find || clumps_cnt == 0){
            clumps_ids[clumps_cnt ++ ] = clump_id;
          }
          has_add = true;
        }
        if(t->keys[1] == 429321){
          LOG(INFO) << "!!!! " << 429321 << " " << key << " " << clump_id << " " << c.coordinator_id;
        }
        if(t->keys[0] == 932){
          LOG(INFO) << "!!!! " << 932 << " " << key << " " << clump_id << " " << c.coordinator_id;
        }
      }

      if(has_add){
        Clump& c = clumps[clumps_ids[0]];
        if(clumps_cnt == 1){
          addToClump(t, c);
        } else if(clumps_cnt > 1){
          for(int i = 1 ; i < clumps_cnt; i ++ ){
            Clump& c2 = clumps[clumps_ids[i]];
            MergeClump(c, c2);
            freeClumps.push(clumps_ids[i]);
            for(auto& key: c2.keys){
              int k = key % 200000;
              int p_id = key / 200000;
              keyToClump[p_id][k] = clumps_ids[0];
            }
            c2.keys.clear();
          }
        }
      } else {
        // has_not_add 
        // first find a free clumps
        int new_clump_id = -1;
        if(!freeClumps.empty()){
          new_clump_id = freeClumps.front();
          freeClumps.pop();
        } else {
          // else alloc a new one 
          new_clump_id = clumps.size();
          Clump c;
          clumps.push_back(c);
        }

        Clump& c = clumps[new_clump_id];
        for(auto& key: t->keys){
          c.keys.insert(key);
          int k = key % 200000;
          int p_id = key / 200000;
          keyToClump[p_id][k] = new_clump_id;
        }
        c.ts = std::chrono::steady_clock::now();
        // DCHECK(t->destination_coordinator != -1);
        c.coordinator_id = -1; // t->destination_coordinator;
      }
    }

    int searchTxn(simpleTransaction* t){
      int ret = -1;
      auto now = std::chrono::steady_clock::now();
      if(std::chrono::duration_cast<std::chrono::seconds>(now - ts).count() > 10){
        for(auto& key: t->keys){
          int k = key % 200000;
          int p_id = key / 200000;
          if(keyToClump[p_id][k] == -1){
            continue;
          }
          int clump_id = keyToClump[p_id][k];
          Clump& c = clumps[clump_id];
          auto now = std::chrono::steady_clock::now();

          if(std::chrono::duration_cast<std::chrono::seconds>(now - c.ts).count() > expire_duration){
            continue;
          }
          ret = c.coordinator_id;
          break;
        }
      }
      return ret;
    }
  };

  LionSSGenerator(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
           const ContextType &context, std::atomic<uint32_t> &worker_status,
           std::atomic<uint32_t> &n_complete_workers,
           std::atomic<uint32_t> &n_started_workers,
           lionss::ScheduleMeta &schedule_meta
           )
      : Worker(coordinator_id, id), db(db), context(context),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        schedule_meta(schedule_meta),
        routerClumps(context),
        partitioner(std::make_unique<LionDynamicPartitioner<Workload> >(
            coordinator_id, context.coordinator_num, db)),
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

      // messages_mutex.emplace_back(std::make_unique<std::mutex>());
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
    controlMessageHandlers = ControlMessageHandler<DatabaseType>::get_message_handlers();

    message_stats.resize(messageHandlers.size(), 0);
    message_sizes.resize(messageHandlers.size(), 0);

    txns_coord_cost.resize(context.batch_size, std::vector<int>(context.coordinator_num, 0));

    for(int i = 0 ; i < 20 ; i ++ ){
      is_full_signal_self[i].store(0);
    }

    generator_num = 1;

    dispatcher_core_id.resize(context.coordinator_num);

    pin_thread_id_ = 3 + 2 * 2 + context.worker_num;

    for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      dispatcher_core_id[i] = pin_thread_id_ + id * context.coordinator_num + i;
    }
    // for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
    //   // dispatcher_core_id[i] = pin_thread_id_ + id * context.coordinator_num + i;
    //   dispatcher_core_id[i] = pin_thread_id_ + id + context.worker_num * i;
    // }
    dispatcher_num = context.worker_num * context.coordinator_num;
    cur_txn_num = context.batch_size / dispatcher_num ; // * context.coordinator_num
    
    is_inited.store(0);
    start_inited.store(0);

    for (auto n = 0u; n < context.coordinator_num; n++) {
      // 处理
      dispatcher.emplace_back([&](int n, int worker_id) {
        ExecutorStatus status = static_cast<ExecutorStatus>(worker_status.load());
        int dispatcher_id  = worker_id * context.coordinator_num + n;
        // 
        do {
          std::this_thread::sleep_for(std::chrono::microseconds(5));
        } while (start_inited.load() == 0);  
        
        while(is_full_signal_self[dispatcher_id].load() == false){
            bool success = prepare_transactions_to_run(workload, storages[dispatcher_id],
                                  transactions_queue_self[dispatcher_id], 
                                  dispatcher_id);

            if(!success){ // full
                is_full_signal_self[dispatcher_id].store(true);
            }
        }
        // 
        is_inited.fetch_add(1);

        do {
          status = static_cast<ExecutorStatus>(worker_status.load());
          std::this_thread::sleep_for(std::chrono::microseconds(5));
        } while (status != ExecutorStatus::START);  
        
        while(status != ExecutorStatus::EXIT && 
              status != ExecutorStatus::CLEANUP){
          status = static_cast<ExecutorStatus>(worker_status.load());
          if(is_full_signal_self[dispatcher_id].load() == true){
            std::this_thread::sleep_for(std::chrono::microseconds(5));
            continue;
          }

          bool success = prepare_transactions_to_run(workload, storages[dispatcher_id],
            transactions_queue_self[dispatcher_id], dispatcher_id);
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

  void router_request(std::vector<int>& router_send_txn_cnt, simpleTransaction* txn) {
    // router transaction to coordinators
    size_t coordinator_id_dst = txn->destination_coordinator;

    // messages_mutex[coordinator_id_dst]->lock();
    size_t router_size = ControlMessageFactory::new_router_transaction_message(
        *async_messages[coordinator_id_dst].get(), 0, *txn, 
        RouterTxnOps::TRANSFER);
    flush_message(async_messages, coordinator_id_dst);
    // messages_mutex[coordinator_id_dst]->unlock();

    // router_send_txn_cnt[coordinator_id_dst]++;
    n_network_size.fetch_add(router_size);
    // router_transactions_send.fetch_add(1);
  };

  bool prepare_transactions_to_run(WorkloadType& workload, StorageType& storage,
      ShareQueue<simpleTransaction*, LAG_NUM>& transactions_queue_self_,
      int dispatcher_id){
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
          partition_id = (0 + skew_factor * context.coordinator_num) % context.partition_num;
      } else {
        // 0 < 50
        //正常
      }
      // 
      std::size_t partition_id_;
      if(context.skew_factor >= skew_factor) {
        partition_id_ = partition_id / hot_area_size * hot_area_size;

      } else {
        partition_id_ = partition_id / hot_area_size * hot_area_size + 
                                partition_id / hot_area_size % context.coordinator_num;;
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
    
    // LOG(INFO) << dispatcher_id << " " 
    //           << txn->partition_id << " " 
    //           << partition_id_ << " " 
    //           << partition_id << " " 
    //           << txn->keys[0] / 200000; //  << " " << txn->keys[1] / 200000 % contex;

    return transactions_queue_self_.push_no_wait(txn); // txn->partition_id % context.coordinator_num [0]
  }

  void txn_nodes_involved(simpleTransaction* t) {
      
      // auto & txns_coord_cost_ = schedule_meta.txns_coord_cost;

      int from_nodes_id[MAX_COORDINATOR_NUM] = {0};           // dynamic replica nums
      int from_nodes_id_secondary[MAX_COORDINATOR_NUM] = {0}; // secondary replica nums
      // std::unordered_map<int, int> nodes_cost;              // cost on each node
      int max_cnt = INT_MIN;
      int max_real_cnt = INT_MIN;
      int max_node = -1;

      size_t ycsbTableID = ycsb::ycsb::tableID;
      auto query_keys = t->keys;


      for (size_t j = 0 ; j < query_keys.size(); j ++ ){
        // LOG(INFO) << "query_keys[j] : " << query_keys[j];
        // judge if is cross txn
        size_t cur_c_id = -1;
        size_t secondary_c_ids;

        // look-up the dynamic router to find-out where
        auto router_table = db.find_router_table(ycsbTableID);// ,master_coordinator_id);
        auto tab = static_cast<RouterValue*>(router_table->search_value((void*) &query_keys[j]));

        cur_c_id = tab->get_dynamic_coordinator_id();
        secondary_c_ids = tab->get_secondary_coordinator_id();
        from_nodes_id[cur_c_id] += 1;

        // key on which node
        for(size_t i = 0; i <= context.coordinator_num; i ++ ){
            if((secondary_c_ids & 1) && i != cur_c_id){
                from_nodes_id_secondary[i] += 1;
            }
            secondary_c_ids = secondary_c_ids >> 1;
        }

        size_t cnt_master = from_nodes_id[cur_c_id];
        size_t cnt_secondary = from_nodes_id_secondary[cur_c_id];
        int cur_score = 0;
        int other_cur_score = 0;

        if(context.migration_only){
          cur_score += 100 * cnt_master;
        } else {
          if(cnt_master == query_keys.size()){
            cur_score += 100 * (int)query_keys.size();
          } else if(cnt_secondary + cnt_master == query_keys.size()){
            cur_score += 50 * cnt_master + 25 * cnt_secondary;
          } else {
            cur_score += 25 * cnt_master + 15 * cnt_secondary;
          }
        }
        if(cur_score + other_cur_score > max_cnt){
          max_node = cur_c_id;
          max_cnt = cur_score + other_cur_score;
          max_real_cnt = cur_score;
        }
      }

      if(context.random_router > 0){
        // 
        // size_t random_value = random.uniform_dist(0, 9);
        max_node = (query_keys[0] / context.keysPerPartition + 1) % context.coordinator_num;
      } 


      t->destination_coordinator = max_node;
      t->execution_cost = 10 * (int)query_keys.size() - max_cnt;
      
      if(max_real_cnt == 100 * (int)query_keys.size()){
        t->is_real_distributed = false;
      } else {
        t->is_real_distributed = true;
      }

      // t->replica_heavy_node = replica_max_node;
      // if(t->is_real_distributed){
      //   std::string debug = "";
      //   for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      //     debug += std::to_string(txns_coord_cost_[t->idx_][i]) + " ";
      //   }
      //   LOG(INFO) << t->keys[0] << " " << t->keys[1] << " " << debug;
      // }
      // size_t cnt_master = from_nodes_id[max_node];
      // size_t cnt_secondary = from_nodes_id_secondary[max_node];

      // if(cnt_secondary + cnt_master != query_keys.size()){
      //   distributed_outfile_excel << t->keys[0] << "\t" << t->keys[1] << "\t" << max_node << "\n";
      // }

    // LOG(INFO) << t->keys[0] << " " << t->keys[1] << " : " << t->destination_coordinator;
    // routerClumps.updateDest(t);


     return;
   }


  void txn_nodes_involved_tpcc(simpleTransaction* t) {

      // auto & txns_coord_cost_ = schedule_meta.txns_coord_cost;

      std::vector<int> busy_local(context.coordinator_num, 0);
      std::vector<int> replicate_busy_local(context.coordinator_num, 0);

      int from_nodes_id[MAX_COORDINATOR_NUM] = {0};              // dynamic replica nums
      int from_nodes_id_secondary[MAX_COORDINATOR_NUM] = {0};; // secondary replica nums
      // std::unordered_map<int, int> nodes_cost;              // cost on each node
      int weight_w = 10;
      int weight_d = 5;
      int weight_c = 1;
      int weight_s = 1;

      size_t weight_sum = 0;

      std::vector<int> query_keys;
      star::tpcc::NewOrderQuery keys;
      keys.unpack_transaction(*t);
      // warehouse_key
      auto warehouse_key = tpcc::warehouse::key(keys.W_ID);
        size_t warehouse_coordinator_id = static_cast<RouterValue*>(db.find_router_table(tpcc::warehouse::tableID)->search_value((void*)&warehouse_key))->get_dynamic_coordinator_id();
      from_nodes_id[warehouse_coordinator_id] += weight_w;
      weight_sum += weight_w;
      query_keys.push_back(warehouse_coordinator_id);
      // district_key
      auto district_key = tpcc::district::key(keys.W_ID, keys.D_ID);
        size_t district_coordinator_id = static_cast<RouterValue*>(db.find_router_table(tpcc::district::tableID)->search_value((void*)&district_key))->get_dynamic_coordinator_id();
      from_nodes_id[district_coordinator_id] += weight_d;
      weight_sum += weight_d;
      query_keys.push_back(district_coordinator_id);
      // customer_key
      auto customer_key = tpcc::customer::key(keys.W_ID, keys.D_ID, keys.C_ID);
        size_t customer_coordinator_id = static_cast<RouterValue*>(db.find_router_table(tpcc::customer::tableID)->search_value((void*)&customer_key))->get_dynamic_coordinator_id();
      from_nodes_id[customer_coordinator_id] += weight_c;
      weight_sum += weight_c;
      query_keys.push_back(customer_coordinator_id);
        
      for(size_t i = 0 ; i < t->keys.size() - 3; i ++ ){
        auto router_table = db.find_router_table(tpcc::stock::tableID);

        auto stock_key = tpcc::stock::key(keys.INFO[i].OL_SUPPLY_W_ID, keys.INFO[i].OL_I_ID);
        auto tab = static_cast<RouterValue*>(router_table->search_value((void*)&stock_key));
        size_t stock_coordinator_id = tab->get_dynamic_coordinator_id();

        
        from_nodes_id[stock_coordinator_id] += weight_s;
        weight_sum += weight_s;
        query_keys.push_back(stock_coordinator_id);
      }

      int max_cnt = INT_MIN;
      int max_node = -1;

      int replica_most_cnt = INT_MIN;
      int replica_max_node = -1;




      for(size_t cur_c_id = 0 ; cur_c_id < context.coordinator_num; cur_c_id ++ ){
        int cur_score = 0; // 5 - 5 * (busy_local[cur_c_id] * 1.0 / cur_txn_num); // 1 ~ 10

        size_t cnt_master = from_nodes_id[cur_c_id];
        size_t cnt_secondary = from_nodes_id_secondary[cur_c_id];
        if(context.migration_only){
          cur_score += 100 * cnt_master;
        } else {
          if(cnt_master == weight_sum){
            cur_score += 100 * weight_sum;
          } else if(cnt_secondary + cnt_master == weight_sum){
            cur_score += 50 * cnt_master + 25 * cnt_secondary;
          } else {
            cur_score += 25 * cnt_master + 15 * cnt_secondary;
          }
        }

        if(cur_score > max_cnt){
          max_node = cur_c_id;
          max_cnt = cur_score;
        }

        if(cnt_secondary > replica_most_cnt){
          replica_max_node = cur_c_id;
          replica_most_cnt = cnt_secondary;
        }

        // txns_coord_cost_[t->idx_][cur_c_id] = 10 * (int)weight_sum - cur_score;
        replicate_busy_local[cur_c_id] += cnt_secondary;
      }


      // if(context.random_router > 0){
      //   // 
      //   // size_t random_value = random.uniform_dist(0, 9);
      //   size_t coordinator_id = (keys.W_ID - 1) % context.coordinator_num;
      //   max_node = coordinator_id; // query_keys[0];
        
      // } 


      t->destination_coordinator = max_node;
      t->execution_cost = 10 * weight_sum - max_cnt;
      t->is_real_distributed = (max_cnt == 100 * weight_sum) ? false : true;

      t->replica_heavy_node = replica_max_node;
      // if(t->is_real_distributed){
      //   std::string debug = "";
      //   for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      //     debug += std::to_string(txns_coord_cost_[t->idx_][i]) + " ";
      //   }
      //   LOG(INFO) << t->keys[0] << " " << t->keys[1] << " " << debug;
      // }
      size_t cnt_master = from_nodes_id[max_node];
      size_t cnt_secondary = from_nodes_id_secondary[max_node];

      // if(cnt_secondary + cnt_master != query_keys.size()){
      //   distributed_outfile_excel << t->keys[0] << "\t" << t->keys[1] << "\t" << max_node << "\n";
      // }

     return;
   }


  void start() override {

    LOG(INFO) << "LionSSGenerator " << id << " starts.";

    
    start_time = std::chrono::steady_clock::now();
    workload.start_time = start_time;
    start_inited.fetch_add(1);
    
    StorageType storage;
    uint64_t last_seed = 0;

    // transaction only commit in a single group

    std::queue<std::unique_ptr<TransactionType>> q;
    std::size_t count = 0;

    int cur_workload_type = 0;

    do {
      int a = is_inited.load();
      std::this_thread::sleep_for(std::chrono::microseconds(5));
    } while (is_inited.load() < context.coordinator_num);  

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
      long long sum_search = 0;

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

          // get one transaction from queue
          bool success = false;
          simpleTransaction* new_txn(transactions_queue_self[dispatcher_id].pop_no_wait(success)); 

          if(!success) continue;
          auto tt = std::chrono::steady_clock::now();
          
          // determine its ideal destination
          if(WorkloadType::which_workload == myTestSet::YCSB){
            txn_nodes_involved(new_txn);
          } else {
            txn_nodes_involved_tpcc(new_txn);
          }

          auto tt_now = std::chrono::steady_clock::now();
          sum_search += std::chrono::duration_cast<std::chrono::nanoseconds>(tt_now - tt).count();

          // router the transaction
          router_request(router_send_txn_cnt, new_txn);   
          send_num[new_txn->destination_coordinator] += 1;

          // add to metis generator for schedule
          bool ss = schedule_meta.transactions_queue_self.push_no_wait(new_txn);
          // if(ss){
          //   LOG(INFO) << dispatcher_id << "for c[" << i << "] " << cur <<
          //                " " << new_txn->keys[0] << 
          //                " " << new_txn->keys[1] << 
          //                " " << new_txn->destination_coordinator << 
          //                " ";
          // }
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
          LOG(INFO) << sum_search * 1.0 / 1000;
          sum_search = 0;
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

  // std::size_t get_partition_id() {

  //   std::size_t partition_id;

  //   if (context.partitioner == "pb") {
  //     partition_id = random.uniform_dist(0, context.partition_num - 1);
  //   } else {
  //     auto partition_num_per_node =
  //         context.partition_num / context.coordinator_num;
  //     partition_id = random.uniform_dist(0, partition_num_per_node - 1) *
  //                        context.coordinator_num +
  //                    coordinator_id;
  //   }
  //   CHECK(partitioner->has_master_partition(partition_id));
  //   return partition_id;
  // }


  void push_message(Message *message) override { 

    for (auto it = message->begin(); it != message->end(); it++) {
      auto messagePiece = *it;
      auto message_type = messagePiece.get_message_type();

      if (message_type == static_cast<int>(ControlMessage::ROUTER_TRANSACTION_RESPONSE)){
        size_t from = message->get_source_node_id();
        schedule_meta.router_transaction_done[from].fetch_sub(1);
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
      // std::lock_guard<std::mutex> l(*messages_mutex[i].get()); // ]->lock();
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
  std::atomic<uint32_t> transmit_request_response;
  size_t generator_num;
  

  std::vector<int> dispatcher_core_id;

  std::vector<std::thread> dispatcher;

  std::vector<int> router_send_txn_cnt;
  std::mutex mm;

  DatabaseType &db;
  ContextType context;
  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  lionss::ScheduleMeta &schedule_meta;

  ShareQueue<simpleTransaction*, LAG_NUM> transactions_queue_self[MAX_COORDINATOR_NUM];
  StorageType storages[MAX_COORDINATOR_NUM];
  std::atomic<uint32_t> is_full_signal_self[MAX_COORDINATOR_NUM];

  std::atomic<uint32_t> is_inited;
  std::atomic<uint32_t> start_inited;

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
  // std::vector<std::unique_ptr<std::mutex>> messages_mutex;

  ShareQueue<simpleTransaction> router_transactions_queue;
  // std::deque<simpleTransaction> transmit_request_queue;

  std::deque<int> router_stop_queue;

  std::vector<std::unique_ptr<TransactionType>> no_use;

  std::vector<
      std::function<void(MessagePiece, Message &, DatabaseType &, const ContextType &,  Partitioner *, TransactionType *)>>
      messageHandlers;

  std::vector<
    std::function<void(MessagePiece, Message &, DatabaseType &, ShareQueue<simpleTransaction>*, std::deque<int>*)>>
    controlMessageHandlers;    

  std::vector<std::size_t> message_stats, message_sizes;
  LockfreeQueue<Message *, 500860> in_queue, out_queue;

  std::vector<std::vector<int>> txns_coord_cost;

  int dispatcher_num;
  int cur_txn_num;

  RouterClump routerClumps;

};
} // namespace group_commit

} // namespace star