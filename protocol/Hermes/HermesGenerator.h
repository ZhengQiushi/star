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
           std::atomic<uint32_t> &n_started_workers,
           hermes::ScheduleMeta &schedule_meta)
      : Worker(coordinator_id, id), db(db), context(context),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        schedule_meta(schedule_meta),
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

    replica_num = partitioner.replica_num();
    ycsbTableID = ycsb::ycsb::tableID;

    for(int i = 0 ; i < MAX_COORDINATOR_NUM ; i ++ ){
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
                  std::vector<std::vector<simpleTransaction>> &txns = schedule_meta.node_txns;

                  for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
                    coordinator_send[i].store(0);
                  }
                  
                  scheduler_transactions(dispatcher_num, dispatcher_id);

                  int idx_offset = dispatcher_id * cur_txn_num;

                  for(int j = 0; j < cur_txn_num; j ++ ){
                    int idx = idx_offset + j;

                    for(size_t r = 0; r < replica_num; r ++ ){
                      txns[r][idx].idx_ = r * context.batch_size + idx;
                      router_request(router_send_txn_cnt, &txns[r][idx]);   
                      coordinator_send[txns[r][idx].destination_coordinator] ++ ;
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
                    auto i = schedule_meta.done_schedule.load();
                    std::this_thread::sleep_for(std::chrono::microseconds(5));
                    status = static_cast<ExecutorStatus>(worker_status.load());
                  }

                  schedule_meta.done_schedule_out.fetch_add(1);

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

  struct cmp{
    bool operator()(const simpleTransaction* a, 
                    const simpleTransaction* b){
      return a->execution_cost < b->execution_cost;
    }
  };

  long long cal_load_distribute(int aver_val, 
                          std::unordered_map<size_t, int>& busy_){
    long long cur_val = 0;
    for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      // 
      cur_val += (aver_val - busy_[i]) * (aver_val - busy_[i]);
    }
    cur_val /= context.coordinator_num;

    return cur_val;
  }
  
  std::pair<int, int> find_idle_node(const std::unordered_map<size_t, int>& idle_node,
                     std::vector<int>& q_costs){
    int idle_coord_id = -1;                        
    int min_cost = INT_MAX;
    // int idle_coord_id = -1;
    for(auto& idle: idle_node){
      // 
      // if(is_init){
        if(min_cost > q_costs[idle.first]){
          min_cost = q_costs[idle.first];
          idle_coord_id = idle.first;
        }
      // } else {
      //   if(q_costs[idle.first] < 100){
      //     idle_coord_id = idle.first;
      //   }
      // }
    }
    if(idle_coord_id != -1 && q_costs[idle_coord_id] < 100){
      idle_coord_id = -1;
    }
    return std::make_pair(idle_coord_id, min_cost);
  }
  void balance_master(int r, 
                      long long aver_val, 
                      long long threshold){
      // start tradeoff for balancing
      auto & txns            = schedule_meta.node_txns;
      auto & busy_           = schedule_meta.node_busy;
      auto & txns_coord_cost = schedule_meta.txns_coord_cost;
    
      int batch_num = 50; // 
      // for(size_t r = 0 ; r < replica_num; r ++ ){
        auto& cur_busy = busy_[r];
        long long cur_val = cal_load_distribute(aver_val, cur_busy);
        for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
          LOG(INFO) <<" busy[" << i << "] = " << cur_busy[i];
        }

        bool is_ok = true;
        std::priority_queue<simpleTransaction*, 
                          std::vector<simpleTransaction*>, 
                          cmp> q_;
        size_t sz = cur_txn_num * dispatcher_num;
        for(size_t i = 0 ; i < sz; i ++ ){
          q_.push(&txns[r][i]);
        }
        do {
          for(int i = 0 ; i < batch_num && q_.size() > 0; i ++ ){

            std::unordered_map<size_t, int> overload_node;
            std::unordered_map<size_t, int> idle_node;

            find_imbalanced_node(overload_node, idle_node, 
                                cur_busy, aver_val, threshold);

            if(overload_node.size() == 0 || idle_node.size() == 0){
              is_ok = false;
              break;
            }
            simpleTransaction* t = q_.top();
            q_.pop();

            if(overload_node.count(t->destination_coordinator)){
              // find minial cost in idle_node
              auto [idle_coord_id, min_cost] = find_idle_node(idle_node, 
                                                txns_coord_cost[r][t->idx_]);
              if(idle_coord_id == -1){
                continue;
              }
              // minus busy
              cur_busy[t->destination_coordinator] -= 1;
              if(--overload_node[t->destination_coordinator] <= 0){
                overload_node.erase(t->destination_coordinator);
              }
              t->is_distributed = true;
              // add dest busy
              t->is_real_distributed = true;
              t->destination_coordinator = idle_coord_id;
              cur_busy[t->destination_coordinator] += 1;
              idle_node[idle_coord_id] -= 1;
              if(idle_node[idle_coord_id] <= 0){
                idle_node.erase(idle_coord_id);
              }
            }

            if(overload_node.size() == 0 || idle_node.size() == 0){
              break;
            }

          }
          if(q_.empty()){
            is_ok = false;
          }
          
        } while(cal_load_distribute(aver_val, cur_busy) > threshold && is_ok);

      // }
  }

  void find_imbalanced_node(std::unordered_map<size_t, int>& overload_node,
                            std::unordered_map<size_t, int>& idle_node,
                            std::unordered_map<size_t, int>& busy,
                            long long aver_val,
                            long long threshold
                            ){
    for(size_t j = 0 ; j < context.coordinator_num; j ++ ){
      // if((long long) (busy[j] - aver_val) * (busy[j] - aver_val) > threshold){
        if((busy[j] - aver_val) > 0){
          overload_node[j] = busy[j] - aver_val;
        } else {
          idle_node[j] = aver_val - busy[j];
        } 
      // }
    }    
  }

  void balance_master_tpcc(int r, long long aver_val, 
                      long long threshold){

      auto & txns              = schedule_meta.node_txns;
      auto & busy              = schedule_meta.node_busy;
      // auto & node_replica_busy = schedule_meta.node_replica_busy;
      auto & txns_coord_cost   = schedule_meta.txns_coord_cost;

      double cur_timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - start_time)
                  .count() * 1.0 / 1000 / 1000;
      int workload_type = ((int)cur_timestamp / context.workload_time) + 1;
      
      size_t sz = cur_txn_num * dispatcher_num;

      
        auto& cur_busy = busy[r];

        Clumps clumps(txns_coord_cost[r]);
        for(size_t i = 0 ; i < sz; i ++ ){
          clumps.AddTxn(&txns[r][i]);
        }

        bool is_ok = true;
        std::vector<int> used(clumps.Size(), 0);
        std::vector<int> priority = clumps.Sort();

        do {
          // start tradeoff for balancing
          bool useClump = false;
          for(int i = 0 ; i < priority.size(); i ++ ){ 
            int cur_idx = priority[i];
            if(used[cur_idx]) continue;
            auto& cur = clumps.At(cur_idx);

            std::unordered_map<size_t, int> overload_node;
            std::unordered_map<size_t, int> idle_node;

            find_imbalanced_node(overload_node, idle_node, 
                                cur_busy, aver_val, threshold);
            if(overload_node.size() == 0 || idle_node.size() == 0){
              is_ok = false;
              break;
            }
            // std::shared_ptr<simpleTransaction> t = q_.top();
            // q_.pop();

            if(overload_node.count(cur.dest)){
              // find minial cost in idle_node
              auto [idle_coord_id, min_cost] = cur.CalIdleNode(idle_node, 
                                                (workload_type == 1 || context.lion_with_metis_init == 0));
              if(idle_coord_id == -1){
                continue;
              }
              used[cur_idx] = true;
              useClump = true;
              // minus busy
              cur_busy[cur.dest] -= cur.hot;
              overload_node[cur.dest] -= cur.hot;
              if(overload_node[cur.dest] <= 0){
                overload_node.erase(cur.dest);
              }
              // 
              cur.UpdateDest(idle_coord_id);
              // 
              cur_busy[idle_coord_id] += cur.hot;
              idle_node[idle_coord_id] -= cur.hot;
              if(idle_node[idle_coord_id] <= 0){
                idle_node.erase(idle_coord_id);
              }
              LOG(INFO) << " after move : " << cur.hot;
              for(size_t j = 0 ; j < context.coordinator_num; j ++ ){
                LOG(INFO) <<" busy[" << j << "] = " << cur_busy[j];
              }
            }
            if(cal_load_distribute(aver_val, cur_busy) <= threshold) {
              break;
            }
            if(overload_node.size() == 0 || idle_node.size() == 0){
              break;
            }

          }
          if(!useClump){
            is_ok = false;
          }
        } while(cal_load_distribute(aver_val, cur_busy) > threshold && is_ok);
      // }
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
    // LOG(INFO) << "txn_id.load() = " << schedule_meta.txn_id.load() << " " << cur_txn_num;
    
    std::vector<std::vector<int>> busy_local(replica_num, std::vector<int>(context.coordinator_num, 0));
    int real_distribute_num = 0;
    for(size_t i = 0; i < (size_t)cur_txn_num; i ++ ){
      bool success = false;
      std::shared_ptr<simpleTransaction> new_txn(transactions_queue_self[dispatcher_id].pop_no_wait(success)); 
      int idx = i + idx_offset;

      for(size_t r = 0 ; r < replica_num; r ++ ){
        txns[r][idx] = *new_txn;
        auto& txn = txns[r][idx];
        txn.idx_ = idx;      

        if(WorkloadType::which_workload == myTestSet::YCSB){
          txn_replica_involved(&txn, r, txns_coord_cost[r]);
        } else {
          txn_replica_involved_tpcc(&txn, r, txns_coord_cost[r]);
        }
        // txn_nodes_involved(txn.get(), false, txns_coord_cost);
        busy_local[r][txn.destination_coordinator] += 1;
      }
    }

    double cur_timestamp__ = std::chrono::duration_cast<std::chrono::microseconds>(
                 std::chrono::steady_clock::now() - staart)
                 .count() * 1.0 / 1000 ;              
    if(real_distribute_num > 0){
      LOG(INFO) << "real_distribute_num = " << real_distribute_num;
    }

    LOG(INFO) << "scheduler : " << cur_timestamp__ << " " << schedule_meta.txn_id.load();
    schedule_meta.txn_id.fetch_add(1);
    {
      std::lock_guard<std::mutex> l(schedule_meta.l);
      for(size_t r = 0; r < replica_num; r ++ ){
        for(size_t i = 0; i < context.coordinator_num; i ++ ){
            busy_[r][i] += busy_local[r][i];
        }
      }

    }

    while((int)schedule_meta.txn_id.load() < dispatcher_num){
      auto i = schedule_meta.txn_id.load();
      std::this_thread::sleep_for(std::chrono::microseconds(5));
    }

    long long threshold = 400 * 400;
    if(WorkloadType::which_workload == myTestSet::YCSB){
      long long threshold = (0.1 * context.batch_size / context.coordinator_num) * (0.1 * context.batch_size / context.coordinator_num); 

      if(cur_timestamp > 10){
        threshold = (0.2 * context.batch_size / context.coordinator_num) * (0.2 * context.batch_size / context.coordinator_num); 
      }
    }
    int aver_val =  cur_txn_num * dispatcher_num / context.coordinator_num;
    

    if(dispatcher_id == 0){
    
      for(size_t r = 0 ; r < replica_num; r ++ ){
        long long cur_val = cal_load_distribute(aver_val, busy_[r]);
        if(cur_val > threshold && context.random_router == 0){ 


          if(WorkloadType::which_workload == myTestSet::YCSB){
            balance_master(r, aver_val, threshold);
          } else {
            balance_master_tpcc(r, aver_val, threshold);
          }
          cur_timestamp__ = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - staart)
                      .count() * 1.0 / 1000 ;
          LOG(INFO) << "scheduler + reorder : " << cur_timestamp__  << " " << cur_val << " " << aver_val << " " << threshold;

          LOG(INFO) << " after: ";
          auto& cur_busy = busy_[r];
          for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
            LOG(INFO) <<" busy[" << i << "] = " << cur_busy[i];
          }
        }

      }

    schedule_meta.reorder_done.store(true);
    }


    while(schedule_meta.reorder_done.load() == false){
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

    void txn_replica_involved_tpcc(simpleTransaction* t, 
                                   int replica_id,
                                   std::vector<std::vector<int>>& txns_coord_cost_) {
      // 
      int replica_master_coordinator[2] = {0};
    
      int replica_destination = -1;

      int weight_w = 10;
      int weight_d = 5;
      int weight_c = 1;
      int weight_s = 1;

      size_t weight_sum = 0;

      // check master num at this replica on each node
      size_t from_nodes_id[MAX_COORDINATOR_NUM] = {0};
      size_t master_max_cnt = 0;

      std::vector<int> query_keys;
      star::tpcc::NewOrderQuery keys;
      keys.unpack_transaction(*t);
      // warehouse_key
      auto warehouse_key = tpcc::warehouse::key(keys.W_ID);
      size_t warehouse_coordinator_id = db.get_dynamic_coordinator_id(
                                            context.coordinator_num, 
                                            tpcc::warehouse::tableID, 
                                            (void*)& warehouse_key, replica_id);
      from_nodes_id[warehouse_coordinator_id] += weight_w;
      weight_sum += weight_w;
      query_keys.push_back(warehouse_coordinator_id);
      // district_key
      auto district_key = tpcc::district::key(keys.W_ID, keys.D_ID);
      size_t district_coordinator_id = db.get_dynamic_coordinator_id(
                                            context.coordinator_num, 
                                            tpcc::district::tableID, 
                                            (void*)& district_key, replica_id);
      from_nodes_id[district_coordinator_id] += weight_d;
      weight_sum += weight_d;
      query_keys.push_back(district_coordinator_id);

      // customer_key
      auto customer_key = tpcc::customer::key(keys.W_ID, keys.D_ID, keys.C_ID);
      size_t customer_coordinator_id = db.get_dynamic_coordinator_id(
                                            context.coordinator_num, 
                                            tpcc::customer::tableID, 
                                            (void*)& customer_key, replica_id);

      from_nodes_id[customer_coordinator_id] += weight_c;
      weight_sum += weight_c;
      query_keys.push_back(customer_coordinator_id);
        
      for(size_t i = 0 ; i >= 3 && i < t->keys.size() - 3; i ++ ){
        auto stock_key = tpcc::stock::key(keys.INFO[i].OL_SUPPLY_W_ID, keys.INFO[i].OL_I_ID);
        size_t stock_coordinator_id = db.get_dynamic_coordinator_id(
                                            context.coordinator_num, 
                                            tpcc::stock::tableID, 
                                            (void*)& stock_key, replica_id);

        from_nodes_id[stock_coordinator_id] += weight_s;
        weight_sum += weight_s;
        query_keys.push_back(stock_coordinator_id);
      }

    for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      if(from_nodes_id[i] > master_max_cnt){
        master_max_cnt = from_nodes_id[i];
        replica_destination = i;
      }
    }

    // int max_node = -1;
    int max_cnt = INT_MIN;
    for(size_t cur_c_id = 0 ; cur_c_id < context.coordinator_num; cur_c_id ++ ){
      int cur_score = 0; // 5 - 5 * (busy_local[cur_c_id] * 1.0 / cur_txn_num); // 1 ~ 10
      size_t cnt_master = from_nodes_id[cur_c_id];
      if(context.migration_only){
        cur_score += 100 * cnt_master;
      } else {
        if(cnt_master == weight_sum){
          cur_score += 100 * weight_sum;
        } else {
          cur_score += 25 * cnt_master;
        }
      }
      if(cur_score > max_cnt){
        // max_node = cur_c_id;
        max_cnt = cur_score;
      }
      txns_coord_cost_[t->idx_][cur_c_id] = 10 * (int)weight_sum - cur_score;
    }

    t->on_replica_id = replica_id;
    t->destination_coordinator = replica_destination;
    if(master_max_cnt == weight_sum){
      t->is_real_distributed = false;
    } else {
      t->is_real_distributed = true;
    }
    t->execution_cost = 10 * weight_sum - max_cnt;

     return;
   }

  void txn_replica_involved(simpleTransaction* t, int replica_id,
                            std::vector<std::vector<int>>& txns_coord_cost_) {
    auto query_keys = t->keys;
    // 
    int replica_master_coordinator[2] = {0};
  
    int replica_destination = -1;
    
    // check master num at this replica on each node
    size_t from_nodes_id[MAX_COORDINATOR_NUM] = {0};
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

    // int max_node = -1;
    int max_cnt = INT_MIN;
    for(size_t cur_c_id = 0 ; cur_c_id < context.coordinator_num; cur_c_id ++ ){
      int cur_score = 0; // 5 - 5 * (busy_local[cur_c_id] * 1.0 / cur_txn_num); // 1 ~ 10
      size_t cnt_master = from_nodes_id[cur_c_id];
      if(context.migration_only){
        cur_score += 100 * cnt_master;
      } else {
        if(cnt_master == query_keys.size()){
          cur_score += 100 * (int)query_keys.size();
        } else {
          cur_score += 25 * cnt_master;
        }
      }
      if(cur_score > max_cnt){
        // max_node = cur_c_id;
        max_cnt = cur_score;
      }
      txns_coord_cost_[t->idx_][cur_c_id] = 10 * (int)query_keys.size() - cur_score;
    }

    t->on_replica_id = replica_id;
    t->destination_coordinator = replica_destination;
    if(master_max_cnt == query_keys.size()){
      t->is_real_distributed = false;
    } else {
      t->is_real_distributed = true;
    }
    t->execution_cost = 10 * (int)query_keys.size() - max_cnt;

    // LOG(INFO) << t->idx_ << " " << t->keys[0] << " " << t->keys[1] << " " << replica_id;
    return;
   }

  int pin_thread_id_ = 3;
  void router_request(std::vector<int>& router_send_txn_cnt, simpleTransaction* txn) {
    int i = txn->destination_coordinator;
    messages_mutex[i]->lock();
    size_t router_size = ControlMessageFactory::new_router_transaction_message(
        *async_messages[i].get(), 0, *txn, 
        RouterTxnOps::TRANSFER);
    flush_message(async_messages, i);
    messages_mutex[i]->unlock();
    // LOG(INFO) << "TXN : " << txn->keys[0] << " " << txn->keys[1] << " -> " << coordinator_id_dst;
    router_send_txn_cnt[i]++;
    n_network_size.fetch_add(router_size);
    router_transactions_send.fetch_add(1);
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

    // main loop
    for (;;) {
      ExecutorStatus status;
      do {
        // exit 
        status = static_cast<ExecutorStatus>(worker_status.load());
        process_request();
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

      
      VLOG_IF(DEBUG_V, id==0) << "worker " << id << " ready to process_request";

      router_send_txn_cnt.resize(context.coordinator_num, 0);

      schedule_meta.start_schedule.store(1);
      // wait for end
      while(schedule_meta.done_schedule_out.load() < context.worker_num * context.coordinator_num && status != ExecutorStatus::EXIT){
        std::this_thread::sleep_for(std::chrono::microseconds(5));
        status = static_cast<ExecutorStatus>(worker_status.load());
      }

      auto cur_timestamp__ = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - test)
                  .count() * 1.0 / 1000;
      // LOG(INFO) << "send : " << cur_timestamp__;

      // 
      for (auto l = 0u; l < context.coordinator_num; l++){
        if(l == context.coordinator_id){
          continue;
        }
        // LOG(INFO) << "SEND ROUTER_STOP " << id << " -> " << l;
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

  std::vector<int> generator_core_id;
  std::vector<int> dispatcher_core_id;

  std::vector<std::thread> dispatcher;

  std::vector<int> router_send_txn_cnt;


  size_t replica_num;
  size_t ycsbTableID;

  // vector<ITable *> router_table_vec;

  size_t generator_num;

  std::mutex mm;
  std::atomic<uint32_t> router_transactions_send, router_transaction_done;

  DatabaseType &db;
  const ContextType &context;
  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  hermes::ScheduleMeta &schedule_meta;

  ShareQueue<simpleTransaction*, 40960> transactions_queue_self[MAX_COORDINATOR_NUM];
  StorageType storages[MAX_COORDINATOR_NUM];
  std::atomic<uint32_t> is_full_signal_self[MAX_COORDINATOR_NUM];
  std::atomic<int> coordinator_send[MAX_COORDINATOR_NUM];

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

  ShareQueue<simpleTransaction> router_transactions_queue;
  std::deque<int> router_stop_queue;

  std::vector<
      std::function<void(MessagePiece, Message &, 
                         DatabaseType &, const ContextType &, Partitioner *,
                        //  ITable &,
                         std::vector<std::unique_ptr<TransactionType>> &)>>
      messageHandlers;

  std::vector<
    std::function<void(MessagePiece, Message &, DatabaseType &, ShareQueue<simpleTransaction>*, std::deque<int>*)>>
    controlMessageHandlers;    

  std::vector<std::size_t> message_stats, message_sizes;
  LockfreeQueue<Message *, 100860> in_queue, out_queue;
  std::vector<std::vector<int>> txns_coord_cost;
  std::vector<std::unique_ptr<TransactionType>> transactions;


  int dispatcher_num;
  int cur_txn_num;

};
} // namespace group_commit

} // namespace star