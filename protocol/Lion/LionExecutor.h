//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include "core/Partitioner.h"

#include "common/BufferedFileWriter.h"
#include "common/Percentile.h"
#include "common/HashMap.h"

#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Lion/Lion.h"
#include "protocol/Lion/LionQueryNum.h"

#include <limits.h>
#include <chrono>
#include <deque>
#include <unordered_set>
#include <unordered_map>

namespace star {


template <class Workload> class LionExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;
  using TransactionType = typename WorkloadType::TransactionType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = Lion<DatabaseType>;

  using MessageType = LionMessage;
  using MessageFactoryType = LionMessageFactory;
  using MessageHandlerType = LionMessageHandler<DatabaseType>;

  LionExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
               const ContextType &context, uint32_t &batch_size,
               std::atomic<uint32_t> &worker_status,
               std::atomic<uint32_t> &n_complete_workers,
               std::atomic<uint32_t> &n_started_workers,
               HashMap<9916, std::string, int> &data_pack_map)
               // std::unordered_map<std::string, int> &data_pack_map)
               // LockfreeQueueMulti<data_pack*, 8064 > &data_pack_queue)
      : Worker(coordinator_id, id), db(db), context(context),
        batch_size(batch_size),
        l_partitioner(std::make_unique<LionDynamicPartitioner<Workload> >(
            context.coordinator_id, context.coordinator_num, db)),
        s_partitioner(std::make_unique<LionStaticPartitioner<Workload> >(
            context.coordinator_id, context.coordinator_num, db)),    
        random(reinterpret_cast<uint64_t>(this)), worker_status(worker_status),
        n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        data_pack_map(data_pack_map),
        // data_pack_queue(data_pack_queue),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)) {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);

      sync_messages.emplace_back(std::make_unique<Message>());
      init_message(sync_messages[i].get(), i);

      async_messages.emplace_back(std::make_unique<Message>());
      init_message(async_messages[i].get(), i);

      record_messages.emplace_back(std::make_unique<Message>());
      init_message(record_messages[i].get(), i);
    }

    partitioner = l_partitioner.get(); // nullptr;

    messageHandlers = MessageHandlerType::get_message_handlers();

    if (context.log_path != "") {
      std::string filename =
          context.log_path + "_" + std::to_string(id) + ".txt";
      logger = std::make_unique<BufferedFileWriter>(filename.c_str());
    }

    s_context = context.get_single_partition_context();
    c_context = context.get_cross_partition_context();

    s_protocol = new ProtocolType(db, s_context, *s_partitioner, id);
    c_protocol = new ProtocolType(db, c_context, *l_partitioner, id);

    my_batch_size = batch_size;

    // sync responds that need to be received 
    async_message_num.store(0);
    async_message_respond_num.store(0);
  }
  void trace_txn(){
    if(coordinator_id == 0 && id == 0){
      std::ofstream outfile_excel;
      outfile_excel.open("/Users/lion/project/01_star/star/result_tnx.xls", std::ios::trunc); // ios::trunc

      outfile_excel << "cross_partition_txn" << "\t";
      for(size_t i = 0 ; i < res.size(); i ++ ){
        std::pair<size_t, size_t> cur = res[i];
        outfile_excel << cur.first << "\t";
      }
      outfile_excel << "\n";
      outfile_excel << "single_partition_txn" << "\t";
      for(size_t i = 0 ; i < res.size(); i ++ ){
        std::pair<size_t, size_t> cur = res[i];
        outfile_excel << cur.second << "\t";
      }
      outfile_excel.close();
    }
  }

  void replication_fence(){
    // 
    while(async_message_num.load() != async_message_respond_num.load()){
      auto i = async_message_num.load();
      auto ii = async_message_respond_num.load();

      process_request();
      std::this_thread::yield();
    }
    async_message_num.store(0);
    async_message_respond_num.store(0);
  }

  void unpack_route_transaction(WorkloadType& c_workload, StorageType& storage){
    while(!router_transactions_queue.empty()){
      simpleTransaction simple_txn = router_transactions_queue.front();
      router_transactions_queue.pop_front();

      auto p = c_workload.unpack_transaction(context, 0, storage, simple_txn);
      if(simple_txn.op == RouterTxnOps::LOCAL){
        r_single_transactions_queue.push_back(std::move(p));
      } else {
        r_transactions_queue.push_back(std::move(p));
      }
      
    }
  }
  
  void start() override {
    LOG(INFO) << "Executor " << id << " starts.";

    // C-Phase to S-Phase, to C-phase ...
    int times = 0;
    auto now = std::chrono::steady_clock::now();

    if(id == 0 && coordinator_id == 0){
      outfile_excel.open("/home/zqs/project/star/data/result.xls", std::ios::trunc); // ios::trunc

      outfile_excel << "timestamp" << "\t" << "txn-items" << "\n";
    }

    for (;;) {
      auto begin = std::chrono::steady_clock::now();
      ExecutorStatus status;
      size_t lion_king_coordinator_id;
      bool is_lion_king = false;
      times ++ ;
      
      do {
        std::tie(lion_king_coordinator_id, status) = split_signal(static_cast<ExecutorStatus>(worker_status.load()));
        process_request();
        if (status == ExecutorStatus::EXIT) {
          // commit transaction in s_phase;
          commit_transactions();
          LOG(WARNING) << "Executor " << id << " exits.";
          VLOG_IF(DEBUG_V, id == 0) << "TIMES : " << times; 
          return;
        }
      } while (status != ExecutorStatus::C_PHASE);

      // commit transaction in s_phase;
      commit_transactions();

      VLOG_IF(DEBUG_V, id == 0) << "worker " << id << " prepare_transactions_to_run";
      
      WorkloadType c_workload = WorkloadType (coordinator_id, db, random, *l_partitioner.get(), start_time);
      WorkloadType s_workload = WorkloadType (coordinator_id, db, random, *s_partitioner.get(), start_time);
      StorageType storage;

      is_lion_king = (coordinator_id == lion_king_coordinator_id);
      // 准备transaction
      now = std::chrono::steady_clock::now();

      prepare_transactions_to_run(c_workload, s_workload, storage, is_lion_king);

      VLOG_IF(DEBUG_V, id == 0) << "prepare_transactions_to_run "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - now)
                     .count()
              << " milliseconds.";
      now = std::chrono::steady_clock::now();

      VLOG_IF(DEBUG_V, id == 0) << id << " prepare_transactions_to_run \n" << 
              "c cross \\ single = " << c_transactions_queue.size() << " \\" << c_single_transactions_queue.size() << " \n" << 
              "r cross \\ single = " << r_transactions_queue.size() << " \\" << r_single_transactions_queue.size() << " \n" << 
              "s single = " << s_transactions_queue.size();

      // c_phase

      VLOG_IF(DEBUG_V, id == 0) << "worker " << id << " c_phase " << lion_king_coordinator_id;
      if (is_lion_king) {
        VLOG_IF(DEBUG_V, id == 0) << "worker " << id << " ready to run_transaction_with_router";
        n_started_workers.fetch_add(1);

        VLOG_IF(DEBUG_V, id == 0) << "planning_ratio: " << planning_ratio;
        if(WorkloadType::which_workload == myTestSet::YCSB){
          if(planning_ratio == 0){
            my_batch_size = batch_size; // 10000;
          } else {
            my_batch_size = batch_size; // int(1000 / planning_ratio);
          }
          // if(planning_ratio > 0){
          //   transaction_planning();
          //   run_transaction_with_router(ExecutorStatus::C_PHASE, async_message_num);
          // } else {
            run_transaction(ExecutorStatus::C_PHASE, &c_transactions_queue, async_message_num, true);
          // }
        } else if(WorkloadType::which_workload == myTestSet::TPCC){
          run_transaction(ExecutorStatus::C_PHASE, &c_transactions_queue, async_message_num, true);
        } else {
          DCHECK(false);
        }

        replication_fence();
        n_complete_workers.fetch_add(1);
        VLOG_IF(DEBUG_V, id == 0) << "worker " << id << " finish run_transaction_with_router";

      } else {
        
        n_started_workers.fetch_add(1);
        VLOG_IF(DEBUG_V, id == 0) << "worker " << id << " ready to process_request";
        
        while (signal_unmask(static_cast<ExecutorStatus>(worker_status.load())) ==
               ExecutorStatus::C_PHASE) {
          process_request();
          unpack_route_transaction(c_workload, storage);
          if(!r_transactions_queue.empty()){
            run_transaction(ExecutorStatus::C_PHASE, &r_transactions_queue, async_message_num);
          }
          // run_local_transaction(ExecutorStatus::C_PHASE, &r_single_transactions_queue, async_message_num, 1);
        }
        VLOG_IF(DEBUG_V, id == 0) << "worker " << id << " finish to process_request";
        // process replication request after all workers stop.
        process_request();
        n_complete_workers.fetch_add(1);
      }


     VLOG_IF(DEBUG_V, id == 0) << "c_phase "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - now)
                     .count()
              << " milliseconds.";
      now = std::chrono::steady_clock::now();

      // wait to s_phase
      VLOG_IF(DEBUG_V, id == 0) << "worker " << id << " wait to s_phase";
      
      while (signal_unmask(static_cast<ExecutorStatus>(worker_status.load())) !=
             ExecutorStatus::S_PHASE) {
        process_request(); 
        // unpack_route_transaction(c_workload, storage);
        // run_local_transaction(ExecutorStatus::C_PHASE, &c_single_transactions_queue, async_message_num, 1);
        // run_local_transaction(ExecutorStatus::C_PHASE, &r_single_transactions_queue, async_message_num, 1);
      }

      VLOG_IF(DEBUG_V, id == 0) << "wait for switch "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - now)
                     .count()
              << " milliseconds.";
      now = std::chrono::steady_clock::now();

      // commit transaction in c_phase;
      commit_transactions();

      // s_phase

      n_started_workers.fetch_add(1);

      run_transaction(ExecutorStatus::C_PHASE, &r_single_transactions_queue, async_message_num);
      run_transaction(ExecutorStatus::C_PHASE, &c_single_transactions_queue, async_message_num);

      VLOG_IF(DEBUG_V, id == 0) << "c_single_transactions_queue "
       << std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::now() - now)
              .count()
       << " milliseconds.";
      now = std::chrono::steady_clock::now();

      // 
      // if(context.lion_no_switch == true){
      //   run_transaction(ExecutorStatus::C_PHASE, &s_transactions_queue, async_message_num);
      // } else {
        // do switch
      run_transaction(ExecutorStatus::S_PHASE, &s_transactions_queue, async_message_num);
      // }

      replication_fence();
      n_complete_workers.fetch_add(1);

      VLOG_IF(DEBUG_V, id == 0) << "s_phase "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - now)
                     .count()
              << " milliseconds.";
      now = std::chrono::steady_clock::now();

      // once all workers are stop, we need to process the replication
      // requests
      while (signal_unmask(static_cast<ExecutorStatus>(worker_status.load())) ==
             ExecutorStatus::S_PHASE) {
        process_request();
      }

      // n_complete_workers has been cleared
      process_request();
      n_complete_workers.fetch_add(1);

      VLOG_IF(DEBUG_V, id == 0) << "wait for switch back "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - now)
                     .count()
              << " milliseconds.";
      now = std::chrono::steady_clock::now();

      VLOG_IF(DEBUG_V, id == 0) << "whole batch "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";
    }

    VLOG_IF(DEBUG_V, id == 0) << "TIMES : " << times; 

    if(id == 0 && coordinator_id == 0){
      outfile_excel.close();
    }
    

  }

  void get_txn_template(TransactionType* cur_transaction){
    /**
     * @brief 根据分区涉及情况，抽象成template
     * 
     */
    /* trace transactions */
    auto items_ = cur_transaction->get_query();
    std::set<int> partition_;
    for(auto i: items_){
      partition_.insert(i / context.keysPerPartition);
    }
    std::string template_name = "";
    for(auto it = partition_.begin(); it != partition_.end(); it ++ ){
      template_name += "_" + std::to_string(*it);
    }
    if(!data_pack_map.contains(template_name)){
      data_pack_map.insert(template_name, 1);
    } else {
      data_pack_map[template_name] ++;
    }
  }
  void prepare_transactions_to_run(WorkloadType& c_workload, WorkloadType& s_workload, StorageType& storage, 
    bool is_lion_king){
    /** 
     * @brief 准备需要的txns
     * @note add by truth 22-01-24
     */
    std::size_t query_num = 0;
    std::size_t query_num_sum = 0;

    ContextType phase_context; 

    planning_ratio = 0;

    std::vector<ExecutorStatus> cur_status;
    cur_status.push_back(ExecutorStatus::C_PHASE);
    cur_status.push_back(ExecutorStatus::S_PHASE);

    for(int round = 0; round < 2 ; round ++ ){
      // 当前状态, 依次遍历两个phase
      auto status = cur_status[round];

      if (status == ExecutorStatus::C_PHASE) {
        query_num =
             LionQueryNum<ContextType>::get_c_phase_query_num(context, my_batch_size, is_lion_king);
        phase_context = context.get_cross_partition_context(); 
        if(id == 0){
          ////  // LOG(INFO) << "debug";
        }
      } else if (status == ExecutorStatus::S_PHASE) {
        query_num =
            LionQueryNum<ContextType>::get_s_phase_query_num(context, my_batch_size);
        phase_context = context.get_single_partition_context(); 
        if(id == 0){
          ////  // LOG(INFO) << "debug";
        }
      } else {
        CHECK(false);
      }   
      query_num_sum += query_num;
      
      uint64_t last_seed = 0;

      for (auto i = 0u; i < query_num; i++) {
        std::unique_ptr<TransactionType> cur_transaction;

        
        if (status == ExecutorStatus::C_PHASE) {
          std::size_t partition_id = get_partition_id(ExecutorStatus::C_PHASE);
          cur_transaction = c_workload.next_transaction(c_context, partition_id, storage);

          get_txn_template(cur_transaction.get());


          // if(id == 0 && coordinator_id == 0){
          //   outfile_excel << current_timestamp << "\t";
          //   for(auto item: items_){
          //     outfile_excel << item << "\t";
          //   }
          //   outfile_excel << "\n";
          // }

        } else {
          if(context.lion_no_switch == true || context.protocol == "LionNS"){
            std::size_t partition_id = get_partition_id(ExecutorStatus::C_PHASE);
            cur_transaction = c_workload.next_transaction(s_context, partition_id, storage);
          } else {
            std::size_t partition_id = get_partition_id(ExecutorStatus::S_PHASE);
            cur_transaction = s_workload.next_transaction(s_context, partition_id, storage);
          }
        }
        // 甄别一下？
        // first figure out which can be execute on S_Phase(static_replica)
        bool is_cross_txn_static = cur_transaction->check_cross_node_txn(false);
        
        if(is_cross_txn_static && status == ExecutorStatus::C_PHASE){ 
            // TODO: 暂时不考虑部分副本处理跨分区事务...
            std::set<int> from_nodes_id = std::move(cur_transaction->txn_nodes_involved(true));
            bool is_cross_node_global = from_nodes_id.size() > 1; // on the same-node or not

            from_nodes_id.insert(context.coordinator_id);
            bool is_cross_txn_dynamic = from_nodes_id.size() > 1;

            if(is_cross_node_global){
              planning_ratio ++ ;
            }

            if(is_cross_txn_dynamic){
              c_transactions_queue.push_back(std::move(cur_transaction));
            } else {
              c_single_transactions_queue.push_back(std::move(cur_transaction));
            }
            // }
        } else {
          s_transactions_queue.push_back(std::move(cur_transaction));
        }

      } // END FOR
    }

    planning_ratio /= query_num_sum;
    return;
  }
  
  void commit_transactions() {
    /**
     * @brief 
     * 
     */

    while (!q.empty()) {
      auto &ptr = q.front();
      auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - ptr->startTime)
                         .count();
      // if (context.star_sync_in_single_master_phase){
      //   Lion<DatabaseType>::sync_messages(*ptr);
      // }
      percentile.add(latency);
      q.pop();
    }
  }

  std::size_t get_partition_id(ExecutorStatus status) {
    /***
     * e.g. 3C  2Thread/per c  6P
     * 
     * C_PHASE (+)
     *  partition_num_per_coordinator = 4
     *  partition_num_per_thread = 2
     * 
     *  C0 worker0 -> P0    = 12 / 2 * 0 + 3 * [0, 2) + 0 =     // (0 - 1 + 3) % 3
     *                P3    
     *   
     *  C0 worker1 -> P6    = 12 / 2 * 1 + 3 * [0, 2) + 0 =
     *                P9
     * 
     *  C1 worker1 -> P7
     *                P10   = 10 + (3 - 1) % 12 = 0
     * 
     * 
     *  C2 worker1 -> P11   = 11 + (3 - 1) % 12 = 1
     *
     * 
     * S_PHASE (-)
     *  C0 worker0 -> P0    = 0 * 3 + 0 = 0
     *  C0 worker1 -> P3    = 1 * 3 + 0 = 3
     * 
     *  C1 worker0 -> P0    = 0 * 3 + 1 = 1
     *  C1 worker1 -> P3    = 1 * 3 + 1 = 4
     * 
     *     ----------------------
     *     | C0   | C1   | C2   |
     *|P0  | -    | +    |      |
     *|P1  |      | -    | +    |
     *|P2  | +    |      | -    | 
     *|P3  | -    | +    |      |
     *|P4  |      | -    | +    |
     *|P5  | +    |      | -    | 
     *|P6  | -    | +    |      |
     *|P7  |      | -    | +    |
     *|P8  | +    |      | -    | 
     *|P9  | -    | +    |      |
     *|P10 |      | -    | +    |
     *|P11 | +    |      | -    | 
     *     ----------------------
     * 
     * 
    */
    std::size_t partition_id;

    if (status == ExecutorStatus::C_PHASE) {
      // 从当前线程管的分区里随机选一个

      // CHECK(coordinator_id == 0);
      CHECK(context.partition_num / context.coordinator_num % context.worker_num == 0);
      CHECK(context.partition_num % context.coordinator_num == 0);
      CHECK(context.partition_num % context.worker_num == 0);

      auto partition_num_per_coordinator = 
          context.partition_num / context.coordinator_num;

      auto partition_num_per_thread =
          partition_num_per_coordinator / context.worker_num;
      
      
      partition_id = (
                        context.partition_num / context.worker_num * id + // partition_num_per_coordinator
                        context.coordinator_num * random.uniform_dist(0, partition_num_per_thread - 1) + 
                        context.coordinator_id + 
                        context.partition_num - 1 
                     ) 
                     % 
                     context.partition_num;

      // partition_id = id * context.coordinator_num + coordinator_id;

    } else if (status == ExecutorStatus::S_PHASE) {
      // 
      partition_id = id * context.coordinator_num + coordinator_id;
    } else {
      CHECK(false);
    }

    DCHECK(context.partition_num > partition_id);
    return partition_id;
  }

  void router_transaction(ExecutionStep& execute_step){
    TransactionType* txn = transaction.get();
    txn->network_size += MessageFactoryType::new_router_transaction_message(
        *(this->async_messages[execute_step.router_coordinator_id]), ycsb::ycsb::tableID, txn, 
        static_cast<uint64_t>(execute_step.ops));
    flush_async_messages(); 
  }
  bool router_to_other_node(bool is_dynamic){
    bool ret = false;

    std::set<int> node = transaction->txn_nodes_involved(is_dynamic);
    if(node.size() == 1){
      // 
      auto it = node.begin();
      uint32_t router_dest = (*it);
      if(router_dest == context.coordinator_id){
        // local single-partition-txn

      } else {
        // remote node single-parition-txn
        // DCHECK(false);
        TransactionType* txn = transaction.get();
        txn->network_size += MessageFactoryType::new_router_transaction_message(
            *(this->async_messages[router_dest]), ycsb::ycsb::tableID, txn, 
            static_cast<uint64_t>(RouterTxnOps::LOCAL));
        ret = true;
        flush_async_messages(); 
      }
    } else {
      // 

    }
    
    return ret;
  }

  const int remote_transfer_cost_factor = 100;
  // const int remote_router_cost_factor = 200;
  const int remote_remaster_cost_factor = 20;
  const int overload_cost_factor = 1;
  
  std::size_t get_dynamic_coordinator_id (std::vector<std::vector<bool>>& partitions_status, uint64_t key){
    // key -> coordinator_id
    std::size_t ret = context.coordinator_num;
    for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      auto& tab = partitions_status[i];
      if(tab[key] == true){
      // if(tab.find(key) != tab.end()){
        ret = i;
        break;
      }
    }
    DCHECK(ret != context.coordinator_num);
    return ret;
  }
  int same_betch_again(std::vector<int> load_, std::vector<std::vector<bool>> partitions_status){
    // transmit only
    int ret = 0;
    ExecutionStep null_;

    auto it_txn = execution_plan_txn_queue.begin();
    auto it_plan = execution_plan.begin();
    auto size_ = execution_plan.size();
    for(auto index = 0u; index < size_; index ++, it_txn ++, it_plan++ ){
      // ret += cost_min(*it_txn, load_, partitions_status, null_);
    }

    return ret;
  }
  int cost_min(std::unique_ptr<TransactionType> &txn, 
                  std::vector<int>& load_, 
                  std::vector<std::vector<bool>>& partitions_status, 
                  // std::vector<std::vector<int>>& tuple_ditribution,
                  ExecutionStep& execute_step){


    std::vector<int> tuple_ditribution(context.coordinator_num, 0);
    
    int tuple_num = 10; // (int)query.size();
    auto& query = txn.get()->get_query();
    // for(size_t i = 0; i < tuple_num; i ++ ){
    //   // 
    //   auto key = query[i];
    //   tuple_ditribution[get_dynamic_coordinator_id(partitions_status, key)] ++;
    // }

    int ret = INT_MAX;
    // txn.txn_nodes_involved(bool is_dynamic)
    for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      // !TODO remastering

      // 
      int cur_cost = remote_transfer_cost_factor * (tuple_num - tuple_ditribution[i]) + 
                           overload_cost_factor * load_[i];
      if(ret > cur_cost){
        ret = cur_cost;
        execute_step.router_coordinator_id = i;
        execute_step.ops = RouterTxnOps::TRANSFER;
      }
      
    }
    return ret;
  }
  template<typename T>
  T** new_2d_array(int rows, int cols){
    T** mat = new T *[rows];        // 开辟行
    for (int i = 0; i < rows; ++i)       // 开辟列
      mat[i] = new T[cols]();       // 内置类型只能初始化为 0
    return mat;
  }

  template<typename T>
  void delete_2d_array(T** array, int rows){
    for (int i = 0; i < rows; ++i)
      delete[] array[i];
    delete[] array;
  }
  union tpccKey {
      tpccKey(){
          // can't be default
      }
      ycsb::ycsb::key ycsb_key;
      tpcc::warehouse::key w_key;
      tpcc::district::key d_key;
      tpcc::customer::key c_key;
      tpcc::stock::key s_key;
  };

  void get_tpcc_key(uint64_t record_key, int& table_id, tpccKey& key_content){
    table_id = (record_key >> RECORD_COUNT_TABLE_ID_OFFSET);

    int32_t w_id = (record_key & RECORD_COUNT_W_ID_VALID) >> RECORD_COUNT_W_ID_OFFSET;
    int32_t d_id = (record_key & RECORD_COUNT_D_ID_VALID) >> RECORD_COUNT_D_ID_OFFSET;
    int32_t c_id = (record_key & RECORD_COUNT_C_ID_VALID) >> RECORD_COUNT_C_ID_OFFSET;
    int32_t s_id = (record_key & RECORD_COUNT_OL_ID_VALID);
    switch (table_id)
    {
    case tpcc::warehouse::tableID:
        key_content.w_key = tpcc::warehouse::key(w_id); // res = new tpcc::warehouse::key(content);
        break;
    case tpcc::district::tableID:
        key_content.d_key = tpcc::district::key(w_id, d_id);
        break;
    case tpcc::customer::tableID:
        key_content.c_key = tpcc::customer::key(w_id, d_id, c_id);
        break;
    case tpcc::stock::tableID:
        key_content.s_key = tpcc::stock::key(w_id, s_id);
        break;
    default:
        DCHECK(false);
        break;
    }

  }
  void transaction_planning(){
    auto now = std::chrono::steady_clock::now();
    //
    execution_plan_txn_queue.clear();
    execution_plan.clear();

    std::vector<int> load_(context.coordinator_num, 0);

    const int txn_size = c_transactions_queue.size();
    const int coordinator_num_ = context.coordinator_num;

    int remote_router_cost_factor = txn_size / context.coordinator_num;


    int* txn_id = new int[txn_size]();
    int** txn_cost = new_2d_array<int>(txn_size, coordinator_num_);//  = {0};
    int* txn_cost_min = new int[txn_size]();//  = {0};
    int** txn_tuple_distribute = new_2d_array<int>(txn_size, coordinator_num_);//  = {0};

    // int* key_coordinator_id = new int[context.partition_num * context.keysPerPartition]();
    // std::vector<int> ** key_within_txn = new std::vector<int> *[2400000]();

    std::unordered_map<int, int> key_coordinator_id;

    std::unordered_map<int, std::vector<int> > key_within_txn;
    std::unordered_set<int> keys;
   //  // LOG(INFO) << "pre-init "
              // << std::chrono::duration_cast<std::chrono::milliseconds>(
              //        std::chrono::steady_clock::now() - now)
              //        .count()
              // << " milliseconds.";
    // int* key_within_txn_length = new (int)[2400000]();

    // std::vector<int>* key_within_txn = new std::vector<int>[2400000]; 

    // std::vector<int> txn_id(c_transactions_queue.size(), 0);


    // std::vector<std::vector<int>> txn_cost(c_transactions_queue.size(), std::vector<int>(context.coordinator_num, 0));
    // std::vector<int> txn_cost_min(c_transactions_queue.size(), 0);

    // std::vector<std::vector<int>> txn_tuple_distribute(c_transactions_queue.size(), std::vector<int>(context.coordinator_num, 0));
    
    // int* key_within_txn_num = new int[2400000]();

    // int** key_within_txn = new_2d_array<int>(2400000, txn_size);

    // std::vector<std::vector<int>> key_within_txn(2400000, std::vector<int>());

    // std::vector<std::vector<bool>> partitions_status(3, std::vector<bool>(2400000, false));

    // init partition status
    int txn_index = 0;
    size_t tuple_num = 10;
    for(auto it = c_transactions_queue.begin(); it != c_transactions_queue.end(); it ++, txn_index ++ ){
      auto& query = it->get()->get_query();
      size_t tuple_num = query.size();
      txn_id[txn_index] = txn_index;

      // std::vector<int> txn_tuple_distribute(context.coordinator_num, 0);

      for(size_t i = 0; i < tuple_num; i ++ ){
        // 
        auto key = query[i];

        if(key_within_txn.find(key) == key_within_txn.end()){
          key_within_txn[key] = std::vector<int>(); 
        }
        key_within_txn[key].push_back(txn_index);
        // keys.insert(key);
        if(WorkloadType::which_workload == myTestSet::YCSB){
          auto key_ = ycsb::ycsb::key(key);
          auto table_id = ycsb::ycsb::tableID;

          auto coordinator_id_ = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, (void*)& key_);
          
          key_coordinator_id[key] = coordinator_id_;
          txn_tuple_distribute[txn_index][coordinator_id_] ++ ;
        } else if (WorkloadType::which_workload == myTestSet::TPCC) {
          // only consider stock...
          if(i > 2){
            tpccKey key_;
            int table_id;
            get_tpcc_key(key, table_id, key_);
            auto coordinator_id_ = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, (void*)& key_);
            key_coordinator_id[key] = coordinator_id_;
            txn_tuple_distribute[txn_index][coordinator_id_] ++ ;
          }


        } else {
          DCHECK(false);
        }
      }

      // 
      int cur_txn_min_cost = INT_MAX;
      for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
        if(txn_tuple_distribute[txn_index][i] == (int)tuple_num){
          // all in the same node
          txn_cost[txn_index][i] = 0;
        } else {
          txn_cost[txn_index][i] = remote_transfer_cost_factor * ((tuple_num - txn_tuple_distribute[txn_index][i]) > 0) + 
                                  remote_router_cost_factor * (i != context.coordinator_id); 

        }
        cur_txn_min_cost = std::min(cur_txn_min_cost, txn_cost[txn_index][i]);
      }
      txn_cost_min[txn_index] = cur_txn_min_cost;
    }


   //  // LOG(INFO) << "init  "
              // << std::chrono::duration_cast<std::chrono::milliseconds>(
              //        std::chrono::steady_clock::now() - now)
              //        .count()
              // << " milliseconds.";
    //
    for(int i = 0 ; i < txn_size; i ++ ){
      // get min_cost
      int min_cost = INT_MAX;
      int min_cost_txn_index = 0;
      ExecutionStep execution_step;

      for(int j = 0 ; j < txn_size; j ++ ){
        if(txn_id[j] != -1){
          
          if(min_cost > txn_cost_min[j]){
            min_cost = txn_cost_min[j];
            min_cost_txn_index = j;
          }
        }
      }

      execution_step.router_coordinator_id = context.coordinator_num;

      
      for(size_t c = 0 ; c < context.coordinator_num; c ++ ){
        int tmp = txn_cost[min_cost_txn_index][c];

        if(min_cost == txn_cost[min_cost_txn_index][c]){
          execution_step.router_coordinator_id = c;
          // if(execution_step.router_coordinator_id == context.coordinator_id){
          //  //  // LOG(INFO) << "local";
          // }
          break;
        }
      }
      DCHECK(execution_step.router_coordinator_id != context.coordinator_num);
      // 
      if(txn_tuple_distribute[min_cost_txn_index][execution_step.router_coordinator_id] == (int)tuple_num){ 
        execution_step.ops = RouterTxnOps::LOCAL;
      } else {
        execution_step.ops = RouterTxnOps::TRANSFER;
      }

      auto lowest_cost_txn = c_transactions_queue.begin();
      std::advance(lowest_cost_txn, min_cost_txn_index);
      DCHECK(txn_id[min_cost_txn_index] != -1);
      auto query = lowest_cost_txn->get()->get_query();
      int query_num = (int)query.size();

      // other txn add load
      for(int j = 0 ; j < txn_size; j ++ ){
        if(txn_id[j] != -1 && txn_cost[j][execution_step.router_coordinator_id] != 0){
          txn_cost[j][execution_step.router_coordinator_id] += overload_cost_factor;
          ////  // LOG(INFO) << "T" << cur_txn_id << " at N" << execution_step.router_coordinator_id << " : " << txn_cost[cur_txn_id][execution_step.router_coordinator_id] ;
        }
      }
      
      for(int t = 0 ; t < query_num; t ++ ){
        auto key = query[t];
        auto & cur_key_related_txn = key_within_txn[key];
        for(size_t tt = 0 ; tt < cur_key_related_txn.size(); tt ++ ){
          // find txn
          auto cur_txn_id = cur_key_related_txn[tt];
          
          if(key_coordinator_id[key] != (int)execution_step.router_coordinator_id){
            // update cost 
            for(int cc = 0 ; cc < (int)context.coordinator_num; cc ++ ){
              if(cc == key_coordinator_id[key]){
                // source, cost increased
                if(txn_tuple_distribute[cur_txn_id][execution_step.router_coordinator_id] + 1 == 1){
                  // this transfer cause the new remote-read
                  txn_cost[cur_txn_id][cc] += remote_transfer_cost_factor;
                }
                
              } else if(cc == (int)execution_step.router_coordinator_id){
                // destination, cost decreased
                if(txn_tuple_distribute[cur_txn_id][key_coordinator_id[key]] - 1 == 0){
                  // this transfer diminish the old remote-read
                  txn_cost[cur_txn_id][cc] -= remote_transfer_cost_factor;
                }

              }
            }
          }



          // 

        }
        for(size_t tt = 0 ; tt < cur_key_related_txn.size(); tt ++ ){
          // find txn
          auto cur_txn_id = cur_key_related_txn[tt];
          
          if(key_coordinator_id[key] != (int)execution_step.router_coordinator_id){
            // update distribution
            txn_tuple_distribute[cur_txn_id][key_coordinator_id[key]] -- ;
            txn_tuple_distribute[cur_txn_id][execution_step.router_coordinator_id] ++ ;

            if(txn_tuple_distribute[cur_txn_id][execution_step.router_coordinator_id] == query_num){
                // all in the same node
                txn_cost[cur_txn_id][execution_step.router_coordinator_id] = 0;
            }
          }



          // int new_min_cost = INT_MAX;
          // for(int cc = 0 ; cc < (int)context.coordinator_num; cc ++ ){
          //   // if(txn_tuple_distribute[cur_txn_id][cc] == query_num){
          //   //   // all in the same node
          //   //   txn_cost[cur_txn_id][cc] = 0;
          //   // }
          //   new_min_cost = std::min(new_min_cost, txn_cost[cur_txn_id][cc]);
          // }
          // update overload

         
        }

        // update partition-status
        key_coordinator_id[key] = execution_step.router_coordinator_id;
      }
      // 
      
      for(int j = 0 ; j < txn_size; j ++ ){
        if(txn_id[j] != -1){
          int new_min_cost = INT_MAX;
          for(int cc = 0 ; cc < (int)context.coordinator_num; cc ++ ){
            // if(txn_tuple_distribute[cur_txn_id][cc] == query_num){
            //   // all in the same node
            //   txn_cost[cur_txn_id][cc] = 0;
            // }
            new_min_cost = std::min(new_min_cost, txn_cost[j][cc]);
          }
          //update overload
          txn_cost_min[j] = new_min_cost;
        }
      }

      ////  // LOG(INFO) << min_cost_txn_index << " at " << execution_step.router_coordinator_id << " " << (int)(execution_step.ops) << " " << min_cost;
      // std::string haha = "";
      // for(size_t j = 0 ; j < query.size(); j ++ ){
      //   char tmp[200];
      //   sprintf(tmp, "%9d", (int)query[j]);
      //   haha += tmp;
      // }
      ////  // LOG(INFO) << haha;
      // 
      txn_cost_min[min_cost_txn_index] = INT_MAX;
      txn_id[min_cost_txn_index] = -1;
      // get execution-step
      execution_plan.push_back(execution_step);
      execution_plan_txn_queue.push_back(std::move(*lowest_cost_txn));
      // c_transactions_queue.erase(lowest_cost_txn);
    }

    // std::vector<std::unordered_set<uint64_t>> partitions_status;

    // partition[coordinator_id][key] = true / false 
    // txn[key][coordinator_id] = [0, 10)
    // std::vector<std::vector<int>> tuple_ditribution(1200000, std::vector<int>(3, 0));

    // // 
    // size_t table_id = ycsb::ycsb::tableID;
    // // for(size_t i = 0; i < context.coordinator_num; i ++ ){
    // //   std::unordered_set<uint64_t> tab_;
    // //   partitions_status.push_back(tab_);
    // // }
    // // return; 
    // int cost = 0;

    // size_t iter_num = c_transactions_queue.size();

    // for(size_t i = 0 ; i < iter_num; i ++ ){
    //   // 
    //   int cost_temp = 0;
    //   ExecutionStep ideal_result;
    //   auto lowest_cost_txn = c_transactions_queue.begin();

    //   auto fuck = std::chrono::steady_clock::now();

    //   // find the lowest txn
    //   for(auto it = c_transactions_queue.begin(); it != c_transactions_queue.end(); it ++ ){
    //     // 
    //     if(it == c_transactions_queue.begin()){
    //       // initialize
    //       cost_temp = cost + cost_min(*it, load_, partitions_status, ideal_result) + 
    //                   0; // same_betch_again(load_, partitions_status);
          
    //     } else {
    //       // 
    //       ExecutionStep tmp;
    //       int new_cost = cost_min(*it, load_, partitions_status, tmp) + 
    //                            0; // same_betch_again(load_, partitions_status);
    //       if(cost_temp < cost + new_cost){
    //         // 
    //         cost_temp = cost + new_cost;
    //         ideal_result = tmp;
    //         lowest_cost_txn = it;
    //       }
    //     }
    //   }

    //   // //  // LOG(INFO) << "find lowest txn  "
    //   //         << std::chrono::duration_cast<std::chrono::milliseconds>(
    //   //                std::chrono::steady_clock::now() - fuck)
    //   //                .count()
    //   //         << " milliseconds.";

    //   // update partition and load 
    //   auto& query = lowest_cost_txn->get()->get_query();
    //   size_t tuple_num = query.size();
    //   for(size_t i = 0; i < tuple_num; i ++ ){
    //     // partition
    //     auto key = query[i];
    //     auto coordinator_id_old = get_dynamic_coordinator_id(partitions_status, key);
    //     if(coordinator_id_old != ideal_result.router_coordinator_id){
    //       partitions_status[coordinator_id_old][key] = false;
    //       partitions_status[ideal_result.router_coordinator_id][key] = true;
    //     }
    //     // load
    //     load_[ideal_result.router_coordinator_id] ++ ;
    //   }

    //   // update execution plan
    //   execution_plan.push_back(ideal_result);
    //   execution_plan_txn_queue.push_back(std::move(*lowest_cost_txn));
    //   c_transactions_queue.erase(lowest_cost_txn);
    // }
    
   //  // LOG(INFO) << "transaction plan "
              // << std::chrono::duration_cast<std::chrono::milliseconds>(
              //        std::chrono::steady_clock::now() - now)
              //        .count()
              // << " milliseconds.";

    DCHECK(execution_plan_txn_queue.size() == execution_plan.size());
    delete []txn_id; // = new int[txn_size]();
    delete_2d_array(txn_cost, txn_size); // = new_2d_array<int>(txn_size, coordinator_num_);//  = {0};
    delete []txn_cost_min;// = new int[txn_size]();//  = {0};
    delete_2d_array(txn_tuple_distribute, txn_size); //  = new_2d_array<int>(txn_size, coordinator_num_);//  = {0};
    // delete []key_coordinator_id; // = new int[2400000]();
    // for(auto it = keys.begin(); it != keys.end(); it ++ ){
    //   delete key_within_txn[*it];
    // }
    // delete []key_within_txn;

    // std::vector<int>* key_within_txn = new std::vector<int>[2400000]; 
   //  // LOG(INFO) << "delete "
              // << std::chrono::duration_cast<std::chrono::milliseconds>(
              //        std::chrono::steady_clock::now() - now)
              //        .count()
              // << " milliseconds.";

    c_transactions_queue.clear();

    /* debug */
    // auto it_step = execution_plan.begin();
    // auto it_txn = execution_plan_txn_queue.begin();
    // 

    // for(size_t i = 0; i < execution_plan_txn_queue.size(); i ++, it_step++, it_txn++){
    //   //
    //  //  // LOG(INFO) << i << " at " << (*it_step).router_coordinator_id << " " << static_cast<int>((*it_step).ops);
    //   auto query = it_txn->get()->get_query();
    //   std::string haha = "";
    //   for(size_t j = 0 ; j < query.size(); j ++ ){
    //     char tmp[200];
    //     sprintf(tmp, "%9d", (int)query[j]);
    //     haha += tmp;
    //   }
    //  //  // LOG(INFO) << haha;
    // }

    return;
  }

  void transaction_planning_ycsb(){
    auto now = std::chrono::steady_clock::now();
    //
    execution_plan_txn_queue.clear();
    execution_plan.clear();

    std::vector<int> load_(context.coordinator_num, 0);

    const int txn_size = c_transactions_queue.size();
    const int coordinator_num_ = context.coordinator_num;

    int remote_router_cost_factor = txn_size / context.coordinator_num;


    int* txn_id = new int[txn_size]();
    int** txn_cost = new_2d_array<int>(txn_size, coordinator_num_);//  = {0};
    int* txn_cost_min = new int[txn_size]();//  = {0};
    int** txn_tuple_distribute = new_2d_array<int>(txn_size, coordinator_num_);//  = {0};

    // int* key_coordinator_id = new int[context.partition_num * context.keysPerPartition]();
    // std::vector<int> ** key_within_txn = new std::vector<int> *[2400000]();

    std::unordered_map<int, int> key_coordinator_id;

    std::unordered_map<int, std::vector<int> > key_within_txn;
    std::unordered_set<int> keys;
   //  // LOG(INFO) << "pre-init "
              // << std::chrono::duration_cast<std::chrono::milliseconds>(
              //        std::chrono::steady_clock::now() - now)
              //        .count()
              // << " milliseconds.";
    // int* key_within_txn_length = new (int)[2400000]();

    // std::vector<int>* key_within_txn = new std::vector<int>[2400000]; 

    // std::vector<int> txn_id(c_transactions_queue.size(), 0);


    // std::vector<std::vector<int>> txn_cost(c_transactions_queue.size(), std::vector<int>(context.coordinator_num, 0));
    // std::vector<int> txn_cost_min(c_transactions_queue.size(), 0);

    // std::vector<std::vector<int>> txn_tuple_distribute(c_transactions_queue.size(), std::vector<int>(context.coordinator_num, 0));
    
    // int* key_within_txn_num = new int[2400000]();

    // int** key_within_txn = new_2d_array<int>(2400000, txn_size);

    // std::vector<std::vector<int>> key_within_txn(2400000, std::vector<int>());

    // std::vector<std::vector<bool>> partitions_status(3, std::vector<bool>(2400000, false));

    // init partition status
    int txn_index = 0;
    size_t tuple_num = 10;
    for(auto it = c_transactions_queue.begin(); it != c_transactions_queue.end(); it ++, txn_index ++ ){
      auto& query = it->get()->get_query();
      size_t tuple_num = query.size();
      txn_id[txn_index] = txn_index;

      // std::vector<int> txn_tuple_distribute(context.coordinator_num, 0);

      for(size_t i = 0; i < tuple_num; i ++ ){
        // 
        auto key = query[i];

        if(key_within_txn.find(key) == key_within_txn.end()){
          key_within_txn[key] = std::vector<int>(); 
        }
        key_within_txn[key].push_back(txn_index);
        // keys.insert(key);
        if(WorkloadType::which_workload == myTestSet::YCSB){
          auto key_ = ycsb::ycsb::key(key);
          auto table_id = ycsb::ycsb::tableID;

          auto coordinator_id_ = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, (void*)& key_);
          
          key_coordinator_id[key] = coordinator_id_;
          txn_tuple_distribute[txn_index][coordinator_id_] ++ ;
        } else if (WorkloadType::which_workload == myTestSet::TPCC) {
          // only consider stock...
          if(i > 2){
            tpccKey key_;
            int table_id;
            get_tpcc_key(key, table_id, key_);
            auto coordinator_id_ = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, (void*)& key_);
            key_coordinator_id[key] = coordinator_id_;
            txn_tuple_distribute[txn_index][coordinator_id_] ++ ;
          }


        } else {
          DCHECK(false);
        }
      }

      // 
      int cur_txn_min_cost = INT_MAX;
      for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
        if(txn_tuple_distribute[txn_index][i] == (int)tuple_num){
          // all in the same node
          txn_cost[txn_index][i] = 0;
        } else {
          txn_cost[txn_index][i] = remote_transfer_cost_factor * ((tuple_num - txn_tuple_distribute[txn_index][i]) > 0) + 
                                  remote_router_cost_factor * (i != context.coordinator_id); 

        }
        cur_txn_min_cost = std::min(cur_txn_min_cost, txn_cost[txn_index][i]);
      }
      txn_cost_min[txn_index] = cur_txn_min_cost;
    }


  //  //  // LOG(INFO) << "init  "
  //             << std::chrono::duration_cast<std::chrono::milliseconds>(
  //                    std::chrono::steady_clock::now() - now)
  //                    .count()
  //             << " milliseconds.";
    //
    for(int i = 0 ; i < txn_size; i ++ ){
      // get min_cost
      int min_cost = INT_MAX;
      int min_cost_txn_index = 0;
      ExecutionStep execution_step;

      for(int j = 0 ; j < txn_size; j ++ ){
        if(txn_id[j] != -1){
          
          if(min_cost > txn_cost_min[j]){
            min_cost = txn_cost_min[j];
            min_cost_txn_index = j;
          }
        }
      }

      execution_step.router_coordinator_id = context.coordinator_num;

      
      for(size_t c = 0 ; c < context.coordinator_num; c ++ ){
        int tmp = txn_cost[min_cost_txn_index][c];

        if(min_cost == txn_cost[min_cost_txn_index][c]){
          execution_step.router_coordinator_id = c;
          // if(execution_step.router_coordinator_id == context.coordinator_id){
          //  //  // LOG(INFO) << "local";
          // }
          break;
        }
      }
      DCHECK(execution_step.router_coordinator_id != context.coordinator_num);
      // 
      if(txn_tuple_distribute[min_cost_txn_index][execution_step.router_coordinator_id] == (int)tuple_num){ 
        execution_step.ops = RouterTxnOps::LOCAL;
      } else {
        execution_step.ops = RouterTxnOps::TRANSFER;
      }

      auto lowest_cost_txn = c_transactions_queue.begin();
      std::advance(lowest_cost_txn, min_cost_txn_index);
      DCHECK(txn_id[min_cost_txn_index] != -1);
      auto query = lowest_cost_txn->get()->get_query();
      int query_num = (int)query.size();

      // other txn add load
      for(int j = 0 ; j < txn_size; j ++ ){
        if(txn_id[j] != -1 && txn_cost[j][execution_step.router_coordinator_id] != 0){
          txn_cost[j][execution_step.router_coordinator_id] += overload_cost_factor;
          ////  // LOG(INFO) << "T" << cur_txn_id << " at N" << execution_step.router_coordinator_id << " : " << txn_cost[cur_txn_id][execution_step.router_coordinator_id] ;
        }
      }
      
      for(int t = 0 ; t < query_num; t ++ ){
        auto key = query[t];
        auto & cur_key_related_txn = key_within_txn[key];
        for(size_t tt = 0 ; tt < cur_key_related_txn.size(); tt ++ ){
          // find txn
          auto cur_txn_id = cur_key_related_txn[tt];
          
          if(key_coordinator_id[key] != (int)execution_step.router_coordinator_id){
            // update cost 
            for(int cc = 0 ; cc < (int)context.coordinator_num; cc ++ ){
              if(cc == key_coordinator_id[key]){
                // source, cost increased
                if(txn_tuple_distribute[cur_txn_id][execution_step.router_coordinator_id] + 1 == 1){
                  // this transfer cause the new remote-read
                  txn_cost[cur_txn_id][cc] += remote_transfer_cost_factor;
                }
                
              } else if(cc == (int)execution_step.router_coordinator_id){
                // destination, cost decreased
                if(txn_tuple_distribute[cur_txn_id][key_coordinator_id[key]] - 1 == 0){
                  // this transfer diminish the old remote-read
                  txn_cost[cur_txn_id][cc] -= remote_transfer_cost_factor;
                }

              }
            }
          }



          // 

        }
        for(size_t tt = 0 ; tt < cur_key_related_txn.size(); tt ++ ){
          // find txn
          auto cur_txn_id = cur_key_related_txn[tt];
          
          if(key_coordinator_id[key] != (int)execution_step.router_coordinator_id){
            // update distribution
            txn_tuple_distribute[cur_txn_id][key_coordinator_id[key]] -- ;
            txn_tuple_distribute[cur_txn_id][execution_step.router_coordinator_id] ++ ;

            if(txn_tuple_distribute[cur_txn_id][execution_step.router_coordinator_id] == query_num){
                // all in the same node
                txn_cost[cur_txn_id][execution_step.router_coordinator_id] = 0;
            }
          }



          // int new_min_cost = INT_MAX;
          // for(int cc = 0 ; cc < (int)context.coordinator_num; cc ++ ){
          //   // if(txn_tuple_distribute[cur_txn_id][cc] == query_num){
          //   //   // all in the same node
          //   //   txn_cost[cur_txn_id][cc] = 0;
          //   // }
          //   new_min_cost = std::min(new_min_cost, txn_cost[cur_txn_id][cc]);
          // }
          // update overload

         
        }

        // update partition-status
        key_coordinator_id[key] = execution_step.router_coordinator_id;
      }
      // 
      
      for(int j = 0 ; j < txn_size; j ++ ){
        if(txn_id[j] != -1){
          int new_min_cost = INT_MAX;
          for(int cc = 0 ; cc < (int)context.coordinator_num; cc ++ ){
            // if(txn_tuple_distribute[cur_txn_id][cc] == query_num){
            //   // all in the same node
            //   txn_cost[cur_txn_id][cc] = 0;
            // }
            new_min_cost = std::min(new_min_cost, txn_cost[j][cc]);
          }
          //update overload
          txn_cost_min[j] = new_min_cost;
        }
      }

      ////  // LOG(INFO) << min_cost_txn_index << " at " << execution_step.router_coordinator_id << " " << (int)(execution_step.ops) << " " << min_cost;
      // std::string haha = "";
      // for(size_t j = 0 ; j < query.size(); j ++ ){
      //   char tmp[200];
      //   sprintf(tmp, "%9d", (int)query[j]);
      //   haha += tmp;
      // }
      ////  // LOG(INFO) << haha;
      // 
      txn_cost_min[min_cost_txn_index] = INT_MAX;
      txn_id[min_cost_txn_index] = -1;
      // get execution-step
      execution_plan.push_back(execution_step);
      execution_plan_txn_queue.push_back(std::move(*lowest_cost_txn));
      // c_transactions_queue.erase(lowest_cost_txn);
    }

    // std::vector<std::unordered_set<uint64_t>> partitions_status;

    // partition[coordinator_id][key] = true / false 
    // txn[key][coordinator_id] = [0, 10)
    // std::vector<std::vector<int>> tuple_ditribution(1200000, std::vector<int>(3, 0));

    // // 
    // size_t table_id = ycsb::ycsb::tableID;
    // // for(size_t i = 0; i < context.coordinator_num; i ++ ){
    // //   std::unordered_set<uint64_t> tab_;
    // //   partitions_status.push_back(tab_);
    // // }
    // // return; 
    // int cost = 0;

    // size_t iter_num = c_transactions_queue.size();

    // for(size_t i = 0 ; i < iter_num; i ++ ){
    //   // 
    //   int cost_temp = 0;
    //   ExecutionStep ideal_result;
    //   auto lowest_cost_txn = c_transactions_queue.begin();

    //   auto fuck = std::chrono::steady_clock::now();

    //   // find the lowest txn
    //   for(auto it = c_transactions_queue.begin(); it != c_transactions_queue.end(); it ++ ){
    //     // 
    //     if(it == c_transactions_queue.begin()){
    //       // initialize
    //       cost_temp = cost + cost_min(*it, load_, partitions_status, ideal_result) + 
    //                   0; // same_betch_again(load_, partitions_status);
          
    //     } else {
    //       // 
    //       ExecutionStep tmp;
    //       int new_cost = cost_min(*it, load_, partitions_status, tmp) + 
    //                            0; // same_betch_again(load_, partitions_status);
    //       if(cost_temp < cost + new_cost){
    //         // 
    //         cost_temp = cost + new_cost;
    //         ideal_result = tmp;
    //         lowest_cost_txn = it;
    //       }
    //     }
    //   }

    //   // //  // LOG(INFO) << "find lowest txn  "
    //   //         << std::chrono::duration_cast<std::chrono::milliseconds>(
    //   //                std::chrono::steady_clock::now() - fuck)
    //   //                .count()
    //   //         << " milliseconds.";

    //   // update partition and load 
    //   auto& query = lowest_cost_txn->get()->get_query();
    //   size_t tuple_num = query.size();
    //   for(size_t i = 0; i < tuple_num; i ++ ){
    //     // partition
    //     auto key = query[i];
    //     auto coordinator_id_old = get_dynamic_coordinator_id(partitions_status, key);
    //     if(coordinator_id_old != ideal_result.router_coordinator_id){
    //       partitions_status[coordinator_id_old][key] = false;
    //       partitions_status[ideal_result.router_coordinator_id][key] = true;
    //     }
    //     // load
    //     load_[ideal_result.router_coordinator_id] ++ ;
    //   }

    //   // update execution plan
    //   execution_plan.push_back(ideal_result);
    //   execution_plan_txn_queue.push_back(std::move(*lowest_cost_txn));
    //   c_transactions_queue.erase(lowest_cost_txn);
    // }
    
   //  // LOG(INFO) << "transaction plan "
              // << std::chrono::duration_cast<std::chrono::milliseconds>(
              //        std::chrono::steady_clock::now() - now)
              //        .count()
              // << " milliseconds.";

    DCHECK(execution_plan_txn_queue.size() == execution_plan.size());
    delete []txn_id; // = new int[txn_size]();
    delete_2d_array(txn_cost, txn_size); // = new_2d_array<int>(txn_size, coordinator_num_);//  = {0};
    delete []txn_cost_min;// = new int[txn_size]();//  = {0};
    delete_2d_array(txn_tuple_distribute, txn_size); //  = new_2d_array<int>(txn_size, coordinator_num_);//  = {0};
    // delete []key_coordinator_id; // = new int[2400000]();
    // for(auto it = keys.begin(); it != keys.end(); it ++ ){
    //   delete key_within_txn[*it];
    // }
    // delete []key_within_txn;

    // std::vector<int>* key_within_txn = new std::vector<int>[2400000]; 
  //  //  // LOG(INFO) << "delete "
  //             << std::chrono::duration_cast<std::chrono::milliseconds>(
  //                    std::chrono::steady_clock::now() - now)
  //                    .count()
  //             << " milliseconds.";

    c_transactions_queue.clear();

    /* debug */
    // auto it_step = execution_plan.begin();
    // auto it_txn = execution_plan_txn_queue.begin();
    // 

    // for(size_t i = 0; i < execution_plan_txn_queue.size(); i ++, it_step++, it_txn++){
    //   //
    //  //  // LOG(INFO) << i << " at " << (*it_step).router_coordinator_id << " " << static_cast<int>((*it_step).ops);
    //   auto query = it_txn->get()->get_query();
    //   std::string haha = "";
    //   for(size_t j = 0 ; j < query.size(); j ++ ){
    //     char tmp[200];
    //     sprintf(tmp, "%9d", (int)query[j]);
    //     haha += tmp;
    //   }
    //  //  // LOG(INFO) << haha;
    // }

    return;
  }



  void run_transaction_with_router(ExecutorStatus status, std::atomic<uint32_t>& async_message_num) {
    /**
     * @brief 
     * @note modified by truth 22-01-24
     *       
    */
    ProtocolType* protocol;

    if (status == ExecutorStatus::C_PHASE) {
      protocol = c_protocol;
    } else if (status == ExecutorStatus::S_PHASE) {
      protocol = s_protocol;
    } else {
      CHECK(false);
    }

    uint64_t last_seed = 0;

    auto i = 0u;
    std::deque<std::unique_ptr<TransactionType>>* cur_transactions_queue = nullptr;
    cur_transactions_queue = &execution_plan_txn_queue;

    size_t cur_queue_size = cur_transactions_queue->size();
    int router_txn_num = 0;
    auto it_txn = cur_transactions_queue->begin();
    auto it_plan = execution_plan.begin();

    // while(!cur_transactions_queue->empty()){ // 为什么不能这样？ 不是太懂
    for (auto i = 0u; i < cur_queue_size; i++, it_txn++, it_plan++) {
      // if(cur_transactions_queue->empty()){
      //   break;
      // }

      transaction =
              std::move(*it_txn);

      if(it_plan->router_coordinator_id != context.coordinator_id){
        // router_transaction
        router_transaction((*it_plan));
        router_txn_num++;
      } else {
        bool retry_transaction = false;

        do {
          ////  // LOG(INFO) << "LionExecutor: "<< id << " " << "process_request" << i;
          process_request();
          last_seed = random.get_seed();

          if (retry_transaction) {
            transaction->reset();
          } else {
            setupHandlers(*transaction, *protocol);
          }
          // auto result = transaction->execute(id);
          transaction->prepare_read_execute(id);
          // bool get_all_router_lock = protocol->lock_router_set(*transaction);
          // if(get_all_router_lock == false){
          //   retry_transaction = true;
          //   protocol->router_abort(*transaction);
          //   continue;
          // }
          auto result = transaction->read_execute(id, ReadMethods::REMOTE_READ_WITH_TRANSFER);
          if(result != TransactionResult::READY_TO_COMMIT){
            if(result == TransactionResult::ABORT){
                  retry_transaction = true;
                  continue;
            }
          } else {
            result = transaction->prepare_update_execute(id);
          }
          // auto result = transaction->execute(id);

          if (result == TransactionResult::READY_TO_COMMIT) {
            ////  // LOG(INFO) << "LionExecutor: "<< id << " " << "commit" << i;

            bool commit =
                protocol->commit(*transaction, messages, async_message_num); // sync_messages, async_messages, record_messages, 
                                // );
            n_network_size.fetch_add(transaction->network_size);
            if (commit) {
              n_commit.fetch_add(1);
              retry_transaction = false;
              q.push(std::move(transaction));
            } else {
              if (transaction->abort_lock) {
                n_abort_lock.fetch_add(1);
              } else {
                DCHECK(transaction->abort_read_validation);
                n_abort_read_validation.fetch_add(1);
              }
              random.set_seed(last_seed);
              retry_transaction = true;
            }
          } else {
            n_abort_no_retry.fetch_add(1);
          }
        } while (retry_transaction);
      }

      // cur_transactions_queue->pop_front();

      if (i % context.batch_flush == 0) {
        flush_messages(messages); 
        flush_async_messages(); 
        flush_sync_messages();
        flush_record_messages();
        
      }
    }
    flush_messages(messages); 
    flush_async_messages();
    flush_record_messages();
    flush_sync_messages();

    execution_plan.clear();
    execution_plan_txn_queue.clear();

    DCHECK(cur_transactions_queue->size() == 0);
   //  // LOG(INFO) << "router_txn_num: " << router_txn_num << " local  solved: " << cur_queue_size - router_txn_num;
  }



  void run_transaction(ExecutorStatus status, 
                       std::deque<std::unique_ptr<TransactionType>>* cur_transactions_queue,
                       std::atomic<uint32_t>& async_message_num,
                       bool naive_router = false,
                       bool reset_time = false) {
    /**
     * @brief 
     * @note modified by truth 22-01-24
     *       
    */
    ProtocolType* protocol;

    if (status == ExecutorStatus::C_PHASE) {
      protocol = c_protocol;
      partitioner = l_partitioner.get();
    } else if (status == ExecutorStatus::S_PHASE) {
      protocol = s_protocol;
      partitioner = s_partitioner.get();
    } else {
      CHECK(false);
    }

    int time1 = 0;
    int time2 = 0;
    int time3 = 0;
    int time4 = 0;

    uint64_t last_seed = 0;

    auto i = 0u;
    size_t cur_queue_size = cur_transactions_queue->size();
    int router_txn_num = 0;

    // while(!cur_transactions_queue->empty()){ // 为什么不能这样？ 不是太懂
    for (auto i = 0u; i < cur_queue_size; i++) {
      if(cur_transactions_queue->empty()){
        break;
      }

      transaction =
              std::move(cur_transactions_queue->front());
      transaction->startTime = std::chrono::steady_clock::now();

      if(false){ // naive_router && router_to_other_node(status == ExecutorStatus::C_PHASE)){
        // pass
        router_txn_num++;
      } else {
        bool retry_transaction = false;

        do {
          ////  // LOG(INFO) << "LionExecutor: "<< id << " " << "process_request" << i;
          process_request();
          last_seed = random.get_seed();

          if (retry_transaction) {
            transaction->reset();
          } else {
            setupHandlers(*transaction, *protocol);
          }

          auto now = std::chrono::steady_clock::now();

          // auto result = transaction->execute(id);
          transaction->prepare_read_execute(id);

          time4 += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
          now = std::chrono::steady_clock::now();
          

          time1 += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
          now = std::chrono::steady_clock::now();
          
          auto result = transaction->read_execute(id, ReadMethods::REMOTE_READ_WITH_TRANSFER);
          if(result != TransactionResult::READY_TO_COMMIT){
            retry_transaction = false;
            protocol->abort(*transaction, messages);
            n_abort_no_retry.fetch_add(1);
            continue;
          } else {
            result = transaction->prepare_update_execute(id);
          }
          // auto result = transaction->execute(id);
          time2 += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
          now = std::chrono::steady_clock::now();

          if (result == TransactionResult::READY_TO_COMMIT) {
            ////  // LOG(INFO) << "LionExecutor: "<< id << " " << "commit" << i;

            bool commit =
                protocol->commit(*transaction, messages, async_message_num); // sync_messages, async_messages, record_messages, 
                                // );
            
            time3 += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
            now = std::chrono::steady_clock::now();

            n_network_size.fetch_add(transaction->network_size);
            if (commit) {
              n_commit.fetch_add(1);
              retry_transaction = false;
              
              // if(reset_time == true){
              //   auto latency =
              //   std::chrono::duration_cast<std::chrono::microseconds>(
              //       std::chrono::steady_clock::now() - transaction->startTime)
              //       .count();
              //   percentile.add(latency);
              // } else {
              q.push(std::move(transaction));
              // }
              
            } else {
              if(transaction->abort_lock && transaction->abort_read_validation){
                // 
                n_abort_read_validation.fetch_add(1);
                retry_transaction = false;
              } else {
                if (transaction->abort_lock) {
                  n_abort_lock.fetch_add(1);
                } else {
                  DCHECK(transaction->abort_read_validation);
                  n_abort_read_validation.fetch_add(1);
                }
                random.set_seed(last_seed);
                retry_transaction = true;
              }
              protocol->abort(*transaction, messages);
            }
          } else {
            n_abort_no_retry.fetch_add(1);
            protocol->abort(*transaction, messages);
          }
        } while (retry_transaction);
      }

      cur_transactions_queue->pop_front();

      if (i % context.batch_flush == 0) {
        flush_messages(messages); 
        flush_async_messages(); 
        flush_sync_messages();
        flush_record_messages();
        
      }
    }
    flush_messages(messages); 
    flush_async_messages();
    flush_record_messages();
    flush_sync_messages();
    VLOG_IF(DEBUG_V4, id == 0) << "prepare: " << time4 / 1000 << "  execute: " << time2 / 1000 << "  commit: " << time3 / 1000 << "  router : " << time1 / 1000; 
    ////  // LOG(INFO) << "router_txn_num: " << router_txn_num << "  local solved: " << cur_queue_size - router_txn_num;
  }


  void run_local_transaction(ExecutorStatus status, 
                             std::deque<std::unique_ptr<TransactionType>>* cur_transactions_queue, 
                             std::atomic<uint32_t>& async_message_num, 
                             size_t run_batch_size) {
    /**
     * @brief try to run local transcations in normal C_Phase
     * @note modified by truth 22-03-25    
    */

    ProtocolType* protocol = c_protocol;// (db, phase_context, *cur_partitioner, id);
    partitioner = l_partitioner.get();

    uint64_t last_seed = 0;
    
    for (auto i = 0u; i < run_batch_size; i++) {
      if(cur_transactions_queue->empty()){
        break;
      }
      bool retry_transaction = false;


      bool is_cross_node = false;
      TransactionResult result;

      do {
        ////  // LOG(INFO) << "LionExecutor: "<< id << " " << "process_request" << i;
        process_request();
        last_seed = random.get_seed();

        if (retry_transaction) {
          transaction->reset();
        } else {          
          transaction =
              std::move(cur_transactions_queue->front());

          setupHandlers(*transaction, *protocol);
        }
        
        transaction->prepare_read_execute(id);
                
        result = transaction->read_execute(id, ReadMethods::LOCAL_READ);
        if(result != TransactionResult::READY_TO_COMMIT){
          if(result == TransactionResult::ABORT){
                retry_transaction = true;
                continue;
          }
        } else {
          result = transaction->prepare_update_execute(id);
        }

        if (result == TransactionResult::READY_TO_COMMIT) {
          bool commit = protocol->commit(*transaction, messages, async_message_num);
          n_network_size.fetch_add(transaction->network_size);
          if (commit) {
            n_commit.fetch_add(1);
            retry_transaction = false;
            q.push(std::move(transaction)); 
          } else {
            // release all router lock and retry
            // protocol->router_abort(*transaction);
            if (transaction->abort_lock) {
              n_abort_lock.fetch_add(1);
            } else {
              DCHECK(transaction->abort_read_validation);
              n_abort_read_validation.fetch_add(1);
            }
            random.set_seed(last_seed);
            retry_transaction = true;
          }
        } else {
          // release all router lock and abort
          n_abort_no_retry.fetch_add(1);
          // protocol->router_abort(*transaction);
        }
      } while (retry_transaction);

      if(is_cross_node || result != TransactionResult::READY_TO_COMMIT){
        // wait for later king-C-Phase 
        c_transactions_queue.push_back(std::move(transaction));
      }

      cur_transactions_queue->pop_front();
    }

  }

  void onExit() override {
   LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50)
              << " us (50%) " << percentile.nth(75) << " us (75%) "
              << percentile.nth(95) << " us (95%) " << percentile.nth(99)
              << " us (99%).";

    if (logger != nullptr) {
      logger->close();
    }
  }

  void push_message(Message *message) override { 

    // message will only be of type signal, COUNT
    // MessagePiece messagePiece = *(message->begin());

    // auto message_type =
    // static_cast<int>(messagePiece.get_message_type());

      // sync_queue.push(message);
      ////  // LOG(INFO) << "sync_queue: " << sync_queue.read_available(); 
      for (auto it = message->begin(); it != message->end(); it++) {
        auto messagePiece = *it;
        auto message_type = messagePiece.get_message_type();
        //!TODO replica 
        if(message_type == static_cast<int>(LionMessage::REPLICATION_RESPONSE)){
          auto message_length = messagePiece.get_message_length();
          
          ////  // LOG(INFO) << "recv : " << ++total_async;
          // async_message_num.fetch_sub(1);
          async_message_respond_num.fetch_add(1);
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

protected:
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

        ////  // LOG(INFO) << "GET MESSAGE TYPE: " << type;
        messageHandlers[type](messagePiece,
                              *sync_messages[message->get_source_node_id()], 
                              db, context, partitioner,
                              transaction.get(), 
                              &router_transactions_queue);

        if (logger) {
          logger->write(messagePiece.toStringPiece().data(),
                        messagePiece.get_message_length());
        }
      }

      size += message->get_message_count();
      flush_sync_messages();
    }
    return size;
  }

  void setupHandlers(TransactionType &txn, ProtocolType &protocol) {
    txn.readRequestHandler =
        [this, &txn, &protocol](std::size_t table_id, std::size_t partition_id,
                     uint32_t key_offset, const void *key, void *value,
                     bool local_index_read, bool &success) -> uint64_t {
      bool local_read = false;

      auto &readKey = txn.readSet[key_offset];
      auto coordinatorID = this->partitioner->master_coordinator(table_id, partition_id, key);
      size_t coordinator_secondaryID = context.coordinator_num + 1;
      if(readKey.get_write_lock_bit()){
        coordinator_secondaryID = this->partitioner->secondary_coordinator(table_id, partition_id, key);
      }

      if(coordinatorID == context.coordinator_num || 
         coordinator_secondaryID == context.coordinator_num || 
         coordinatorID == coordinator_secondaryID){
        success = false;
        return 0;
      }

      readKey.set_dynamic_coordinator_id(coordinatorID);
      readKey.set_dynamic_secondary_coordinator_id(coordinator_secondaryID);

      // SiloRWKey *writeKey = txn.get_write_key(readKey.get_key());
      // if(writeKey != nullptr){
      //   writeKey->set_dynamic_coordinator_id(coordinatorID);
      //   writeKey->set_dynamic_secondary_coordinator_id(coordinator_secondaryID);
      // }

      bool remaster = false;

      ITable *table = this->db.find_table(table_id, partition_id);
      if (coordinatorID == coordinator_id) {
        std::atomic<uint64_t> &tid = table->search_metadata(key, success);
        if(success == false){
          return 0;
        }
        // 赶快本地lock
        if(readKey.get_write_lock_bit()){
          TwoPLHelper::write_lock(tid, success);
          VLOG(DEBUG_V14) << "LOCK-write " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_dynamic_secondary_coordinator_id();
        } else {
          TwoPLHelper::read_lock(tid, success);
          VLOG(DEBUG_V14) << "LOCK-read " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_dynamic_secondary_coordinator_id();
        }
        if(success){
          readKey.set_read_respond_bit();
        } else {
          return 0;
        }
        local_read = true;
      } else {
        // if(table_id == 2){
        //   LOG(INFO) << "nani?";
        //   txn.readSet[key_offset].get_dynamic_coordinator_id();
        // }
        // FUCK 此处获得的table partition并不是我们需要从对面读取的partition
        remaster = table->contains(key); // current coordniator
        
        VLOG(DEBUG_V8) << table_id << " ASK " << coordinatorID << " " << *(int*)key << " " << remaster;
      }

      if (local_index_read || local_read) {
        auto ret = protocol.search(table_id, partition_id, key, value, success);
        return ret;
      } else {
        
        for(size_t i = 0; i < context.coordinator_num; i ++ ){
          if(i == coordinator_id){
            continue;
          }
          if(i == coordinatorID){
            txn.network_size += MessageFactoryType::new_search_message(
                *(this->messages[i]), *table, key, key_offset, remaster);
          } else {
            txn.network_size += MessageFactoryType::new_search_router_only_message(
                *(this->messages[i]), *table, key, key_offset);
          }            
          txn.pendingResponses++;
        }
        txn.distributed_transaction = true;
        return 0;
      }
    };

    // txn.localReadRequestHandler =
    //     [this, &txn, &protocol](std::size_t table_id, std::size_t partition_id,
    //                  uint32_t key_offset, const void *key, void *value,
    //                  bool local_index_read) -> uint64_t {
    //   bool local_read = false;


    //   auto coordinatorID = txn.readSet[key_offset].get_dynamic_coordinator_id();

    //   if (coordinatorID == coordinator_id
    //        ) {
    //     local_read = true;
    //   }

    //   if (local_index_read || local_read) {
    //     return protocol.search(table_id, partition_id, key, value);
    //   } else {
    //     return INT_MAX;
    //   }
    // };

    // txn.readOnlyRequestHandler =
    //     [this, &txn, &protocol](std::size_t table_id, std::size_t partition_id,
    //                  uint32_t key_offset, const void *key, void *value,
    //                  bool local_index_read) -> uint64_t {
    //   bool local_read = false;
      
    //   auto coordinatorID = txn.readSet[key_offset].get_dynamic_coordinator_id();

    //   if (coordinatorID == coordinator_id // ||
    //       // (this->partitioner->is_partition_replicated_on(
    //       //      partition_id, this->coordinator_id) &&
    //       //  this->context.read_on_replica)
    //        ) {
    //     local_read = true;
    //   }

    //   if (local_index_read || local_read) {
    //     return protocol.search(table_id, partition_id, key, value);
    //   } else {
    //     ITable *table = this->db.find_table(table_id, partition_id);
    //     // auto coordinatorID =
    //     //     txn.partitioner.master_coordinator(table_id, partition_id, key);
        
    //     txn.network_size += MessageFactoryType::new_search_read_only_message(
    //             *(this->messages[coordinatorID]), *table, key, key_offset);
    //     txn.pendingResponses++;
    //     txn.distributed_transaction = true;
    //     return 0;
    //   }
    // };

    txn.remote_request_handler = [this]() { return this->process_request(); };
    txn.message_flusher = [this]() { this->flush_messages(messages); };
  }

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

  void flush_sync_messages() { flush_messages(sync_messages); }

  void flush_async_messages() { flush_messages(async_messages); }

  void flush_record_messages() { flush_messages(record_messages); }

  void init_message(Message *message, std::size_t dest_node_id) {
    message->set_source_node_id(coordinator_id);
    message->set_dest_node_id(dest_node_id);
    message->set_worker_id(id);
  }

protected:
  DatabaseType &db;
  const ContextType &context;
  uint32_t &batch_size;
  std::unique_ptr<Partitioner> l_partitioner, s_partitioner;
  Partitioner* partitioner;
  RandomType random;
  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> async_message_num;
  std::atomic<uint32_t> async_message_respond_num;

  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  // LockfreeQueueMulti<data_pack*, 8064 > &data_pack_queue;

  HashMap<9916, std::string, int> &data_pack_map;
  // std::unordered_map<std::string, int> &data_pack_map;
  

  std::unique_ptr<Delay> delay;
  std::unique_ptr<BufferedFileWriter> logger;
  Percentile<uint64_t> percentile;
  std::unique_ptr<TransactionType> transaction;

  std::vector<std::unique_ptr<Message>> messages;
  // transaction only commit in a single group
  std::queue<std::unique_ptr<TransactionType>> q;
  std::vector<std::unique_ptr<Message>> sync_messages, async_messages, record_messages;
  std::vector<std::function<void(MessagePiece, Message &, DatabaseType &, const ContextType &, Partitioner *, // add partitioner
                                 TransactionType *, 
                                 std::deque<simpleTransaction>*)>>
      messageHandlers;
  LockfreeQueue<Message *> in_queue, out_queue, 
                           sync_queue; // for value sync when phase switching occurs

  // std::unique_ptr<WorkloadType> s_workload, c_workload;

  ContextType s_context, c_context;
  ProtocolType* s_protocol, *c_protocol;

  std::deque<std::unique_ptr<TransactionType>> s_transactions_queue, c_transactions_queue,
                                               c_single_transactions_queue,
                                               r_transactions_queue, r_single_transactions_queue;
  // 
  double planning_ratio;
  int my_batch_size;

  std::deque<std::unique_ptr<TransactionType>> execution_plan_txn_queue;
  std::deque<ExecutionStep> execution_plan;

  std::deque<simpleTransaction> router_transactions_queue;

  std::vector<std::pair<size_t, size_t> > res; // record tnx

  // for test
  std::ofstream outfile_excel;

};
} // namespace star