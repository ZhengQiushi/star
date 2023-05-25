//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include "core/Partitioner.h"

#include "common/BufferedFileWriter.h"
#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Lion/Lion.h"
#include "protocol/Lion/LionQueryNum.h"

#include <chrono>
#include <deque>

namespace star {

template <class Workload> class LionExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;
  using TransactionType = LionTransaction;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = Lion<DatabaseType>;

  using MessageType = LionMessage;
  using MessageFactoryType = LionMessageFactory;
  using MessageHandlerType = LionMessageHandler<DatabaseType>;

  int pin_thread_id_ = 8;

  LionExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
               const ContextType &context, uint32_t &batch_size,
               std::atomic<uint32_t> &worker_status,
               std::atomic<uint32_t> &n_complete_workers,
               std::atomic<uint32_t> &n_started_workers, 
               std::atomic<uint32_t> &skip_s_phase, 
               std::atomic<uint32_t> &transactions_prepared, 
               TransactionMeta<WorkloadType>& txn_meta
              //  std::vector<std::unique_ptr<TransactionType>> &txn_meta.s_transactions_queue, 
              //  std::vector<std::unique_ptr<TransactionType>> &txn_meta.c_transactions_queue,
              //  std::mutex &txn_meta.s_l,
              //  std::mutex &txn_meta.c_l, 
              //  ShareQueue<simpleTransaction> &txn_meta.router_transactions_queue,
              //  ShareQueue<int> &txn_meta.s_txn_id_queue,
              //  ShareQueue<int> &txn_meta.c_txn_id_queue,
              //  std::vector<StorageType> &txn_meta.storages
               ) // ,
               // HashMap<9916, std::string, int> &data_pack_map)
      : Worker(coordinator_id, id), db(db), context(context),
        batch_size(batch_size),
        l_partitioner(std::make_unique<LionDynamicPartitioner<Workload> >(
            coordinator_id, context.coordinator_num, db)),
        s_partitioner(std::make_unique<LionStaticPartitioner<Workload> >(
            coordinator_id, context.coordinator_num, db)),
        random(reinterpret_cast<uint64_t>(this)), worker_status(worker_status),
        n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        skip_s_phase(skip_s_phase),
        transactions_prepared(transactions_prepared),
        txn_meta(txn_meta),
        // txn_meta.s_transactions_queue(txn_meta.s_transactions_queue),
        // txn_meta.c_transactions_queue(txn_meta.c_transactions_queue),
        // txn_meta.s_l(txn_meta.s_l),
        // txn_meta.c_l(txn_meta.c_l),
        // txn_meta.router_transactions_queue(txn_meta.router_transactions_queue),
        // txn_meta.s_txn_id_queue(txn_meta.s_txn_id_queue),
        // txn_meta.c_txn_id_queue(txn_meta.c_txn_id_queue),
        // txn_meta.storages(txn_meta.storages),
        // data_pack_map(data_pack_map),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)) {

    for (auto i = 0u; i <= context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);

      sync_messages.emplace_back(std::make_unique<Message>());
      init_message(sync_messages[i].get(), i);

      async_messages.emplace_back(std::make_unique<Message>());
      init_message(async_messages[i].get(), i);

      // record_messages.emplace_back(std::make_unique<Message>());
      // init_message(record_messages[i].get(), i);

      // messages_mutex.emplace_back(std::make_unique<std::mutex>());

      // 
      metis_messages.emplace_back(std::make_unique<Message>());
      init_message(metis_messages[i].get(), i);

      metis_sync_messages.emplace_back(std::make_unique<Message>());
      init_message(metis_sync_messages[i].get(), i);

      // metis_messages_mutex.emplace_back(std::make_unique<std::mutex>());
    }

    // storages_self.resize(context.batch_size);

    messageHandlers = MessageHandlerType::get_message_handlers();
    controlMessageHandlers = ControlMessageHandler<DatabaseType>::get_message_handlers();

    if (context.log_path != "") {
      std::string filename =
          context.log_path + "_" + std::to_string(id) + ".txt";
      logger = std::make_unique<BufferedFileWriter>(filename.c_str());
    }

    s_context = context.get_single_partition_context();
    c_context = context.get_cross_partition_context();

    s_protocol = new ProtocolType(db, s_context, *s_partitioner, id);
    c_protocol = new ProtocolType(db, c_context, *l_partitioner, id);

    c_workload = new WorkloadType (coordinator_id, worker_status, db, random, *l_partitioner.get(), start_time);
    s_workload = new WorkloadType (coordinator_id, worker_status, db, random, *s_partitioner.get(), start_time);

    partitioner = l_partitioner.get(); // nullptr;
    metis_partitioner = l_partitioner.get(); // nullptr;

    // sync responds that need to be received 
    async_message_num.store(0);
    async_message_respond_num.store(0);

    metis_async_message_num.store(0);
    metis_async_message_respond_num.store(0);

    router_transaction_done.store(0);
    router_transactions_send.store(0);

    remaster_delay_transactions = 0;

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

  void async_fence(){
    while(async_pend_num.load() != async_respond_num.load()){
      int a = async_pend_num.load();
      int b = async_respond_num.load();

      process_request();
      std::this_thread::yield();
    }
    async_pend_num.store(0);
    async_respond_num.store(0);
  }

  void replication_fence(ExecutorStatus status){
    while(async_message_num.load() != async_message_respond_num.load()){
      int a = async_message_num.load();
      int b = async_message_respond_num.load();

      process_request();
      std::this_thread::yield();
    }
    async_message_num.store(0);
    async_message_respond_num.store(0);
  }

  void router_fence(){

    while(router_transaction_done.load() != router_transactions_send.load()){
      process_request(); 
    }
    router_transaction_done.store(0);
    router_transactions_send.store(0);
  }

  void unpack_route_transaction(){
    // int size_ = txn_meta.router_transactions_queue.size();
    
    while(true){
      bool success = false;
      simpleTransaction simple_txn = 
        txn_meta.router_transactions_queue.pop_no_wait(success);
      if(!success) break;
      n_network_size.fetch_add(simple_txn.size);
      
      uint32_t txn_id;
      std::unique_ptr<TransactionType> null_txn(nullptr);
      if(!simple_txn.is_distributed && !simple_txn.is_transmit_request){
        {
          std::lock_guard<std::mutex> l(txn_meta.s_l);
          txn_id = txn_meta.s_transactions_queue.size();
          txn_meta.s_transactions_queue.push_back(std::move(null_txn));
          txn_meta.s_txn_id_queue.push_no_wait(txn_id);
        }
        auto p = s_workload->unpack_transaction(context, 0, storage, simple_txn);
        txn_meta.s_transactions_queue[txn_id] = std::move(p);
      } else {
        {
          std::lock_guard<std::mutex> l(txn_meta.c_l);
          txn_id = txn_meta.c_transactions_queue.size();
          if(txn_id >= txn_meta.storages.size()){
            DCHECK(false);
          }
          txn_meta.c_transactions_queue.push_back(std::move(null_txn));
          txn_meta.c_txn_id_queue.push_no_wait(txn_id);
        }
        auto p = c_workload->unpack_transaction(context, 0, txn_meta.storages[txn_id], simple_txn);
        
        if(simple_txn.is_transmit_request){
          DCHECK(false);
        } else {
          if(simple_txn.is_real_distributed){
            cur_real_distributed_cnt += 1;
            p->distributed_transaction = true;
            if(cur_real_distributed_cnt < 10){
              LOG(INFO) << " test if abort?? " << simple_txn.keys[0] << " " << simple_txn.keys[1];
            }
          } 
          p->id = txn_id;
        }
        txn_meta.c_transactions_queue[txn_id] = std::move(p);
      }
    }
  }
  
  bool is_router_stopped(int& router_recv_txn_num){
    bool ret = false;
    size_t num = 1; // context.coordinator_num
    if(router_stop_queue.size() < num){
      ret = false;
    } else {
      //
      int i = num; // context.coordinator_num;
      while(i > 0){
        i --;
        DCHECK(router_stop_queue.size() > 0);
        int recv_txn_num = router_stop_queue.front();
        router_stop_queue.pop_front();
        router_recv_txn_num += recv_txn_num;
        VLOG(DEBUG_V8) << " RECV : " << recv_txn_num;
      }
      ret = true;
    }
    return ret;
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

  
  void start() override {

    LOG(INFO) << "Executor " << id << " starts.";

    // C-Phase to S-Phase, to C-phase ...

    int times = 0;
    ExecutorStatus status;

    for (;;) {
      auto begin = std::chrono::steady_clock::now();
      auto cur_time = std::chrono::duration_cast<std::chrono::seconds>(
                     std::chrono::steady_clock::now() - start_time)
                     .count();
                     
      LOG(INFO) << "new batch processing ";
      times ++ ;

      do {
        status = static_cast<ExecutorStatus>(worker_status.load());
        process_request();
        if (status == ExecutorStatus::EXIT) {
          // commit transaction in s_phase;
          commit_transactions();
          LOG(INFO) << "Executor " << id << " exits.";
          VLOG_IF(DEBUG_V, id==0) << "TIMES : " << times; 
          // if(metis_transaction != nullptr){
          //   metis_transaction->status = ExecutorStatus::EXIT;
          // }

          LOG(INFO) << " router : " << router_percentile.nth(50) << " " <<
          router_percentile.nth(80) << " " << router_percentile.nth(95) << " " << 
          router_percentile.nth(99);

          LOG(INFO) << " analysis : " << analyze_percentile.nth(50) << " " <<
          analyze_percentile.nth(80) << " " << analyze_percentile.nth(95) << " " << 
          analyze_percentile.nth(99);

          LOG(INFO) << " execution : " << execute_latency.nth(50) << " " <<
          execute_latency.nth(80) << " " << execute_latency.nth(95) << " " << 
          execute_latency.nth(99);
          return;

          // for(auto& t: transmiter){
          //   t.join();
          // }
          return;
        }
      } while (status != ExecutorStatus::C_PHASE);

      // commit transaction in s_phase;
      commit_transactions();

      // c_phase
      VLOG_IF(DEBUG_V, id==0) << "wait c_phase "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";
      
      

      int router_recv_txn_num = 0;
      // 准备transaction
      // if (id == 0) {
        
        while(!is_router_stopped(router_recv_txn_num)){
          process_request();
          std::this_thread::sleep_for(std::chrono::microseconds(5));
        }

        auto router_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - begin)
                      .count();

        LOG(INFO) << "prepare_transactions_to_run "
                << router_time
                << " milliseconds.";

        if(cur_time > 10)
          router_percentile.add(router_time);

        cur_real_distributed_cnt = 0;
        unpack_route_transaction(); // 

        VLOG_IF(DEBUG_V, id==0) << txn_meta.c_transactions_queue.size() << " "  << txn_meta.s_transactions_queue.size() << " OMG : " << cur_real_distributed_cnt;

        transactions_prepared.fetch_add(1);
      // // }
      LOG(INFO) << "[C-PHASE] do remaster "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";

      while(transactions_prepared.load() < context.worker_num){
        std::this_thread::yield();
        process_request();
      }

      n_started_workers.fetch_add(1);

      if(cur_real_distributed_cnt > 0){
        do_remaster_transaction(ExecutorStatus::C_PHASE, txn_meta.c_transactions_queue,async_message_num);
        async_fence();
      }

      VLOG_IF(DEBUG_V, id==0) << "[C-PHASE] do remaster "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";

      run_transaction(ExecutorStatus::C_PHASE, 
      txn_meta.c_transactions_queue,
      txn_meta.c_txn_id_queue,
      async_message_num);
    
      n_complete_workers.fetch_add(1);

      VLOG_IF(DEBUG_V, id==0) << "[C-PHASE] C_phase - local "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";
      // now = std::chrono::steady_clock::now();
      // wait to s_phase
      
      replication_fence(ExecutorStatus::C_PHASE);

      // commit transaction in c_phase;
      // commit_transactions();
      VLOG_IF(DEBUG_V, id==0) << "[C-PHASE] C_phase router done "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";
      // now = std::chrono::steady_clock::now();
        VLOG_IF(DEBUG_V, id==0) << "[C-PHASE] worker " << id << " wait to s_phase";
        while (static_cast<ExecutorStatus>(worker_status.load()) !=
              ExecutorStatus::S_PHASE) {
          process_request(); 
        }

      VLOG_IF(DEBUG_V, id==0) << "S_phase enter"
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";

      if(id == 0){
        transactions_prepared.store(0);
      }

      if(skip_s_phase.load() == false){
        // s_phase
        VLOG_IF(DEBUG_V, id==0) << "[S-PHASE] wait for s-phase "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - begin)
                      .count()
                << " milliseconds.";
        // now = std::chrono::steady_clock::now();

        n_started_workers.fetch_add(1);
        VLOG_IF(DEBUG_V, id==0) << "[S-PHASE] worker " << id << " ready to run_transaction";

        // r_size = txn_meta.s_transactions_queue.size();
        // LOG(INFO) << "txn_meta.s_transactions_queue.size() : " <<  r_size;
        run_transaction(ExecutorStatus::S_PHASE, 
        txn_meta.s_transactions_queue, 
        txn_meta.s_txn_id_queue,
        async_message_num);
        
        // VLOG_IF(DEBUG_V, id==0) << "worker " << id << " ready to replication_fence";

        VLOG_IF(DEBUG_V, id==0) << "[S-PHASE] done "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - begin)
                      .count()
                << " milliseconds.";
        // now = std::chrono::steady_clock::now();
        
        replication_fence(ExecutorStatus::S_PHASE);
        n_complete_workers.fetch_add(1);
        VLOG_IF(DEBUG_V, id==0) << "[S-PHASE] fence "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - begin)
                      .count()
                << " milliseconds.";
        // now = std::chrono::steady_clock::now();

        // once all workers are stop, we need to process the replication
        // requests

        while (static_cast<ExecutorStatus>(worker_status.load()) ==
              ExecutorStatus::S_PHASE) {
          process_request();
        }

        VLOG_IF(DEBUG_V, id==0) << "wait back "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - begin)
                      .count()
                << " milliseconds.";
        // now = std::chrono::steady_clock::now();


        // n_complete_workers has been cleared
        process_request();
        n_complete_workers.fetch_add(1);

      } else {
        VLOG_IF(DEBUG_V, id==0) << "skip s phase";
        process_request();
        n_complete_workers.fetch_add(1);
      }
      if(id == 0){
        txn_meta.s_transactions_queue.clear();
        txn_meta.c_transactions_queue.clear();
      }
        auto execution_schedule_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - begin)
                      .count();

        if(cur_time > 10)
          execute_latency.add(execution_schedule_time);

      VLOG_IF(DEBUG_V, id==0) << "whole batch "
              << execution_schedule_time
              << " milliseconds.";
    }
    VLOG_IF(DEBUG_V, id==0) << "TIMES : " << times; 


  }

  void commit_transactions() {
    while (!q.empty()) {
      auto &ptr = q.front();
      auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - ptr->startTime)
                         .count();
      percentile.add(latency);
      q.pop();
    }
  }

  std::size_t get_partition_id(ExecutorStatus status) {

    std::size_t partition_id;

    if (status == ExecutorStatus::C_PHASE) {
      // 从当前线程管的分区里随机选一个
      // CHECK(coordinator_id == 0);
      // CHECK(context.partition_num % context.worker_num == 0);
      auto partition_num_per_thread =
          context.partition_num / context.worker_num;
      partition_id = id * partition_num_per_thread +
                     random.uniform_dist(0, partition_num_per_thread - 1);

    } else if (status == ExecutorStatus::S_PHASE) {
      partition_id = id * context.coordinator_num + coordinator_id;
    } else {
      CHECK(false);
    }

    return partition_id;
  }
  

  void run_transaction(ExecutorStatus status, 
                       std::vector<std::unique_ptr<TransactionType>>& cur_trans,
                       ShareQueue<int>& txn_id_queue,
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

    auto begin = std::chrono::steady_clock::now();
    // int time1 = 0;
    int time_prepare_read = 0;  
    int time_before_prepare_set = 0;
    int time_before_prepare_read = 0;  
    int time_before_prepare_request = 0;
    int time_read_remote = 0;
    int time3 = 0;
    int time4 = 0;

    Percentile<int64_t> txn_percentile;

    uint64_t last_seed = 0;

    auto count = 0u;
    size_t cur_queue_size = cur_trans.size();
    int router_txn_num = 0;

    std::vector<int> why(20, 0);

    // while(!cur_trans->empty()){ // 为什么不能这样？ 不是太懂
    // for (auto i = id; i < cur_queue_size; i += context.worker_num) {
    auto i = 0l;
    for(;;) {
      bool success = false;
      if(status == ExecutorStatus::C_PHASE){
        i = txn_id_queue.pop_no_wait(success);
        if(!success){
          i = sub_c_txn_id_queue.pop_no_wait(success);
        } 
      } else {
        i = txn_id_queue.pop_no_wait(success);
      }
      if(!success){
        break;
      }
      if(i >= cur_trans.size() || cur_trans[i].get() == nullptr){
        // DCHECK(false) << i << " " << cur_trans.size();
        continue;
      }

      auto now = std::chrono::steady_clock::now();
      count += 1;
      auto txnStartTime = cur_trans[i]->startTime = std::chrono::steady_clock::now();

      if(false){ // naive_router && router_to_other_node(status == ExecutorStatus::C_PHASE)){
        // pass
        router_txn_num++;
      } else {
        bool retry_transaction = false;

        do {
          ////  // LOG(INFO) << "LionExecutor: "<< id << " " << "process_request" << i;
          time_before_prepare_request += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();

          process_request();
          ////
          last_seed = random.get_seed();

          time_before_prepare_set += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();

          if (retry_transaction) {
            cur_trans[i]->reset();
          } else {
            setupHandlers(*cur_trans[i], *protocol);
          }

          time_before_prepare_read += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
          
          cur_trans[i]->prepare_read_execute(id);

          time_prepare_read += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
          now = std::chrono::steady_clock::now();
          
          auto result = cur_trans[i]->read_execute(id, ReadMethods::REMOTE_READ_WITH_TRANSFER);
          
          if(result != TransactionResult::READY_TO_COMMIT){
            retry_transaction = false;
            protocol->abort(*cur_trans[i], messages);
            n_abort_no_retry.fetch_add(1);
            continue;
          } else {
            result = cur_trans[i]->prepare_update_execute(id);
          }

          time_read_remote += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
          now = std::chrono::steady_clock::now();

          if (result == TransactionResult::READY_TO_COMMIT) {
            bool commit = protocol->commit(*cur_trans[i], messages, async_message_num, false);
            
            n_network_size.fetch_add(cur_trans[i]->network_size);
            if (commit) {
              n_commit.fetch_add(1);
              retry_transaction = false;
              
              n_migrate.fetch_add(cur_trans[i]->migrate_cnt);
              n_remaster.fetch_add(cur_trans[i]->remaster_cnt);

              q.push(std::move(cur_trans[i]));
            } else {
              if(cur_trans[i]->abort_lock && cur_trans[i]->abort_read_validation){
                // 
                n_abort_read_validation.fetch_add(1);
                retry_transaction = false;
              } else {
                if (cur_trans[i]->abort_lock) {
                  n_abort_lock.fetch_add(1);
                } else {
                  DCHECK(cur_trans[i]->abort_read_validation);
                  n_abort_read_validation.fetch_add(1);
                }
                random.set_seed(last_seed);
                retry_transaction = true;
              }
              protocol->abort(*cur_trans[i], messages);
            }
          } else {
            n_abort_no_retry.fetch_add(1);
            protocol->abort(*cur_trans[i], messages);
          }
          time3 += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
          now = std::chrono::steady_clock::now();

        } while (retry_transaction);
      }

      now = std::chrono::steady_clock::now();
      flush_messages(messages); 
      flush_async_messages(); 

      if (i % context.batch_flush == 0) {
        flush_sync_messages();
      }
      
      time4 += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
      now = std::chrono::steady_clock::now();

      txn_percentile.add(
              std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - txnStartTime)
              .count()
        
      );
    }
    flush_messages(messages); 
    flush_async_messages();
    // flush_record_messages();
    flush_sync_messages();

    auto total_sec = std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count() * 1.0;
    if(count > 0){
      LOG(INFO) << total_sec / 1000 / 1000 << " s, " << total_sec / count << " per/micros."
                << txn_percentile.nth(10) << " " 
                << txn_percentile.nth(50) << " "
                << txn_percentile.nth(80) << " "
                << txn_percentile.nth(90) << " "
                << txn_percentile.nth(95);

    
      VLOG(DEBUG_V4)  << time_read_remote << " " << count 
      << " pre: "     << time_before_prepare_request / count  
      << " set: "     << time_before_prepare_set / count 
      << " gap: "     << time_before_prepare_read / count
      << " prepare: " << time_prepare_read / count 
      << " execute: " << time_read_remote / count 
      << " commit: "  << time3 / count 
      << " send: "    << time4 / count
      << "\n "
      << " : "        << why[11] / count
      << " : "        << why[12] / count
      << " : "        << why[13] / count; // << "  router : " << time1 / cur_queue_size; 
      // LOG(INFO) << "remaster_delay_transactions: " << remaster_delay_transactions;
      // remaster_delay_transactions = 0;
    }
      

    ////  // LOG(INFO) << "router_txn_num: " << router_txn_num << "  local solved: " << cur_queue_size - router_txn_num;
  }

  void do_remaster_transaction(ExecutorStatus status, 
                       std::vector<std::unique_ptr<TransactionType>>& cur_trans,
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

    auto begin = std::chrono::steady_clock::now();
    // int time1 = 0;
    int time_prepare_read = 0;    
    int time_read_remote = 0;
    int time3 = 0;


    // uint64_t last_seed = 0;

    auto i = 0u;
    size_t cur_queue_size = cur_trans.size();
    int router_txn_num = 0;


    // for(;;) {
    for (auto x = id; x < cur_queue_size; x += context.worker_num) {
      bool success = false;
      auto i = txn_meta.c_txn_id_queue.pop_no_wait(success);
      if(!success){
        break;
      }
      if(i >= cur_trans.size() || cur_trans[i].get() == nullptr){
        // DCHECK(false) << i << " " << cur_trans.size();
        continue;
      }
      
      if(cur_trans[i]->distributed_transaction == false){
        txn_meta.c_txn_id_queue.push_no_wait(i);
        continue;
      }
      sub_c_txn_id_queue.push_no_wait(i);

      cur_trans[i]->startTime = std::chrono::steady_clock::now();
      bool retry_transaction = false;

      do {
        ////  // LOG(INFO) << "LionExecutor: "<< id << " " << "process_request" << i;
        process_request();
        // last_seed = random.get_seed();

        if (retry_transaction) {
          cur_trans[i]->reset();
        } else {
          setupHandlers(*cur_trans[i], *protocol);
        }

        auto now = std::chrono::steady_clock::now();

        cur_trans[i]->prepare_read_execute(id);
        time_prepare_read += std::chrono::duration_cast<std::chrono::microseconds>(
                                                              std::chrono::steady_clock::now() - now)
            .count();
        now = std::chrono::steady_clock::now();
        
        auto result = cur_trans[i]->read_execute(id, ReadMethods::REMASTER_ONLY);
        
        if(result != TransactionResult::READY_TO_COMMIT){
          retry_transaction = false;
          protocol->abort(*cur_trans[i], messages);
          n_abort_no_retry.fetch_add(1);
        }

        time_read_remote += std::chrono::duration_cast<std::chrono::microseconds>(
                                                              std::chrono::steady_clock::now() - now)
            .count();
        now = std::chrono::steady_clock::now();

      } while (retry_transaction);

      cur_trans[i]->reset();
      flush_messages(messages); 

      if (i % context.batch_flush == 0) {
        flush_async_messages(); 
        flush_sync_messages();
      }
    }
    flush_messages(messages); 
    flush_async_messages();
    flush_sync_messages();

    auto total_sec = std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count() * 1.0;
    

    if(cur_queue_size > 0){
      LOG(INFO) << total_sec / 1000 / 1000 << " s, " << total_sec / cur_queue_size << " per/micros.";

      VLOG(DEBUG_V4) << time_read_remote << " "<< cur_queue_size  << " prepare: " << time_prepare_read / cur_queue_size << "  execute: " << time_read_remote / cur_queue_size << "  commit: " << time3 / cur_queue_size;
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
    MessagePiece messagePiece = *(message->begin());

    auto message_type =
    static_cast<int>(messagePiece.get_message_type());

    // sync_queue.push(message);
    // LOG(INFO) << "sync_queue: " << sync_queue.read_available(); 
    for (auto it = message->begin(); it != message->end(); it++) {
      auto messagePiece = *it;
      auto message_type = messagePiece.get_message_type();
      //!TODO replica 
      if(message_type == static_cast<int>(LionMessage::REPLICATION_RESPONSE)){
        auto message_length = messagePiece.get_message_length();
        
        ////  // LOG(INFO) << "recv : " << ++total_async;
        // async_message_num.fetch_sub(1);
        int debug_key;
        auto stringPiece = messagePiece.toStringPiece();
        Decoder dec(stringPiece);
        dec >> debug_key;

        // async_message_respond_num.fetch_add(1);
        VLOG(DEBUG_V16) << "async_message_respond_num : " << async_message_respond_num.load() << "from " << message->get_source_node_id() << " to " << message->get_dest_node_id() << " " << debug_key;
      }
    }
    // if(static_cast<int>(LionMessage::METIS_SEARCH_REQUEST) <= message_type && 
    //    message_type <= static_cast<int>(LionMessage::METIS_IGNORE)){
    //   in_queue_metis.push(message);
    // } else {
    in_queue.push(message);
    // }
    
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

private:
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
        if(type < controlMessageHandlers.size()){
          // transaction router from Generator
          controlMessageHandlers[type](
            messagePiece,
            *async_messages[message->get_source_node_id()], db,
            &txn_meta.router_transactions_queue, 
            &router_stop_queue
          );
        } else {
          messageHandlers[type](messagePiece,
                                *sync_messages[message->get_source_node_id()], 
                                sync_messages,
                                db, context, partitioner,
                                txn_meta.c_transactions_queue);

          if(type == static_cast<int>(LionMessage::ASYNC_SEARCH_RESPONSE) || 
             type == static_cast<int>(LionMessage::ASYNC_SEARCH_RESPONSE_ROUTER_ONLY)){
              async_respond_num.fetch_add(1);
          }
        }

        if (logger) {
          logger->write(messagePiece.toStringPiece().data(),
                        messagePiece.get_message_length());
        }
      }

      size += message->get_message_count();
      flush_sync_messages();
      flush_async_messages();
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
      ITable *table = this->db.find_table(table_id, partition_id);
      // master-replica
      size_t coordinatorID = this->partitioner->master_coordinator(table_id, partition_id, key);
      uint64_t coordinator_secondaryIDs = 0; // = context.coordinator_num + 1;
      if(readKey.get_write_lock_bit()){
        // write key, the find all its replica
        LionInitPartitioner* tmp = (LionInitPartitioner*)(this->partitioner);
        coordinator_secondaryIDs = tmp->secondary_coordinator(table_id, partition_id, key);
      }
      // sec keys replicas
      readKey.set_dynamic_coordinator_id(coordinatorID);
      readKey.set_router_value(coordinatorID, coordinator_secondaryIDs);

      bool remaster = false;
      if (coordinatorID == coordinator_id) {
        // master-replica is at local node 
        std::atomic<uint64_t> &tid = table->search_metadata(key, success);
        if(success == false){
          return 0;
        }
        // immediatly lock local record 赶快本地lock
        if(readKey.get_write_lock_bit()){
          TwoPLHelper::write_lock(tid, success);
          // VLOG(DEBUG_V14) << "LOCK-LOCAL-write " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " tid:" << tid;
        } else {
          TwoPLHelper::read_lock(tid, success);
          // VLOG(DEBUG_V14) << "LOCK-read " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " tid:" << tid ;
        }
        // 
        txn.tids[key_offset] = &tid;

        if(success){
          // VLOG(DEBUG_V14) << "LOCK-LOCAL. " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " tid:" << tid ;
          readKey.set_read_respond_bit();
        } else {
          return 0;
        }
        local_read = true;
      } else {
        // master not at local, but has a secondary one. need to be remastered
        // FUCK 此处获得的table partition并不是我们需要从对面读取的partition
        remaster = table->contains(key); // current coordniator
        // if(remaster && context.read_on_replica){ // && !readKey.get_write_lock_bit()
          
        //   std::atomic<uint64_t> &tid = table->search_metadata(key, success);
        //   TwoPLHelper::read_lock(tid, success);
        //   if(!success){
        //     return 0;
        //   }
        //   txn.tids[key_offset] = &tid;
        //   // VLOG(DEBUG_V8) << "LOCK LOCAL " << table_id << " ASK " << coordinatorID << " " << *(int*)key << " " << remaster;
        //   readKey.set_read_respond_bit();
        //   local_read = true;
          
        //   for(size_t i = 0; i <= context.coordinator_num; i ++ ){ 
        //     // also send to generator to update the router-table
        //     if(i == coordinator_id){
        //       continue; // local
        //     }
        //     if(i == coordinatorID){
        //       // target
        //       txn.network_size += MessageFactoryType::new_async_search_message(
        //           *(this->messages[i]), *table, key, 
        //           key_offset, 
        //           txn.id,
        //           remaster, false);
        //     } else {
        //       // others, only change the router
        //       txn.network_size += MessageFactoryType::new_async_search_router_only_message(
        //           *(this->messages[i]), *table, key, 
        //           key_offset, 
        //           txn.id,
        //           false);
        //     }   
        //     VLOG(DEBUG_V8) << "ASYNC REMASTER " << table_id << " ASK " << i << " " << *(int*)key << " " << txn.readSet.size();
        //     // txn.asyncPendingResponses++;
        //     this->async_pend_num.fetch_add(1);
        //   }
        //   // this->flush_messages(messages);
          
        // }
        if(!context.read_on_replica){
          remaster = false;
        }
        if(remaster){
          txn.remaster_cnt ++ ;
          VLOG(DEBUG_V12) << "LOCK LOCAL " << table_id << " ASK " << coordinatorID << " " << *(int*)key << " " << txn.readSet.size();
        } else {
          txn.migrate_cnt ++ ;
        }
      }

      if (local_index_read || local_read) {
        auto ret = protocol.search(table_id, partition_id, key, value, success);
        return ret;
      } else {
        for(size_t i = 0; i <= context.coordinator_num; i ++ ){ 
          // also send to generator to update the router-table
          if(i == coordinator_id){
            continue; // local
          }
          if(i == coordinatorID){
            // target
            txn.network_size += MessageFactoryType::new_search_message(
                *(this->messages[i]), *table, key, 
                key_offset, txn.id, remaster, false);
          } else {
            // others, only change the router
            txn.network_size += MessageFactoryType::new_search_router_only_message(
                *(this->messages[i]), *table, 
                key, key_offset, txn.id, false);
          }            
          txn.pendingResponses++;
            
          VLOG(DEBUG_V8) << "SYNC !! " << txn.id << " " 
                         << table_id   << " ASK " 
                         << i << " " << *(int*)key << " " << txn.readSet.size() << " " << txn.pendingResponses;
          // LOG(INFO) << "txn.pendingResponses: " << txn.pendingResponses << " " << readKey.get_write_lock_bit();
        }
        txn.distributed_transaction = true;
        return 0;
      }
    };

    txn.remasterOnlyReadRequestHandler =
        [this, &txn, &protocol](std::size_t table_id, std::size_t partition_id,
                     uint32_t key_offset, const void *key, void *value,
                     bool local_index_read, bool &success) -> uint64_t {
      bool local_read = false;
      auto &readKey = txn.readSet[key_offset];
      ITable *table = this->db.find_table(table_id, partition_id);
      // master-replica
      size_t coordinatorID = this->partitioner->master_coordinator(table_id, partition_id, key);
      uint64_t coordinator_secondaryIDs = 0; // = context.coordinator_num + 1;
      if(readKey.get_write_lock_bit()){
        // write key, the find all its replica
        LionInitPartitioner* tmp = (LionInitPartitioner*)(this->partitioner);
        coordinator_secondaryIDs = tmp->secondary_coordinator(table_id, partition_id, key);
      }
      // sec keys replicas
      readKey.set_dynamic_coordinator_id(coordinatorID);
      readKey.set_router_value(coordinatorID, coordinator_secondaryIDs);

      bool remaster = false;
      if (coordinatorID == coordinator_id) {
        // master-replica is at local node 
        std::atomic<uint64_t> &tid = table->search_metadata(key, success);
        if(success == false){
          return 0;
        }
        // immediatly lock local record 赶快本地lock
        // if(readKey.get_write_lock_bit()){
        //   TwoPLHelper::write_lock(tid, success);
        //   // VLOG(DEBUG_V14) << "LOCK-LOCAL-write " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " tid:" << tid;
        // } else {
        //   TwoPLHelper::read_lock(tid, success);
        //   // VLOG(DEBUG_V14) << "LOCK-read " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " tid:" << tid ;
        // }
        // // 
        // txn.tids[key_offset] = &tid;

        if(success){
          // VLOG(DEBUG_V14) << "LOCK-LOCAL. " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " tid:" << tid ;
          readKey.set_read_respond_bit();
        } else {
          return 0;
        }
        local_read = true;
      } else {
        // master not at local, but has a secondary one. need to be remastered
        // FUCK 此处获得的table partition并不是我们需要从对面读取的partition
        remaster = table->contains(key); // current coordniator
        if(remaster && context.read_on_replica){
          
          std::atomic<uint64_t> &tid = table->search_metadata(key, success);
          // TwoPLHelper::read_lock(tid, success);
          // txn.tids[key_offset] = &tid;
          // VLOG(DEBUG_V8) << "LOCK LOCAL " << table_id << " ASK " << coordinatorID << " " << *(int*)key << " " << remaster;
          readKey.set_read_respond_bit();
          local_read = true;
          
          for(size_t i = 0; i <= context.coordinator_num; i ++ ){ 
            // also send to generator to update the router-table
            if(i == coordinator_id){
              continue; // local
            }
            if(i == coordinatorID){
              // target
              txn.network_size += MessageFactoryType::new_async_search_message(
                  *(this->messages[i]), *table, key, 
                  key_offset, txn.id, remaster, false);
            } else {
              // others, only change the router
              txn.network_size += MessageFactoryType::new_async_search_router_only_message(
                  *(this->messages[i]), *table, key, 
                  key_offset, txn.id, false);
            }   
            VLOG(DEBUG_V8) << "ASYNC REMASTER " << table_id << " ASK " << i << " " << *(int*)key << " " << txn.readSet.size();
            // txn.asyncPendingResponses++;
            this->async_pend_num.fetch_add(1);
          }
          // this->flush_messages(messages);
          
        }
        if(!context.read_on_replica){
          remaster = false;
        }
        if(remaster){
          txn.remaster_cnt ++ ;
          VLOG(DEBUG_V12) << "LOCK LOCAL " << table_id << " ASK " << coordinatorID << " " << *(int*)key << " " << txn.readSet.size();
        } else {
          txn.migrate_cnt ++ ;
        }
      }

      if (local_index_read || local_read) {
        auto ret = protocol.search(table_id, partition_id, key, value, success);
        return ret;
      } else {
        // for(size_t i = 0; i <= context.coordinator_num; i ++ ){ 
        //   // also send to generator to update the router-table
        //   if(i == coordinator_id){
        //     continue; // local
        //   }
        //   if(i == coordinatorID){
        //     // target
        //     txn.network_size += MessageFactoryType::new_search_message(
        //         *(this->messages[i]), *table, key, key_offset, remaster, false);
        //   } else {
        //     // others, only change the router
        //     txn.network_size += MessageFactoryType::new_search_router_only_message(
        //         *(this->messages[i]), *table, key, key_offset, false);
        //   }            
        //   txn.pendingResponses++;
        //   // LOG(INFO) << "txn.pendingResponses: " << txn.pendingResponses << " " << readKey.get_write_lock_bit();
        // }
        // txn.distributed_transaction = true;
        return 0;
      }
    };



    txn.remote_request_handler = [this]() { return this->process_request(); };
    txn.message_flusher = [this]() { this->flush_messages(messages); };
  }

  void flush_messages(std::vector<std::unique_ptr<Message>> &messages_) {
    for (auto i = 0u; i < messages_.size(); i++) {
      if (i == coordinator_id) {
        continue;
      }

      if (messages_[i]->get_message_count() == 0) {
        continue;
      }

      auto message = messages_[i].release();
      ////debug

//      auto it = message->begin(); 
//      MessagePiece messagePiece = *it;
//      LOG(INFO) << "messagePiece " << messagePiece.get_message_type() << " " << i << " = " << static_cast<int>(LionMessage::REPLICATION_RESPONSE);
      ////
      out_queue.push(message);
      messages_[i] = std::make_unique<Message>();
      init_message(messages_[i].get(), i);
    }
  }

  void flush_sync_messages() { flush_messages(sync_messages); }

  void flush_metis_sync_messages() { flush_messages(metis_sync_messages); }

  void flush_async_messages() { flush_messages(async_messages); }

  // void flush_record_messages() { flush_messages(record_messages); }

  void init_message(Message *message, std::size_t dest_node_id) {
    message->set_source_node_id(coordinator_id);
    message->set_dest_node_id(dest_node_id);
    message->set_worker_id(id);
  }

private:
  DatabaseType &db;
  const ContextType &context;
  uint32_t &batch_size;
  std::unique_ptr<Partitioner> l_partitioner, s_partitioner;
  Partitioner* partitioner;
  Partitioner* metis_partitioner;

  
  RandomType random;
  RandomType metis_random;

  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> async_message_num;
  std::atomic<uint32_t> async_message_respond_num;

  std::atomic<uint32_t> metis_async_message_num;
  std::atomic<uint32_t> metis_async_message_respond_num;

  std::atomic<uint32_t> router_transactions_send, router_transaction_done;

  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  std::atomic<uint32_t> &skip_s_phase;

  std::atomic<uint32_t> &transactions_prepared;
  std::atomic<uint32_t> cur_real_distributed_cnt;


  TransactionMeta<WorkloadType>& txn_meta;
  StorageType storage;


  // ShareQueue<int> s_txn_id_queue_self;
  // ShareQueue<int> c_txn_id_queue_self;

  ShareQueue<int> sub_c_txn_id_queue;

  
  // std::vector<StorageType> storages_self;
  // std::vector<std::unique_ptr<TransactionType>> &r_transactions_queue;


  
  std::unique_ptr<Delay> delay;
  std::unique_ptr<BufferedFileWriter> logger;
  Percentile<int64_t> percentile;
  // std::unique_ptr<TransactionType> transaction;

  std::atomic<uint32_t> async_pend_num;
  std::atomic<uint32_t> async_respond_num;

  std::vector<std::unique_ptr<Message>> messages;
  std::vector<std::unique_ptr<Message>> metis_messages;

  // transaction only commit in a single group
  std::queue<std::unique_ptr<TransactionType>> q;
  std::vector<std::unique_ptr<Message>> sync_messages, async_messages, // record_messages, 
                                        metis_sync_messages;
  // std::vector<std::function<void(MessagePiece, Message &, DatabaseType &,
  //                                TransactionType *, std::deque<simpleTransaction>*)>>
  //     messageHandlers;
  std::vector<std::function<void(
              MessagePiece, 
              Message &,               
              std::vector<std::unique_ptr<Message>>&, 
              DatabaseType &, 
              const ContextType &, 
              Partitioner *,
              std::vector<std::unique_ptr<TransactionType>>&
              )>>
      messageHandlers;
  LockfreeQueue<Message *, 100860> in_queue, out_queue,
                          //  in_queue_metis,  
                           sync_queue; // for value sync when phase switching occurs

  // ShareQueue<simpleTransaction> &txn_meta.router_transactions_queue;
  std::deque<int> router_stop_queue;

  // HashMap<9916, std::string, int> &data_pack_map;

  std::vector<
      std::function<void(MessagePiece, Message &, DatabaseType &, ShareQueue<simpleTransaction>* ,std::deque<int>* )>>
      controlMessageHandlers;

  std::size_t remaster_delay_transactions;

  ContextType s_context, c_context;
  ProtocolType* s_protocol, *c_protocol;
  WorkloadType* c_workload;
  WorkloadType* s_workload;
  
  StorageType metis_storage;// 临时存储空间   

  std::vector<std::unique_ptr<std::mutex>> messages_mutex;

  std::deque<uint64_t> s_source_coordinator_ids, c_source_coordinator_ids, r_source_coordinator_ids;

  std::vector<std::pair<size_t, size_t> > res; // record tnx

  Percentile<int64_t> router_percentile, analyze_percentile, execute_latency;

};
} // namespace star