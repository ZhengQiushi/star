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


  LionExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
               const ContextType &context, uint32_t &batch_size,
               std::atomic<uint32_t> &worker_status,
               std::atomic<uint32_t> &n_complete_workers,
               std::atomic<uint32_t> &n_started_workers, 
               std::atomic<uint32_t> &skip_s_phase, 
               std::atomic<uint32_t> &transactions_prepared, 
               lion::TransactionMeta<WorkloadType>& txn_meta
               )
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
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)) {

    for (auto i = 0u; i <= context.coordinator_num; i++) {

      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);


      sync_messages.emplace_back(std::make_unique<Message>());
      init_message(sync_messages[i].get(), i);

      async_messages.emplace_back(std::make_unique<Message>());
      init_message(async_messages[i].get(), i);

    }

    messageHandlers = MessageHandlerType::get_message_handlers();
    controlMessageHandlers = ControlMessageHandler<DatabaseType>::get_message_handlers();

    message_stats.resize(messageHandlers.size(), 0);
    message_sizes.resize(messageHandlers.size(), 0);

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
    LOG(INFO) << "replication_fence : " << async_message_num.load() << " " << async_message_respond_num.load();

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
        router_transactions_queue.pop_no_wait(success);
      if(!success) break;
      n_network_size.fetch_add(simple_txn.size);
      
      uint32_t txn_id;
      
      // if(!simple_txn.is_distributed){
      //   {
      //     std::unique_ptr<TransactionType> null_txn(nullptr);
      //     std::lock_guard<std::mutex> l(txn_meta.s_l);
      //     txn_id = txn_meta.s_transactions_queue.size();
      //     if(txn_id >= txn_meta.s_storages.size()){
      //       DCHECK(false);
      //     }
      //     txn_meta.s_transactions_queue.push_back(std::move(null_txn));
      //     txn_meta.s_txn_id_queue.push_no_wait(txn_id);
      //   }
      //   auto p = s_workload->unpack_transaction(context, 0, txn_meta.s_storages[txn_id], simple_txn);
      //   p->fully_single_transaction = true;
      //   // if(p.get() == nullptr) continue;
      //   txn_meta.s_transactions_queue[txn_id] = std::move(p);
      // } else {
        {
          std::unique_ptr<TransactionType> null_txn(nullptr);
          std::lock_guard<std::mutex> l(txn_meta.c_l);
          txn_id = txn_meta.c_transactions_queue.size();
          if(txn_id >= txn_meta.c_storages.size()){
            DCHECK(false);
          }
          txn_meta.c_transactions_queue.push_back(std::move(null_txn));
          txn_meta.c_txn_id_queue.push_no_wait(txn_id);
        }
        auto p = c_workload->unpack_transaction(context, 0, txn_meta.c_storages[txn_id], simple_txn);
        
        if(simple_txn.is_real_distributed){
          cur_real_distributed_cnt += 1;
          p->distributed_transaction = true;
          // if(cur_real_distributed_cnt < 10){
          //   LOG(INFO) << " test if abort?? " << simple_txn.keys[0] << " " << simple_txn.keys[1];
          // }
        } 
        p->id = txn_id;
        // if(p.get() == nullptr) continue;
        txn_meta.c_transactions_queue[txn_id] = std::move(p);
      // }
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
      if(clear_status.load() == true){
        clear_time_status();
        clear_status.store(false);
      }

      do {
        status = static_cast<ExecutorStatus>(worker_status.load());
        process_request();
        std::this_thread::sleep_for(std::chrono::microseconds(5));
        if (status == ExecutorStatus::EXIT) {
          // commit transaction in s_phase;
          commit_transactions();
          LOG(INFO) << "Executor " << id << " exits.";
          VLOG_IF(DEBUG_V, id==0) << "TIMES : " << times; 

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
        }
      } while (status != ExecutorStatus::START);

      // commit transaction in s_phase;
      commit_transactions();

      process_request();

      // c_phase
      VLOG_IF(DEBUG_V, id==0) << "wait c_phase "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";
      
      

      int router_recv_txn_num = 0;
      // 准备transaction
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

      VLOG_IF(DEBUG_V, id==0) << txn_meta.c_transactions_queue.size() << " "  <<txn_meta.s_transactions_queue.size() << " OMG : " << cur_real_distributed_cnt;
      
      txn_meta.transactions_prepared.fetch_add(1);
      // // }

      while(txn_meta.transactions_prepared.load() < context.worker_num){
        std::this_thread::yield();
        process_request();
      }

      n_started_workers.fetch_add(1);

      if(cur_real_distributed_cnt > 0){
      LOG(INFO) << "[C-PHASE] do remaster "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";

        do_remaster_transaction(ExecutorStatus::START, txn_meta.c_transactions_queue,async_message_num);
        async_fence();
      }

      VLOG_IF(DEBUG_V, id==0) << "[C-PHASE] do remaster "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";

      run_transaction(ExecutorStatus::START, 
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
      
      // replication_fence(ExecutorStatus::START);

      // commit transaction in c_phase;
      // commit_transactions();
      while (static_cast<ExecutorStatus>(worker_status.load()) !=
             ExecutorStatus::CLEANUP) {
        process_request();
        std::this_thread::sleep_for(std::chrono::microseconds(5));
      }
      // now = std::chrono::steady_clock::now();
      //   VLOG_IF(DEBUG_V, id==0) << "[C-PHASE] worker " << id << " wait to s_phase";
      //   while (static_cast<ExecutorStatus>(worker_status.load()) !=
      //         ExecutorStatus::S_PHASE) {
      //     std::this_thread::sleep_for(std::chrono::microseconds(5));
      //     process_request(); 
      //   }

      // VLOG_IF(DEBUG_V, id==0) << "S_phase enter"
      //         << std::chrono::duration_cast<std::chrono::milliseconds>(
      //                std::chrono::steady_clock::now() - begin)
      //                .count()
      //         << " milliseconds.";

      // // if(id == 0){
      // //   transactions_prepared.store(0);
      // // }

      // if(skip_s_phase.load() == false){
      //   // s_phase
      //   VLOG_IF(DEBUG_V, id==0) << "[S-PHASE] wait for s-phase "
      //           << std::chrono::duration_cast<std::chrono::milliseconds>(
      //                 std::chrono::steady_clock::now() - begin)
      //                 .count()
      //           << " milliseconds.";
      //   // now = std::chrono::steady_clock::now();

      //   n_started_workers.fetch_add(1);
      //   VLOG_IF(DEBUG_V, id==0) << "[S-PHASE] worker " << id << " ready to run_transaction";

      //   // r_size = txn_meta.s_transactions_queue.size();
      //   // LOG(INFO) << "txn_meta.s_transactions_queue.size() : " <<  r_size;
      //   run_transaction(ExecutorStatus::S_PHASE, 
      //   txn_meta.s_transactions_queue, 
      //   txn_meta.s_txn_id_queue,
      //   async_message_num);
        
      //   // VLOG_IF(DEBUG_V, id==0) << "worker " << id << " ready to replication_fence";

      //   VLOG_IF(DEBUG_V, id==0) << "[S-PHASE] done "
      //           << std::chrono::duration_cast<std::chrono::milliseconds>(
      //                 std::chrono::steady_clock::now() - begin)
      //                 .count()
      //           << " milliseconds.";
      //   // now = std::chrono::steady_clock::now();
        
      //   // replication_fence(ExecutorStatus::S_PHASE);
      //   n_complete_workers.fetch_add(1);
      //   VLOG_IF(DEBUG_V, id==0) << "[S-PHASE] fence "
      //           << std::chrono::duration_cast<std::chrono::milliseconds>(
      //                 std::chrono::steady_clock::now() - begin)
      //                 .count()
      //           << " milliseconds.";
      //   // now = std::chrono::steady_clock::now();

      //   // once all workers are stop, we need to process the replication
      //   // requests

      //   while (static_cast<ExecutorStatus>(worker_status.load()) ==
      //         ExecutorStatus::S_PHASE) {
      //     process_request();
      //     std::this_thread::sleep_for(std::chrono::microseconds(5));
      //   }

      //   VLOG_IF(DEBUG_V, id==0) << "wait back "
      //           << std::chrono::duration_cast<std::chrono::milliseconds>(
      //                 std::chrono::steady_clock::now() - begin)
      //                 .count()
      //           << " milliseconds.";
      //   // now = std::chrono::steady_clock::now();


      //   // n_complete_workers has been cleared
      //   process_request();
      //   n_complete_workers.fetch_add(1);
      //   txn_meta.done.fetch_add(1);
      // } else {
      //   VLOG_IF(DEBUG_V, id==0) << "skip s phase";
      //   process_request();
      //   n_complete_workers.fetch_add(1);
      //   txn_meta.done.fetch_add(1);
      // }


      auto execution_schedule_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - begin)
                    .count();

      if(cur_time > 10)
        execute_latency.add(execution_schedule_time);

      LOG(INFO) << "whole batch "
              << execution_schedule_time
              << " milliseconds, " 
              << commit_num;
      
      // time_total.add(execution_schedule_time * 1000.0 / commit_num);
      commit_num = 0;
      if(id == 0){
        txn_meta.clear();
      }
      VLOG_IF(DEBUG_V, id==0) << "sync "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count()
              << " milliseconds.";

      process_request();
      n_complete_workers.fetch_add(1);

    }
    VLOG_IF(DEBUG_V, id==0) << "TIMES : " << times; 


  }

  void record_commit_transactions(TransactionType &txn){

    // time_router.add(txn.b.time_router);
    // time_scheuler.add(txn.b.time_scheuler);
    // time_local_locks.add(txn.b.time_local_locks);
    // time_remote_locks.add(txn.b.time_remote_locks);
    // time_execute.add(txn.b.time_execute);
    // time_commit.add(txn.b.time_commit);
    // time_wait4serivce.add(txn.b.time_wait4serivce);
    // time_other_module.add(txn.b.time_other_module);

    txn.b.time_latency = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - txn.b.startTime)
                         .count();

    txn_statics.add(txn.b);

    total_latency.add(std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - txn.startTime)
                         .count());
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

    if (status == ExecutorStatus::START) {
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

    if (status == ExecutorStatus::START) {
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
    int time_read_remote1 = 0;
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
    size_t i = 0;
    for(;;) {
      bool success = false;
      if(status == ExecutorStatus::START){
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

      
      count += 1;
      auto& transaction = cur_trans[i];
      auto txnStartTime = transaction->b.startTime
                        = std::chrono::steady_clock::now();

      if(false){ // naive_router && router_to_other_node(status == ExecutorStatus::START)){
        // pass
        router_txn_num++;
      } else {
        bool retry_transaction = false;

        auto now = std::chrono::steady_clock::now();

        do {
          ////  // LOG(INFO) << "LionExecutor: "<< id << " " << "process_request" << i;
          process_request();
          ////
          last_seed = random.get_seed();

          if (retry_transaction) {
            transaction->reset();
          } else {
            setupHandlers(*transaction, *protocol);
          }
          //#####
          int before_prepare = std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - transaction->b.startTime)
              .count();
          time_before_prepare_set += before_prepare;
          now = std::chrono::steady_clock::now();
          transaction->b.time_wait4serivce += before_prepare;
          //#####
          
          transaction->prepare_read_execute(id);

          //#####
          int prepare_read = std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - transaction->b.startTime)
              .count();

          time_prepare_read += prepare_read;
          now = std::chrono::steady_clock::now();
          transaction->b.time_local_locks += prepare_read;
          //#####
          
          auto result = transaction->read_execute(id, ReadMethods::REMOTE_READ_WITH_TRANSFER);
          // ####
          int remote_read = std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - transaction->b.startTime)
              .count();
          time_read_remote += remote_read;
          now = std::chrono::steady_clock::now();
          transaction->b.time_remote_locks += remote_read;
          // #### 
          
          if(result != TransactionResult::READY_TO_COMMIT){
            retry_transaction = false;
            protocol->abort(*transaction, sync_messages);
            n_abort_no_retry.fetch_add(1);
            continue;
          } else {
            result = transaction->prepare_update_execute(id);

            // ####
            int write_time = std::chrono::duration_cast<std::chrono::microseconds>(
                                                                  std::chrono::steady_clock::now() - transaction->b.startTime)
                .count();
            time_read_remote1 += write_time;
            now = std::chrono::steady_clock::now();
            transaction->b.time_execute += write_time;
            // #### 
          }


          if (result == TransactionResult::READY_TO_COMMIT) {
            bool commit = protocol->commit(*transaction, sync_messages, async_message_num, false);
            
            n_network_size.fetch_add(transaction->network_size);
            if (commit) {
              n_commit.fetch_add(1);
              retry_transaction = false;
              
              n_migrate.fetch_add(transaction->migrate_cnt);
              n_remaster.fetch_add(transaction->remaster_cnt);
              // if(transaction->migrate_cnt > 0){
              //   LOG(INFO) << "migrate : " << *(int*)transaction->readSet[0].get_key() << " "
              //             << *(int*)transaction->readSet[1].get_key();
              // }
              // ####
              int commit_time = std::chrono::duration_cast<std::chrono::microseconds>(
                                                                    std::chrono::steady_clock::now() - transaction->b.startTime)
                  .count();
              time3 += commit_time;
              transaction->b.time_commit += commit_time;
              
              now = std::chrono::steady_clock::now();
              // ####
              record_commit_transactions(*transaction);
              // txn_meta.commit_num.fetch_add(1);
              commit_num += 1;

              q.push(std::move(transaction));
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
              protocol->abort(*transaction, sync_messages);
            }
          } else {
            n_abort_no_retry.fetch_add(1);
            protocol->abort(*transaction, sync_messages);
          }
        } while (retry_transaction);
      }


      if (i % context.batch_flush == 0) {
        flush_sync_messages();
        flush_async_messages(); 
      }
      
      txn_percentile.add(
              std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - txnStartTime)
              .count()
        
      );
    }
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
      << " execute1: " << time_read_remote1 / count 
      << " commit: "  << time3 / count 
      << " send: "    << time4 / count
      << "\n "
      << " : "        << why[11] / count
      << " : "        << why[12] / count
      << " : "        << why[13] / count; // << "  router : " << time1 / cur_queue_size; 
      // LOG(INFO) << "remaster_delay_transactions: " << remaster_delay_transactions;
      // remaster_delay_transactions = 0;

      // int test = 2601515;
      // LOG(INFO) << "w2601515 " << db.get_dynamic_coordinator_id(context.coordinator_num, 0, (void*) &test);
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

    if (status == ExecutorStatus::START) {
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
    int cnt = 0;
    size_t cur_queue_size = cur_trans.size();
    int router_txn_num = 0;


    // for(;;) {
    for (auto x = id; x < cur_queue_size; x += context.worker_num) {
      bool success = false;
      int i = txn_meta.c_txn_id_queue.pop_no_wait(success);
      if(!success){
        break;
      }
      if(i >= (int)cur_trans.size() || cur_trans[i].get() == nullptr){
        // DCHECK(false) << i << " " << cur_trans.size();
        continue;
      }
      
      if(cur_trans[i]->distributed_transaction == false){
        txn_meta.c_txn_id_queue.push_no_wait(i);
        continue;
      }
      sub_c_txn_id_queue.push_no_wait(i);

      if(context.migration_only == true){
        continue;
      }

      cnt += 1;
      cur_trans[i]->startTime = std::chrono::steady_clock::now();
      bool retry_transaction = false;

      auto rematser_begin = std::chrono::steady_clock::now();
      
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

        auto result = cur_trans[i]->read_execute(id, ReadMethods::REMASTER_ONLY);
        
        if(result != TransactionResult::READY_TO_COMMIT){
          retry_transaction = false;
          protocol->abort(*cur_trans[i], async_messages);
          n_abort_no_retry.fetch_add(1);
        }

      } while (retry_transaction);

      int remaster_time = std::chrono::duration_cast<std::chrono::microseconds>(
                                                              std::chrono::steady_clock::now() - cur_trans[i]->b.startTime)
            .count();
      cur_trans[i]->b.time_other_module += remaster_time;

      n_migrate.fetch_add(cur_trans[i]->migrate_cnt);
      n_remaster.fetch_add(cur_trans[i]->remaster_cnt);

      cur_trans[i]->reset();

      if (i % context.batch_flush == 0) {
        flush_async_messages(); 
        flush_sync_messages();
      }
    }
    flush_async_messages();
    flush_sync_messages();

    auto total_sec = std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::steady_clock::now() - begin)
                     .count() * 1.0;
    

    if(cnt > 0){
      LOG(INFO) << "rrrrremaster : " << total_sec / 1000 / 1000 << " s, " << total_sec / cnt << " per/micros.";

      VLOG(DEBUG_V4) << time_read_remote << " "<< cnt  << " prepare: " << time_prepare_read / cnt << "  execute: " << time_read_remote / cnt << "  commit: " << time3 / cnt;
    } else {
      LOG(INFO) << "skip remaster";
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
    if (id == 0) {
      for (auto i = 0u; i < message_stats.size(); i++) {
        LOG(INFO) << "message stats, type: " << i
                  << " count: " << message_stats[i]
                  << " total size: " << message_sizes[i];
      }
      // write_latency.save_cdf(context.cdf_path);
    }

  }

  void push_message(Message *message) override { 

    // message will only be of type signal, COUNT
    MessagePiece messagePiece = *(message->begin());
    auto message_type =
    static_cast<int>(messagePiece.get_message_type());
    for (auto it = message->begin(); it != message->end(); it++) {
      auto messagePiece = *it;
      auto message_type = messagePiece.get_message_type();
      //!TODO replica 
      if(message_type == static_cast<int>(LionMessage::REPLICATION_RESPONSE)){
        auto message_length = messagePiece.get_message_length();
        // int debug_key;
        // auto stringPiece = messagePiece.toStringPiece();
        // Decoder dec(stringPiece);
        // dec >> debug_key;

        async_message_respond_num.fetch_add(1);
        // VLOG(DEBUG_V16) << "async_message_respond_num : " << async_message_respond_num.load() << "from " << message->get_source_node_id() << " to " << message->get_dest_node_id() << " " << debug_key;
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
            *messages[message->get_source_node_id()], db,
            &router_transactions_queue, 
            &router_stop_queue
          );
        } else {
          messageHandlers[type](messagePiece,
                                *messages[message->get_source_node_id()], 
                                messages,
                                db, context, partitioner,
                                txn_meta.c_transactions_queue);

          if(type == static_cast<int>(LionMessage::ASYNC_SEARCH_RESPONSE) || 
             type == static_cast<int>(LionMessage::ASYNC_SEARCH_RESPONSE_ROUTER_ONLY)){
              async_respond_num.fetch_add(1);
          }
        }
        message_stats[type]++;
        message_sizes[type] += messagePiece.get_message_length();

        // if (logger) {
        //   logger->write(messagePiece.toStringPiece().data(),
        //                 messagePiece.get_message_length());
        // }
      }

      size += message->get_message_count();
      flush_messages(messages);
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
      readKey.set_dynamic_coordinator_id(coordinatorID);


      if(readKey.get_write_lock_bit()){
        // write key, the find all its replica
        LionInitPartitioner* tmp = (LionInitPartitioner*)(this->partitioner);
        uint64_t coordinator_secondaryIDs = tmp->secondary_coordinator(table_id, partition_id, key);
        readKey.set_router_value(coordinatorID, coordinator_secondaryIDs);
      }
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
          DCHECK(txn.fully_single_transaction == false);
          // master not at local, but has a secondary one. need to be remastered
          // FUCK 此处获得的table partition并不是我们需要从对面读取的partition
          remaster = table->contains(key); // current coordniator

          if(!context.read_on_replica){
            remaster = false;
          }
          if(remaster && !context.migration_only){
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
        DCHECK(txn.fully_single_transaction == false);
        for(size_t i = 0; i <= context.coordinator_num; i ++ ){ 
          // also send to generator to update the router-table
          if(i == coordinator_id){
            continue; // local
          }
          if(i == coordinatorID){
            // target
            txn.network_size += MessageFactoryType::new_search_message(
                *(this->sync_messages[i]), *table, key, 
                key_offset, txn.id, remaster, false);

                
            // LOG(INFO) << "SYNC !! " << txn.id << " " 
            //           << table_id   << " ASK " 
            //           << i << " " << *(int*)key << " " << txn.readSet.size() << " " 
            //           << txn.pendingResponses 
            //           << db.get_dynamic_coordinator_id(context.coordinator_num, 0, key);
          } else {
            // others, only change the router
            txn.network_size += MessageFactoryType::new_search_router_only_message(
                *(this->sync_messages[i]), *table, 
                key, key_offset, txn.id, false);
          }            
          txn.pendingResponses++;
            

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
                  *(this->sync_messages[i]), *table, key, 
                  key_offset, txn.id, remaster, false);
            } else {
              // others, only change the router
              txn.network_size += MessageFactoryType::new_async_search_router_only_message(
                  *(this->sync_messages[i]), *table, key, 
                  key_offset, txn.id, false);
            }   
            VLOG(DEBUG_V8) << "ASYNC REMASTER " << table_id << " ASK " << i << " " << *(int*)key << " " << txn.readSet.size();
            // txn.asyncPendingResponses++;
            this->async_pend_num.fetch_add(1);
          }
          
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
        return 0;
      }
    };



    txn.remote_request_handler = [this]() { return this->process_request(); };
    txn.message_flusher = [this]() { this->flush_messages(sync_messages); };
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

  // void flush_metis_sync_messages() { flush_messages(metis_sync_messages); }

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


  lion::TransactionMeta<WorkloadType>& txn_meta;
  StorageType storage;


  // ShareQueue<int> s_txn_id_queue_self;
  // ShareQueue<int> c_txn_id_queue_self;

  ShareQueue<int> sub_c_txn_id_queue;

  int commit_num;
  // std::vector<StorageType> storages_self;
  // std::vector<std::unique_ptr<TransactionType>> &r_transactions_queue;


  
  std::unique_ptr<Delay> delay;
  std::unique_ptr<BufferedFileWriter> logger;
  Percentile<int64_t> percentile;
  // std::unique_ptr<TransactionType> transaction;

  std::atomic<uint32_t> async_pend_num;
  std::atomic<uint32_t> async_respond_num;

  // transaction only commit in a single group
  std::queue<std::unique_ptr<TransactionType>> q;
  std::vector<std::unique_ptr<Message>> sync_messages, async_messages, messages;
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

  std::vector<std::size_t> message_stats, message_sizes;

  ShareQueue<simpleTransaction> router_transactions_queue;

  // Percentile<int64_t> router_percentile, analyze_percentile, execute_latency;

};
} // namespace star