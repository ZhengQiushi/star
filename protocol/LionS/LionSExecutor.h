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
// #include "Manager.h"
#include <chrono>
#include "protocol/LionS/LionSMeta.h"

namespace star {
// namespace group_commit {

template <class Workload, class Protocol> class LionSExecutor : public Worker {
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

  LionSExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
           const ContextType &context, std::atomic<uint32_t> &worker_status,
           std::atomic<uint32_t> &n_complete_workers,
           std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        txn_meta(context.coordinator_num, context.batch_size),
        partitioner(std::make_unique<LionDynamicPartitioner<Workload> >(
            coordinator_id, context.coordinator_num, db)),
        random(reinterpret_cast<uint64_t>(this)),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)) {

    for (auto i = 0u; i <= context.coordinator_num; i++) {
      sync_messages.emplace_back(std::make_unique<Message>());
      init_message(sync_messages[i].get(), i);

      async_messages.emplace_back(std::make_unique<Message>());
      init_message(async_messages[i].get(), i);

      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    protocol = new ProtocolType(db, context, *partitioner, id);
    workload = new WorkloadType(coordinator_id, worker_status, db, random, *partitioner, start_time);

    messageHandlers = MessageHandlerType::get_message_handlers();
    controlMessageHandlers = ControlMessageHandler<DatabaseType>::get_message_handlers();

    message_stats.resize(messageHandlers.size(), 0);
    message_sizes.resize(messageHandlers.size(), 0);
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
      
        {
          std::unique_ptr<TransactionType> null_txn(nullptr);
          std::lock_guard<std::mutex> l(txn_meta.c_l);
          txn_id = txn_meta.c_transactions_queue.size();
          if(txn_id >= txn_meta.storages.size()){
            DCHECK(false);
          }
          txn_meta.c_transactions_queue.push_back(std::move(null_txn));
          txn_meta.c_txn_id_queue.push_no_wait(txn_id);
        }
        auto p = workload->unpack_transaction(context, 0, txn_meta.storages[txn_id], simple_txn);
        
        if(simple_txn.is_real_distributed){
          cur_real_distributed_cnt += 1;
          p->distributed_transaction = true;
          // if(cur_real_distributed_cnt < 10){
          //   LOG(INFO) << " test if abort?? " << simple_txn.keys[0] << " " << simple_txn.keys[1];
          // }
          // if(transaction->distributed_transaction){
              // auto debug = p->debug_record_keys();
              // auto debug_master = p->debug_record_keys_master();

              // LOG(INFO) << " OMG ";
              // for(int i = 0 ; i < debug.size(); i ++){
              //   LOG(INFO) << " #### : " << debug[i] << " " << debug_master[i]; 
              // }
          // }

        } 
        p->id = txn_id;
        txn_meta.c_transactions_queue[txn_id] = std::move(p);
      // }
    }
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
  }

  void run_transaction(std::vector<std::unique_ptr<TransactionType>>& cur_trans,
                       ShareQueue<int>& txn_id_queue) {
    /**
     * @brief 
     * @note modified by truth 22-01-24
     *       
    */

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
      if(true){
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

          if (!retry_transaction) {
            setupHandlers(*transaction, protocol);
          }
          transaction->reset();

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
            // LOG(INFO) << "abort: ";
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
              if(transaction->migrate_cnt > 0 || transaction->remaster_cnt > 0){
                distributed_num.fetch_add(1);
                // LOG(INFO) << distributed_num.load();
              } else {
                singled_num.fetch_add(1);
              }
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

              // q.push(std::move(transaction));
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
              LOG(INFO) << "abort: ";
              protocol->abort(*transaction, sync_messages);
            }
          } else {
            n_abort_no_retry.fetch_add(1);
            protocol->abort(*transaction, sync_messages);
            LOG(INFO) << "abort: ";
          }
        } while (retry_transaction);
      }

        ControlMessageFactory::router_transaction_response_message(*(messages[context.coordinator_num]));
        flush_messages(messages);


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

    // auto total_sec = std::chrono::duration_cast<std::chrono::microseconds>(
    //                  std::chrono::steady_clock::now() - begin)
    //                  .count() * 1.0;
    // if(count > 0){
    //   LOG(INFO) << total_sec / 1000 / 1000 << " s, " << total_sec / count << " per/micros."
    //             << txn_percentile.nth(10) << " " 
    //             << txn_percentile.nth(50) << " "
    //             << txn_percentile.nth(80) << " "
    //             << txn_percentile.nth(90) << " "
    //             << txn_percentile.nth(95);

    
    //   VLOG(DEBUG_V4)  << time_read_remote << " " << count 
    //   << " pre: "     << time_before_prepare_request / count  
    //   << " set: "     << time_before_prepare_set / count 
    //   << " gap: "     << time_before_prepare_read / count
    //   << " prepare: " << time_prepare_read / count 
    //   << " execute: " << time_read_remote / count 
    //   << " execute1: " << time_read_remote1 / count 
    //   << " commit: "  << time3 / count 
    //   << " send: "    << time4 / count
    //   << "\n "
    //   << " : "        << why[11] / count
    //   << " : "        << why[12] / count
    //   << " : "        << why[13] / count; // << "  router : " << time1 / cur_queue_size; 
    //   // LOG(INFO) << "remaster_delay_transactions: " << remaster_delay_transactions;
    //   // remaster_delay_transactions = 0;
    // }
      

    ////  // LOG(INFO) << "router_txn_num: " << router_txn_num << "  local solved: " << cur_queue_size - router_txn_num;
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

    StorageType storage;
    uint64_t last_seed = 0;

    ExecutorStatus status;

    while ((status = static_cast<ExecutorStatus>(worker_status.load())) !=
           ExecutorStatus::START) {
      std::this_thread::yield();
    }

    n_started_workers.fetch_add(1);
    bool retry_transaction = false;

    auto begin = std::chrono::steady_clock::now();
    int print_per_second = 3;
    int cur_time_second = 0;
    do {
      
      process_request();

      unpack_route_transaction(); // 

      run_transaction(txn_meta.c_transactions_queue,
                      txn_meta.c_txn_id_queue);

      auto total_sec = std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::steady_clock::now() - begin)
                        .count();
      
      if(total_sec % print_per_second == 0 && count > 0 && cur_time_second != total_sec){

        LOG(INFO) << "  nums: "    << count 
                  << " pre: "     << time_before_prepare_request / count
                  << " set: "     << time_before_prepare_set / count 
                  << " gap: "     << time_before_prepare_read / count
                  << "  prepare: " << time_prepare_read / count  << " " << time_prepare_read
                  << "  execute: " << time_read_remote / count   << " " << time_read_remote
                  << " execute1: " << time_read_remote1 / count 
                  << "  commit: "  << time3 / count              << " " << time3
                  << "  router : " << time1 / count              << " " << time1
                  << print_per_second * 1000 * 1000 / count << " per/micros.";

        time_prepare_read = 0;
        time_before_prepare_set = 0;
        time_before_prepare_read = 0;
        time_before_prepare_request = 0;
        time_read_remote = 0;
        time_read_remote1 = 0;
        time1 = 0;
        time2 = 0;
        time3 = 0;
        time4 = 0;
        count = 0u;

        cur_time_second = total_sec;
      }
      txn_meta.clear();
      status = static_cast<ExecutorStatus>(worker_status.load());
    } while (status != ExecutorStatus::STOP && status != ExecutorStatus::CLEANUP);

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

    LOG(INFO) << "Executor " << id << " exits.";
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
    // MessagePiece messagePiece = *(message->begin());
    // auto message_type =
    // static_cast<int>(messagePiece.get_message_type());
    // for (auto it = message->begin(); it != message->end(); it++) {
    //   auto messagePiece = *it;
    //   auto message_type = messagePiece.get_message_type();
    //   //!TODO replica 
    //   if(message_type == static_cast<int>(SiloGCMessage::REPLICATION_RESPONSE)){


    //     // async_message_respond_num.fetch_add(1); // "async_message_respond_num : " << async_message_respond_num.load()
    //     LOG(INFO) << " get RESPONSE from " << message->get_source_node_id() << " to " << message->get_dest_node_id(); // << " " << debug_key;
    //   } else if(message_type == static_cast<int>(SiloGCMessage::REPLICATION_REQUEST)){
    //     auto message_length = messagePiece.get_message_length();
    //     int debug_key;
    //     auto stringPiece = messagePiece.toStringPiece();
    //     Decoder dec(stringPiece);
    //     dec >> debug_key;

    //     // async_message_respond_num.fetch_add(1); // "async_message_respond_num : " << async_message_respond_num.load()
    //     LOG(INFO) << " get REPLICATION_REQUEST from " << message->get_source_node_id() << " to " << message->get_dest_node_id() << " " << debug_key;
    //   }
    // }
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
            *async_messages[message->get_source_node_id()], db,
            &router_transactions_queue,
            &router_stop_queue
          );
        } else {
          messageHandlers[type](messagePiece,
                                *sync_messages[message->get_source_node_id()], 
                                sync_messages,
                                db, context, partitioner.get(),
                                txn_meta.c_transactions_queue);
        }
        // messageHandlers[type](messagePiece,
        //                       *sync_messages[message->get_source_node_id()],
        //                       *table, transaction.get());
        message_stats[type]++;
        message_sizes[type] += messagePiece.get_message_length();
      }

      size += message->get_message_count();
      flush_async_messages();
      flush_sync_messages();
    }
    return size;
  }

  // virtual void setupHandlers(TransactionType &txn) = 0;

  void setupHandlers(TransactionType &txn, ProtocolType *protocol) {
    txn.readRequestHandler =
        [this, &txn, protocol](std::size_t table_id, std::size_t partition_id,
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
        auto tmp = this->partitioner.get();
        uint64_t coordinator_secondaryIDs = tmp->secondary_coordinator(table_id, partition_id, key);
        readKey.set_router_value(coordinatorID, coordinator_secondaryIDs);
      }
      bool remaster = false;

        if (coordinatorID == coordinator_id) {
          // master-replica is at local node 
          std::atomic<uint64_t> &tid = table->search_metadata(key, success);
          if(success == false){
            LOG(INFO) << "failed LOCK-LOCAL. " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " tid:" << tid ;
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

          if(success){
            // todo ycsb only
            ycsb::ycsb::key k(*(size_t*)key % 200000 / 500 + 200000 * partition_id);
            ITable &router_lock_table = *db.find_router_lock_table(table_id, partition_id);
            std::atomic<uint64_t> &lock_tid = router_lock_table.search_metadata((void*) &k);
            TwoPLHelper::write_lock(lock_tid, success); // be locked 

            if(!success){
              // 
              // LOG(INFO) << " Failed to add write lock, since current is being migrated" << *(int*)key;
              if (readKey.get_write_lock_bit()) {
                TwoPLHelper::write_lock_release(tid);
              } else {
                TwoPLHelper::read_lock_release(tid);
              }
            } else {
              TwoPLHelper::write_lock_release(lock_tid);
            }
            // 
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
        auto ret = protocol->search(table_id, partition_id, key, value, success);
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
          } else {
            // others, only change the router
            txn.network_size += MessageFactoryType::new_search_router_only_message(
                *(this->sync_messages[i]), *table, 
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
        [this, &txn, protocol](std::size_t table_id, std::size_t partition_id,
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
        auto tmp = this->partitioner.get();
        coordinator_secondaryIDs = tmp->secondary_coordinator(table_id, partition_id, key);
      }
      // sec keys replicas
      readKey.set_dynamic_coordinator_id(coordinatorID);
      readKey.set_router_value(coordinatorID, coordinator_secondaryIDs);

      bool remaster = false;
      if (coordinatorID == coordinator_id) {
        // master-replica is at local node 
        std::atomic<uint64_t> &tid = table->search_metadata(key, success);
        if(success){
          readKey.set_read_respond_bit();
        } else {
          LOG(INFO) << "failed LOCK-LOCAL. " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " tid:" << tid ;
          
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
            // this->async_pend_num.fetch_add(1);
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
        auto ret = protocol->search(table_id, partition_id, key, value, success);
        return ret;
      } else {
        return 0;
      }
    };



    txn.remote_request_handler = [this]() { return this->process_request(); };
    txn.message_flusher = [this]() { this->flush_messages(sync_messages); };
  }


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

  void flush_sync_messages() { flush_messages(sync_messages); }

  void flush_async_messages() { flush_messages(async_messages); }

  void init_message(Message *message, std::size_t dest_node_id) {
    message->set_source_node_id(coordinator_id);
    message->set_dest_node_id(dest_node_id);
    message->set_worker_id(id);
  }

private:
  DatabaseType &db;
  const ContextType &context;
  // uint32_t &batch_size;
  std::unique_ptr<Partitioner> partitioner;
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
  // std::atomic<uint32_t> &skip_s_phase;

  // std::atomic<uint32_t> &transactions_prepared;
  std::atomic<uint32_t> cur_real_distributed_cnt;


  lions::TransactionMeta<WorkloadType> txn_meta;
  StorageType storage;


  // ShareQueue<int> s_txn_id_queue_self;
  // ShareQueue<int> c_txn_id_queue_self;

  ShareQueue<int> sub_c_txn_id_queue;

  int commit_num;
  // std::vector<StorageType> storages_self;
  // std::vector<std::unique_ptr<TransactionType>> &r_transactions_queue;

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
  ProtocolType* protocol;
  WorkloadType* workload;
  
  StorageType metis_storage;// 临时存储空间   

  std::vector<std::unique_ptr<std::mutex>> messages_mutex;

  std::deque<uint64_t> s_source_coordinator_ids, c_source_coordinator_ids, r_source_coordinator_ids;

  std::vector<std::pair<size_t, size_t> > res; // record tnx

  std::vector<std::size_t> message_stats, message_sizes;

  ShareQueue<simpleTransaction> router_transactions_queue;
  
  std::unique_ptr<Delay> delay;
  Percentile<int64_t> commit_latency, write_latency;
  Percentile<int64_t> dist_latency, local_latency;

  int time_prepare_read = 0;  
  int time_before_prepare_set = 0;
  int time_before_prepare_read = 0;  
  int time_before_prepare_request = 0;
  int time_read_remote = 0;
  int time_read_remote1 = 0;
  int time1 = 0;
  int time2 = 0;
  int time3 = 0;
  int time4 = 0;
  int count = 0;
};
// } // namespace group_commit

} // namespace star