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

#include "common/ShareQueue.h"
#include "protocol/LionS/LionSManager.h"
#include "protocol/ClaySS/ClaySSMessage.h"
#include "protocol/ClaySS/ClaySSTransaction.h"
#include "protocol/ClaySS/ClaySSMeta.h"

#include <chrono>

namespace star {
template <class Workload> class ClaySSExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;
  using TransactionType = ClaySSTransaction;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = ClaySS<DatabaseType>;

  using MessageType = ClaySSMessage;
  using MessageFactoryType = ClaySSMessageFactory;
  using MessageHandlerType = ClaySSMessageHandler<DatabaseType>;

  ClaySSExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
           const ContextType &context, std::atomic<uint32_t> &worker_status,
           std::atomic<uint32_t> &n_complete_workers,
           std::atomic<uint32_t> &n_started_workers
           )
      : Worker(coordinator_id, id), db(db), context(context),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        txn_meta(1, context.batch_size),
        partitioner(std::make_unique<LionDynamicPartitioner<Workload> >(
            coordinator_id, context.coordinator_num, db)),
        random(reinterpret_cast<uint64_t>(this)),
        protocol(db, context, *partitioner),
        workload(coordinator_id, worker_status, db, random, *partitioner, start_time),
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

    // partitioner_ptr = partitioner.get();

    messageHandlers = MessageHandlerType::get_message_handlers();
    controlMessageHandlers = ControlMessageHandler<DatabaseType>::get_message_handlers();

    message_stats.resize(messageHandlers.size(), 0);
    message_sizes.resize(messageHandlers.size(), 0);
  }


  void unpack_route_transaction(){
    cur_real_distributed_cnt = 0;

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
          break;
          DCHECK(false) << txn_id << " " << txn_meta.storages.size();
        }
        txn_meta.c_transactions_queue.push_back(std::move(null_txn));
        txn_meta.c_txn_id_queue.push_no_wait(txn_id);
      }
      auto p = workload.unpack_transaction(context, 0, txn_meta.storages[txn_id], simple_txn);

      if(simple_txn.is_real_distributed){
        cur_real_distributed_cnt += 1;
        p->distributed_transaction = true;
      } 

      txn_meta.c_transactions_queue[txn_id] = std::move(p);
    }
  }


  void do_remaster_transaction(std::vector<std::unique_ptr<TransactionType>>& cur_txns) {
    /**
     * @brief 
     * @note modified by truth 22-01-24
     *       
    */
    auto begin = std::chrono::steady_clock::now();
    // int time1 = 0;
    int time_prepare_read = 0;    
    int time_read_remote = 0;
    int time3 = 0;


    // uint64_t last_seed = 0;

    auto i = 0u;
    int cnt = 0;
    size_t cur_queue_size = my_sz;// cur_txns.size();
    int router_txn_num = 0;


    // for(;;) {
    for (auto x = 0; x < cur_queue_size; x += 1) {
      bool success = false;
      int i = txn_meta.c_txn_id_queue.pop_no_wait(success);
      if(!success){
        break;
      }
      if(i >= (int)cur_txns.size() || cur_txns[i].get() == nullptr){
        // DCHECK(false) << i << " " << cur_txns.size();
        continue;
      }
      transaction = cur_txns[i].get();
      if(transaction->distributed_transaction == false){
        txn_meta.c_txn_id_queue.push_no_wait(i);
        continue;
      }
      sub_c_txn_id_queue.push_no_wait(i);

      if(context.migration_only == true){
        continue;
      }

      
      cnt += 1;
      transaction->startTime = std::chrono::steady_clock::now();
      bool retry_transaction = false;

      auto rematser_begin = std::chrono::steady_clock::now();
      
      do {
        ////  // LOG(INFO) << "LionExecutor: "<< id << " " << "process_request" << i;
        process_request();
        // last_seed = random.get_seed();

        if (retry_transaction) {
          transaction->reset();
        } else {
          setupHandlers(*transaction);
        }

        auto now = std::chrono::steady_clock::now();

        transaction->prepare_read_execute(id);

        auto result = transaction->read_execute(id, ReadMethods::REMASTER_ONLY);
        
        if(transaction->abort_lock){
          // protocol.abort(*transaction, async_messages);
          retry_transaction = true;
          n_abort_no_retry.fetch_add(1);
          LOG(INFO) << "RETRY" << *(int*)transaction->readSet[0].get_key() << " " << *(int*)transaction->readSet[1].get_key();
        } else {
          retry_transaction = false;
        }
        
        n_migrate.fetch_add(transaction->migrate_cnt);
        n_remaster.fetch_add(transaction->remaster_cnt);

      } while (retry_transaction);

      // int remaster_time = std::chrono::duration_cast<std::chrono::microseconds>(
      //                                                         std::chrono::steady_clock::now() - cur_txns[i]->b.startTime)
      //       .count();
      // cur_txns[i]->b.time_other_module += remaster_time;

      // n_migrate.fetch_add(cur_txns[i]->migrate_cnt);
      // n_remaster.fetch_add(cur_txns[i]->remaster_cnt);
      int remaster_num = transaction->remaster_cnt;
      n_network_size.fetch_add(transaction->network_size);

      transaction->reset();
      transaction->remaster_cnt = remaster_num;
      
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
    

    if(cnt > 100){
      LOG(INFO) << "rrrrremaster : " << total_sec / 1000 / 1000 << " s, " << total_sec / cnt << " per/micros." << cnt ;

      VLOG(DEBUG_V4) << time_read_remote << " "<< cnt  << " prepare: " << time_prepare_read / cnt << "  execute: " << time_read_remote / cnt << "  commit: " << time3 / cnt;
    } else {
      // LOG(INFO) << "skip remaster";
    }

  }

    void run_transaction(std::vector<std::unique_ptr<TransactionType>> & cur_txns, 
                         ShareQueue<int>& txn_id_queue) {
    /**
     * @brief 
     * @note modified by truth 22-01-24
     *       
    */
    uint64_t last_seed = 0;

    // int time1 = 0;
    auto begin = std::chrono::steady_clock::now();
    int cur_cross_num = 0;
    std::vector<int> why(30, 0);

    auto i = 0u;
    size_t cur_queue_size = my_sz; // cur_txns.size(); 
    int router_txn_num = 0;
    // 
    int total_span = 0;
    // for (auto i = id; i < cur_queue_size; i += context.worker_num) {
    for(int x = 0 ; x < my_sz; x += 1) {
      bool success = false;
      i = txn_id_queue.pop_no_wait(success);
      if(!success){
        i = sub_c_txn_id_queue.pop_no_wait(success);
      } 
      if(!success){
        break;
      }
      if(i >= cur_txns.size() || cur_txns[i].get() == nullptr){
        // DCHECK(false) << i << " " << cur_txns.size();
        continue;
      }

      auto now = std::chrono::steady_clock::now();
      bool retry_transaction = false;
      count += 1;
      transaction = cur_txns[i].get();
      transaction->startTime = std::chrono::steady_clock::now();;
      // LOG(INFO) << transaction->get_query_printed();

      do {
        process_request();
        last_seed = random.get_seed();

          time_before_prepare_set += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();

        if (retry_transaction) {
          transaction->reset();
        } else {
          // std::size_t partition_id = transaction->get_partition_id();
          setupHandlers(*transaction);
        }
        
        
        // auto result = transaction->execute(id);
          transaction->prepare_read_execute(id);

          time_prepare_read += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
          now = std::chrono::steady_clock::now();
          

          // time1 += std::chrono::duration_cast<std::chrono::microseconds>(
          //                                                       std::chrono::steady_clock::now() - now)
          //     .count();
          // now = std::chrono::steady_clock::now();
          
          auto result = transaction->read_execute(id, ReadMethods::REMOTE_READ_WITH_TRANSFER);
          
          if(result != TransactionResult::READY_TO_COMMIT){
            retry_transaction = false;
            protocol.abort(*transaction, sync_messages);
            n_abort_no_retry.fetch_add(1);
            continue;
          } else {
            result = transaction->prepare_update_execute(id);
          }
          
          time_read_remote += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
          now = std::chrono::steady_clock::now();


        if(!transaction->is_transmit_requests()){
          if(transaction->distributed_transaction){
            cross_txn_num ++ ;
            cur_cross_num += 1;
            if(cur_cross_num < 5){
              // auto k = transaction->get_query();
              // auto kc = transaction->get_query_master();
              // // MoveRecord<WorkloadType> rec;
              // // rec.set_real_key(*(uint64_t*)readSet[0].get_key());
              
              // LOG(INFO) << "cross_txn_num ++ : " <<  total_span / (i + 1) << " " << 
              //                                " " << k[0] << " | "
              //                                " " << k[1] << " | " 
              //                                " " << k[2] << " | " 
              //                                " " << k[3] << " | " 
              //                                " " << k[4];
            }
          } else {
            single_txn_num ++ ;

            // debug
            // LOG(INFO) << "single_txn_num: " << single_txn_num << " " 
            //           << transaction->get_query_printed();
          }
        } else {
          LOG(INFO) << "transmit txn: " << transaction->get_query_printed();
        }

        if (result == TransactionResult::READY_TO_COMMIT) {
          // // LOG(INFO) << "StarExecutor: "<< id << " " << "commit" << i;

          bool commit = protocol.commit(*transaction, sync_messages, async_messages);
          n_network_size.fetch_add(transaction->network_size);
          if (commit) {
            n_commit.fetch_add(1);
            retry_transaction = false;

            total_span += std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now() - transaction->startTime)
                          .count();
                          
            q.push(std::move(cur_txns[i]));
          } else {
            if (transaction->abort_lock) {
              n_abort_lock.fetch_add(1);
            } else {
              DCHECK(transaction->abort_read_validation);
              n_abort_read_validation.fetch_add(1);
            }
            random.set_seed(last_seed);
            retry_transaction = false;
            // protocol.abort(*transaction, sync_messages);
            n_abort_no_retry.fetch_add(1);
          }
        } else if(result == TransactionResult::TRANSMIT_REQUEST){
          // pass
          n_migrate.fetch_add(transaction->migrate_cnt);
          n_remaster.fetch_add(transaction->remaster_cnt);
        } else {
          protocol.abort(*transaction, sync_messages);
          n_abort_no_retry.fetch_add(1);
        }

          time3 += std::chrono::duration_cast<std::chrono::microseconds>(
                                                                std::chrono::steady_clock::now() - now)
              .count();
          now = std::chrono::steady_clock::now();


      } while (retry_transaction);

        ControlMessageFactory::router_transaction_response_message(*(messages[context.coordinator_num]));
        flush_messages(messages);

      // cur_txns.pop_front();

      // if (i % context.batch_flush == 0) {
        flush_async_messages();
        flush_sync_messages();
      // }
    }
    flush_async_messages();
    flush_sync_messages();

    if(cur_queue_size > 500)
      LOG(INFO) << "cur_queue_size: " << cur_queue_size  << " " << " cross_txn_num: " << cur_cross_num;

  }
  
  
  bool is_router_stopped(int& router_recv_txn_num){
    bool ret = false;
    size_t num = 1; // context.coordinator_num; // 1
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
      
      int total_size = txn_meta.c_transactions_queue.size();
      for(int i = 0 ; i < total_size; i += 500){
        if(context.repartition_strategy == "clay" || cur_real_distributed_cnt > 0 && context.lion_self_remaster){
          do_remaster_transaction(txn_meta.c_transactions_queue);
        }

        run_transaction(txn_meta.c_transactions_queue,
                        txn_meta.c_txn_id_queue);
      }


      // migrate_run_transaction(migrate_txn_meta.t_txn_id_queue,
      //                         migrate_txn_meta.t_transactions_queue);

      auto total_sec = std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::steady_clock::now() - begin)
                        .count();
      
      if((total_sec - cur_time_second > print_per_second || total_sec % print_per_second == 0) && 
          count > 0 && 
          cur_time_second != total_sec){

        LOG(INFO) << "  nums: "    << count << " " 
                  << single_txn_num << " " << cross_txn_num << " "
                  << " pre: "     << time_before_prepare_request / count
                  << " set: "     << time_before_prepare_set / count 
                  << " gap: "     << time_before_prepare_read / count
                  << "  prepare: " << time_prepare_read / count  << " " << time_prepare_read
                  << "  execute: " << time_read_remote / count   << " " << time_read_remote
                  << " execute1: " << time_read_remote1 / count 
                  << "  commit: "  << time3 / count              << " " << time3
                  << "  router : " << time1 / count              << " " << time1
                  << print_per_second * 1000 * 1000 / count << " per/micros.";

        single_txn_num = 0;
        cross_txn_num = 0;
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
            *messages[message->get_source_node_id()], db,
            &router_transactions_queue,
            &router_stop_queue
          );
        } else {
          messageHandlers[type](messagePiece,
                              *messages[message->get_source_node_id()], 
                              db, context, partitioner.get(),
                              transaction);
        }
        message_stats[type]++;
        message_sizes[type] += messagePiece.get_message_length();
      }

      size += message->get_message_count();
      // flush_async_messages();
      flush_messages(messages);
    }
    return size;
  }

  void setupHandlers(TransactionType &txn) {
    txn.lock_request_handler =
        [this, &txn](std::size_t table_id, std::size_t partition_id,
                     uint32_t key_offset, const void *key, void *value,
                     bool local_index_read, bool write_lock, bool &success,
                     bool &remote) -> uint64_t {
      if (local_index_read) {
        success = true;
        remote = false;
        return this->protocol.search(table_id, partition_id, key, value);
      }

      bool local_read = false;
      auto &readKey = txn.readSet[key_offset];
      ITable *table = this->db.find_table(table_id, partition_id);
      // master-replica
      size_t coordinatorID = readKey.get_dynamic_coordinator_id();

      if (coordinatorID == context.coordinator_id) {

        remote = false;

        std::atomic<uint64_t> &tid = table->search_metadata(key);

        if (write_lock) {
          TwoPLHelper::write_lock(tid, success);
        } else {
          TwoPLHelper::read_lock(tid, success);
        }

        if(success){
          // todo ycsb only
          std::atomic<uint64_t> *lock_tid;
          if(WorkloadType::which_workload == myTestSet::YCSB){
            ycsb::ycsb::key k(*(size_t*)key % 200000 / 50000 + 200000 * partition_id);
            ITable &router_lock_table = *db.find_router_lock_table(table_id, partition_id);
            lock_tid = &router_lock_table.search_metadata((void*) &k);
          } else {
            ITable &router_lock_table = *db.find_router_lock_table(table_id, partition_id);
            lock_tid = &router_lock_table.search_metadata((void*) key);
          }
          TwoPLHelper::write_lock(*lock_tid, success); // be locked 

          if(!success){
            // 
            // LOG(INFO) << " Failed to add write lock, since current is being migrated" << *(int*)key;
            if (write_lock) {
              TwoPLHelper::write_lock_release(tid);
            } else {
              TwoPLHelper::read_lock_release(tid);
            }
          } else {
            TwoPLHelper::write_lock_release(*lock_tid);
          }
          // 
        }

        if (success) {
          // LOG(INFO) << "local LOCK " << *(int*)key;
          return this->protocol.search(table_id, partition_id, key, value);
        } else {
          // LOG(INFO) << "FAILED TO LOCK " << *(int*)key;
          return 0;
        }

      } else {
        remote = true;

        if (write_lock) {
          txn.network_size += MessageFactoryType::new_write_lock_message(
              *(this->sync_messages[coordinatorID]), *table, key, key_offset);
          txn.pendingResponses++;

        } else {
          txn.network_size += MessageFactoryType::new_read_lock_message(
              *(this->sync_messages[coordinatorID]), *table, key, key_offset);
          txn.pendingResponses++;
        }
        txn.distributed_transaction = true;

        txn.remote_cnt += 1;
        return 0;
      }
    };

    txn.remaster_request_handler =
        [this, &txn](std::size_t table_id, std::size_t partition_id,
                     uint32_t key_offset, const void *key, void *value,
                     bool local_index_read, bool write_lock, bool &success,
                     bool &remote) -> uint64_t {
      if (local_index_read) {
        success = true;
        remote = false;
        return this->protocol.search(table_id, partition_id, key, value);
      }

      bool local_read = false;
      auto &readKey = txn.readSet[key_offset];
      ITable *table = this->db.find_table(table_id, partition_id);
      size_t coordinatorID = readKey.get_dynamic_coordinator_id();
      // master-replica
      // size_t coordinatorID = this->partitioner->master_coordinator(table_id, partition_id, key);
      // uint64_t coordinator_secondaryIDs = 0; // = context.coordinator_num + 1;
      // if(readKey.get_write_lock_request_bit()){
      //   // write key, the find all its replica
      //   LionInitPartitioner* tmp = (LionInitPartitioner*)(this->partitioner.get());
      //   coordinator_secondaryIDs = tmp->secondary_coordinator(table_id, partition_id, key);
      // }
      // // sec keys replicas
      // readKey.set_dynamic_coordinator_id(coordinatorID);
      // readKey.set_router_value(coordinatorID, coordinator_secondaryIDs);

      bool remaster = false;
      success = true;
      if (coordinatorID == context.coordinator_id) {
        
        remote = false;

        std::atomic<uint64_t> &tid = table->search_metadata(key);

        // if (write_lock) {
        //   TwoPLHelper::write_lock(tid, success);
        // } else {
        //   TwoPLHelper::read_lock(tid, success);
        // }

        // if (success) {
          // LOG(INFO) << "remaster LOCK " << *(int*)key;
        return this->protocol.search(table_id, partition_id, key, value);
        // } else {
        //   LOG(INFO) << "FAILED TO LOCK " << *(int*)key;
        //   return 0;
        // }

      } else {
        remote = true;

        remaster = table->contains(key); // current coordniator

        if(remaster && !context.migration_only){
          txn.remaster_cnt ++ ;
          VLOG(DEBUG_V12) << "LOCK LOCAL " << table_id << " ASK " << coordinatorID << " " << *(int*)key << " " << txn.readSet.size();
          for(size_t i = 0; i <= context.coordinator_num; i ++ ){ 
            // also send to generator to update the router-table
            if(i == coordinator_id){
              continue; // local
            }
            if(i == coordinatorID){
              // target
                // LOG(INFO) << "new_transmit_message : " << *(int*)key << " " << context.coordinator_id << " -> " << coordinatorID;
                txn.network_size += MessageFactoryType::new_async_search_message(
                    *(this->sync_messages[coordinatorID]), *table, key, key_offset, 
                    remaster, txn.op_);
                txn.pendingResponses++;
            } else {
                // others, only change the router
                // if(i == context.coordinator_num){
                //   LOG(INFO) << "new_transmit_router_only_message: " <<  *(int*)key;
                // }
                txn.network_size += MessageFactoryType::new_async_search_router_only_message(*(this->sync_messages[i]), *table, key, key_offset, txn.op_, coordinatorID);
                txn.pendingResponses++;
            }            
          }
        } 



        txn.distributed_transaction = true;
      }
      return 0;
    };

    txn.remote_request_handler = [this]() { return this->process_request(); };
    txn.message_flusher = [this]() { 
      this->flush_sync_messages();
      this->flush_async_messages();
     };
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

  void flush_sync_messages() { flush_messages(sync_messages); }

  void flush_async_messages() { flush_messages(async_messages); }

  void init_message(Message *message, std::size_t dest_node_id) {
    message->set_source_node_id(coordinator_id);
    message->set_dest_node_id(dest_node_id);
    message->set_worker_id(id);
  }

protected:
  DatabaseType &db;
  const ContextType &context;
  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  // std::vector<std::unique_ptr<TransactionType>>  &r_transactions_queue;
  // ShareQueue<int> &txn_id_queue;
  clayss::TransactionMeta<WorkloadType> txn_meta;


  std::unique_ptr<Partitioner> partitioner;
  // Partitioner* partitioner_ptr;
  std::mutex mm;
  
  ShareQueue<int> sub_c_txn_id_queue;

  RandomType random;
  ProtocolType protocol;
  WorkloadType workload;
  std::unique_ptr<Delay> delay;
  Percentile<int64_t> commit_latency, write_latency;
  Percentile<int64_t> dist_latency, local_latency;
  TransactionType* transaction;
  std::vector<std::unique_ptr<Message>> sync_messages, async_messages, messages;
  std::vector<
      std::function<void(MessagePiece, Message &, DatabaseType &, const ContextType &,  Partitioner *, TransactionType *)>>
      messageHandlers;

  std::vector<
      std::function<void(MessagePiece, Message &, DatabaseType &, ShareQueue<simpleTransaction>*, std::deque<int>* )>>
      controlMessageHandlers;


  ShareQueue<simpleTransaction> router_transactions_queue;
  // ShareQueue<simpleTransaction> router_transactions_queue;           // router
  std::deque<int> router_stop_queue;           // router stop-SIGNAL

  std::vector<std::size_t> message_stats, message_sizes;
  LockfreeQueue<Message *, 100860> in_queue, out_queue;
  std::atomic<uint32_t> cur_real_distributed_cnt;

  std::queue<std::unique_ptr<TransactionType>> q;
  Percentile<int64_t> percentile;

  int single_txn_num = 0;
  int cross_txn_num = 0;

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

  int my_sz = 500;

};
} // namespace star