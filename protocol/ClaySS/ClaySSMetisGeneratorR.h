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

#include <unordered_map>
#include "core/Coordinator.h"
#include <mutex>          // std::mutex, std::lock_guard

#include "protocol/ClaySS/ClaySSMeta.h"

namespace star {
namespace group_commit {

#define MAX_COORDINATOR_NUM 80


template <class Workload, class Protocol> class ClaySSMetisGeneratorR : public Worker {
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


  ClaySSMetisGeneratorR(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
           const ContextType &context, std::atomic<uint32_t> &worker_status,
           std::atomic<uint32_t> &n_complete_workers,
           std::atomic<uint32_t> &n_started_workers, 
           clayss::ScheduleMeta &schedule_meta)
      : Worker(coordinator_id, id), db(db), context(context),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        schedule_meta(schedule_meta),
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

      metis_async_messages.emplace_back(std::make_unique<Message>());
      init_message(metis_async_messages[i].get(), i);

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

    is_full_signal.store(0);
    generator_core_id.resize(context.coordinator_num);

    generator_num = 1;

    dispatcher_num = 1;
    dispatcher_id = 0;
    cur_txn_num = context.batch_size / dispatcher_num; 
    
    outfile_excel.open(context.data_src_path_dir + "metis_router.xls", std::ios::trunc); // ios::trunc
  }

  int router_transmit_request(ShareQueue<std::shared_ptr<myMove<WorkloadType>>>& move_plans){
    // transmit_request_queue
    std::vector<int> router_send_txn_cnt(context.coordinator_num, 0);

    auto new_transmit_generate = [&](int idx){ // int n
      simpleTransaction* s = new simpleTransaction();
      s->idx_ = idx;
      s->is_transmit_request = true;
      s->is_distributed = true;
      // s->partition_id = n;
      return s;
    };


    // 一一对应
    std::vector<simpleTransaction*> metis_txns;
    std::vector<std::shared_ptr<myMove<WorkloadType>>> cur_moves;




    const int transmit_block_size = 10;

    int cur_move_size = move_plans.size();
    long long access_freq_aver = 0;

    // pack up move-steps to transmit request
    double thresh_ratio = 1;
    for(int i = 0 ; i <  thresh_ratio * cur_move_size; i ++ ){
      bool success = false;
      std::shared_ptr<myMove<WorkloadType>> cur_move;
      
      success = move_plans.pop_no_wait(cur_move);
      DCHECK(success == true);
      
      
      auto metis_new_txn = new_transmit_generate(metis_transmit_idx ++ );
      metis_new_txn->access_frequency = cur_move->access_frequency;

      access_freq_aver += cur_move->access_frequency;

      for(auto move_record: cur_move->records){
          metis_new_txn->keys.push_back(move_record.record_key_);
          metis_new_txn->update.push_back(true);
      }
      //
      metis_txns.push_back(metis_new_txn);
      cur_moves.push_back(cur_move);
    }
    if(cur_move_size == 0){
      return cur_move_size;
    }
    access_freq_aver /= cur_move_size;

    scheduler_transactions_(metis_txns, router_send_txn_cnt, access_freq_aver);


    // pull request
    std::vector<simpleTransaction*> transmit_requests;
    // int64_t coordinator_id_dst = select_best_node(metis_new_txn);
    for(size_t i = 0 ; i < metis_txns.size(); i ++ ){
      // split into sub_transactions
      auto new_txn = new_transmit_generate(transmit_idx ++ );

      for(auto move_record: cur_moves[i]->records){
          new_txn->keys.push_back(move_record.record_key_);
          new_txn->update.push_back(false);
          new_txn->destination_coordinator = metis_txns[i]->destination_coordinator;
          new_txn->metis_idx_ = metis_txns[i]->idx_;
          new_txn->is_real_distributed = metis_txns[i]->is_real_distributed;
          new_txn->is_distributed = metis_txns[i]->is_distributed;

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


    move_plans.clear();

    
    for(size_t i = 0 ; i < transmit_requests.size(); i ++ ){ // 
      // outfile_excel << "Send LION Metis migration transaction ID(" << transmit_requests[i]->idx_ << " " << transmit_requests[i]->metis_idx_ << " " << transmit_requests[i]->keys[0] << " ) to " << transmit_requests[i]->destination_coordinator << "\n";

      router_request(router_send_txn_cnt, transmit_requests[i], RouterTxnOps::ADD_REPLICA);
      // metis_migration_router_request(router_send_txn_cnt, transmit_requests[i]);        
      // if(i > 5){ // debug
      //   break;
      // }
    }

    outfile_excel << "Done. \n";

    transmit_idx = 0; // split into sub-transactions
    metis_transmit_idx = 0;

    LOG(INFO) << "OMG transmit_requests.size() : " << transmit_requests.size();

    return cur_move_size;
  }



  int metis_router_transmit_request(ShareQueue<std::shared_ptr<myMove<WorkloadType>>>& move_plans){
    // transmit_request_queue
    std::vector<int> router_send_txn_cnt(context.coordinator_num, 0);

    auto new_transmit_generate = [&](int idx){ // int n
      simpleTransaction* s = new simpleTransaction();
      s->idx_ = idx;
      s->is_transmit_request = true;
      s->is_distributed = true;
      // s->partition_id = n;
      return s;
    };


    // 一一对应
    std::vector<simpleTransaction*> metis_txns;
    std::vector<std::shared_ptr<myMove<WorkloadType>>> cur_moves;




    const int transmit_block_size = 10;

    int cur_move_size = move_plans.size();
    long long access_freq_aver = 0;

    // pack up move-steps to transmit request
    double thresh_ratio = 1;
    for(int i = 0 ; i <  thresh_ratio * cur_move_size; i ++ ){
      bool success = false;
      std::shared_ptr<myMove<WorkloadType>> cur_move;
      
      success = move_plans.pop_no_wait(cur_move);
      DCHECK(success == true);
      
      
      auto metis_new_txn = new_transmit_generate(metis_transmit_idx ++ );
      metis_new_txn->access_frequency = cur_move->access_frequency;

      access_freq_aver += cur_move->access_frequency;

      for(auto move_record: cur_move->records){
          metis_new_txn->keys.push_back(move_record.record_key_);
          metis_new_txn->update.push_back(true);
      }
      //
      metis_txns.push_back(metis_new_txn);
      cur_moves.push_back(cur_move);
    }
    if(cur_move_size == 0){
      return cur_move_size;
    }
    access_freq_aver /= cur_move_size;

    scheduler_transactions_(metis_txns, router_send_txn_cnt, access_freq_aver);


    // pull request
    std::vector<simpleTransaction*> transmit_requests;
    // int64_t coordinator_id_dst = select_best_node(metis_new_txn);
    for(size_t i = 0 ; i < metis_txns.size(); i ++ ){
      // split into sub_transactions
      auto new_txn = new_transmit_generate(transmit_idx ++ );

      for(auto move_record: cur_moves[i]->records){
          new_txn->keys.push_back(move_record.record_key_);
          new_txn->update.push_back(false);
          if(cur_move_size < 20){
            new_txn->destination_coordinator = i % context.coordinator_num; 
          } else {
            new_txn->destination_coordinator = metis_txns[i]->destination_coordinator;
          }
          // 
          new_txn->metis_idx_ = metis_txns[i]->idx_;
          new_txn->is_real_distributed = metis_txns[i]->is_real_distributed;
          new_txn->is_distributed = metis_txns[i]->is_distributed;

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


    move_plans.clear();

    
    for(size_t i = 0 ; i < transmit_requests.size(); i ++ ){ // 
      // outfile_excel << "Send LION Metis migration transaction ID(" << transmit_requests[i]->idx_ << " " << transmit_requests[i]->metis_idx_ << " " << transmit_requests[i]->keys[0] << " ) to " << transmit_requests[i]->destination_coordinator << "\n";

      router_request(router_send_txn_cnt, transmit_requests[i], RouterTxnOps::TRANSFER);
      // metis_migration_router_request(router_send_txn_cnt, transmit_requests[i]);        
      // if(i > 5){ // debug
      //   break;
      // }
    }

    outfile_excel << "Done. \n";

    transmit_idx = 0; // split into sub-transactions
    metis_transmit_idx = 0;

    LOG(INFO) << "OMG transmit_requests.size() : " << transmit_requests.size();

    return cur_move_size;
  }


void migration(std::string file_name_){
     

    LOG(INFO) << "start read from file";
    my_clay->clear_graph();

    while(true){
      ShareQueue<std::shared_ptr<myMove<WorkloadType>>> rows;
      if(context.repartition_strategy == "lion"){
        my_clay->lion_partiion_read_from_file(file_name_.c_str(), context.batch_size, rows);
      } else if(context.repartition_strategy == "clay"){
        my_clay->clay_partiion_read_from_file(file_name_.c_str(), context.batch_size, rows);
      } else if(context.repartition_strategy == "metis"){
        my_clay->mmetis_partiion_read_from_file(file_name_.c_str(), context.batch_size, rows);
      }

      if(rows.size() <= 0){
        LOG(INFO) << "all metis DONE!!!!!!";
        break;
      }
      LOG(INFO) << "read from file done";

      auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - begin)
                              .count();
      LOG(INFO) << "lion loading file" << file_name_ << ". Used " << latency << " ms.";


      // my_clay->metis_partition_graph();

      latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - begin)
                          .count();
      LOG(INFO) << "lion with metis graph initialization finished. Used " << latency << " ms.";
      
      // std::vector<simpleTransaction*> transmit_requests(context.coordinator_num);
      
      int num = 0;
      if(context.repartition_strategy == "lion"){
        num = router_transmit_request(rows);
      } else if(context.repartition_strategy == "clay"){
        num = clay_router_transmit_request(rows);
      } else if(context.repartition_strategy == "metis"){
        num = metis_router_transmit_request(rows);
      }
      
      

      if(num > 0){
        LOG(INFO) << "router transmit request " << num; 
      }    
    }
  }

  // void migration(std::string file_name_){
     

  //   LOG(INFO) << "start read from file";
    
  //   if(context.repartition_strategy == "lion"){
  //     my_clay->lion_partiion_read_from_file(file_name_.c_str());
  //   } else if(context.repartition_strategy == "clay"){
  //     my_clay->clay_partiion_read_from_file(file_name_.c_str());
  //   } else if(context.repartition_strategy == "metis"){
  //     DCHECK(false);
  //   }

    
  //   LOG(INFO) << "read from file done";

  //   auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
  //                           std::chrono::steady_clock::now() - begin)
  //                           .count();
  //   LOG(INFO) << "lion loading file" << file_name_ << ". Used " << latency << " ms.";


  //   // my_clay->metis_partition_graph();

  //   latency = std::chrono::duration_cast<std::chrono::milliseconds>(
  //                       std::chrono::steady_clock::now() - begin)
  //                       .count();
  //   LOG(INFO) << "lion with metis graph initialization finished. Used " << latency << " ms.";
    
  //   // std::vector<simpleTransaction*> transmit_requests(context.coordinator_num);
    
  //   int num = 0;
  //   if(context.repartition_strategy == "lion"){
  //     num = router_transmit_request(my_clay->move_plans);
  //   } else if(context.repartition_strategy == "clay"){
  //     num = clay_router_transmit_request(my_clay->move_plans);
  //   } else if(context.repartition_strategy == "metis"){
  //     DCHECK(false);
  //   }
    
    

  //   if(num > 0){
  //     LOG(INFO) << "router transmit request " << num; 
  //   }    
  // }

  void txn_nodes_involved(simpleTransaction* t) {
      
      auto & txns_coord_cost_ = schedule_meta.txns_coord_cost;

      std::vector<int> busy_local(context.coordinator_num, 0);
      std::vector<int> replicate_busy_local(context.coordinator_num, 0);


      std::unordered_map<int, int> from_nodes_id;           // dynamic replica nums
      std::unordered_map<int, int> from_nodes_id_secondary; // secondary replica nums
      // std::unordered_map<int, int> nodes_cost;              // cost on each node
      std::vector<int> coordi_nums_;

      
      size_t ycsbTableID = ycsb::ycsb::tableID;
      auto query_keys = t->keys;

      // std::string str = "";

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

        // str += std::to_string(query_keys[j]) + "=" + std::to_string(cur_c_id) + " ";

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

      int replica_most_cnt = INT_MIN;
      int replica_max_node = -1;

      for(size_t cur_c_id = 0 ; cur_c_id < context.coordinator_num; cur_c_id ++ ){
        int cur_score = 0; // 5 - 5 * (busy_local[cur_c_id] * 1.0 / cur_txn_num); // 1 ~ 10

        size_t cnt_master = from_nodes_id[cur_c_id];
        size_t cnt_secondary = from_nodes_id_secondary[cur_c_id];
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

        if(cur_score > max_cnt){
          max_node = cur_c_id;
          max_cnt = cur_score;
        }

        if(cnt_secondary > replica_most_cnt){
          replica_max_node = cur_c_id;
          replica_most_cnt = cnt_secondary;
        }

        txns_coord_cost_[t->idx_][cur_c_id] = 10 * (int)query_keys.size() - cur_score;

        // str += " | " + std::to_string(txns_coord_cost_[t->idx_][cur_c_id]);

        replicate_busy_local[cur_c_id] += cnt_secondary;
      }


      if(context.random_router > 0){
        // 
        // size_t random_value = random.uniform_dist(0, 9);
        max_node = (query_keys[0] / context.keysPerPartition + 1) % context.coordinator_num;
      } 

      // LOG(INFO) << str;

      t->destination_coordinator = max_node;
      t->old_dest = t->destination_coordinator;

      t->execution_cost = 10 * (int)query_keys.size() - max_cnt;
      t->is_real_distributed = (max_cnt == 100 * (int)query_keys.size()) ? false : true;

      t->replica_heavy_node = replica_max_node;
      // if(t->is_real_distributed){
      //   std::string debug = "";
      //   for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      //     debug += std::to_string(txns_coord_cost_[t->idx_][i]) + " ";
      //   }
      //   LOG(INFO) << t->keys[0] << " " << t->keys[1] << " -> " << max_node << " : " << debug;
      // }
      size_t cnt_master = from_nodes_id[max_node];
      size_t cnt_secondary = from_nodes_id_secondary[max_node];

      // if(cnt_secondary + cnt_master != query_keys.size()){
      //   distributed_outfile_excel << t->keys[0] << "\t" << t->keys[1] << "\t" << max_node << "\n";
      // }

     return;
   }


  void txn_nodes_involved_tpcc(simpleTransaction* t) {

      auto & txns_coord_cost_ = schedule_meta.txns_coord_cost;

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

        txns_coord_cost_[t->idx_][cur_c_id] = 10 * (int)weight_sum - cur_score;
        replicate_busy_local[cur_c_id] += cnt_secondary;
      }


      // if(context.random_router > 0){
      //   // 
      //   // size_t random_value = random.uniform_dist(0, 9);
      //   size_t coordinator_id = (keys.W_ID - 1) % context.coordinator_num;
      //   max_node = coordinator_id; // query_keys[0];
        
      // } 


      t->destination_coordinator = max_node;
      t->old_dest = t->destination_coordinator;

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
  
  long long cal_load_average(std::unordered_map<size_t, long long>& busy_){
    long long cur_val = 0;
    for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      // 
      cur_val += busy_[i];
    }
    cur_val /= context.coordinator_num;

    return cur_val;
  }
  
  struct cmp{
    bool operator()(simpleTransaction* a, 
                    simpleTransaction* b){
      return a->execution_cost < b->execution_cost;
    }
  };

  void distributed_transactions(std::priority_queue<simpleTransaction*, 
                        std::vector<simpleTransaction*>, 
                        cmp>& q_, 
                        std::unordered_map<size_t, long long>& busy_){
    
    long long aver_val = cal_load_average(busy_);//; cur_thread_transaction_num / context.coordinator_num;
    int weight = 300;
    long long threshold = weight / (context.coordinator_num / 2) * weight / (context.coordinator_num / 2); // 2200 - 2800

    long long cur_val = cal_load_distribute(aver_val, busy_);

    if(cur_val > threshold){
      // start tradeoff for balancing
      int batch_num = 50; // 

      bool is_ok = true;
      do {
        
        for(int i = 0 ; i < batch_num && q_.size() > 0; i ++ ){

          std::unordered_map<int, int> overload_node;
          std::unordered_map<int, int> idle_node;
          aver_val = cal_load_average(busy_);
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
          simpleTransaction* t = q_.top();
          q_.pop();

          if(overload_node.count(t->destination_coordinator)){
            for(auto& idle: idle_node){
              // 
              busy_[t->destination_coordinator] -= t->access_frequency;
              overload_node[t->destination_coordinator] -= t->access_frequency;
              if(overload_node[t->destination_coordinator] <= 0){
                overload_node.erase(t->destination_coordinator);
              }
              
              // LOG(INFO) << "rebalanced " << t->idx_ << " " << t->keys[0] << " " << t->keys[1] << " " << t->destination_coordinator << "->" << idle.first;
              
              t->destination_coordinator = idle.first;
              busy_[t->destination_coordinator] += t->access_frequency;
              idle.second -= t->access_frequency;
              if(idle.second <= 0){
                idle_node.erase(idle.first);
              }
              break;
            }
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
  }


  void scheduler_transactions_(
      std::vector<simpleTransaction*>& metis_txns,
      std::vector<int>& router_send_txn_cnt,
      long long access_freq_aver){    
        
    // std::vector<simpleTransaction*> txns;
    std::priority_queue<simpleTransaction*, 
                        std::vector<simpleTransaction*>, 
                        cmp> heavy_queue;

    std::priority_queue<simpleTransaction*, 
                        std::vector<simpleTransaction*>, 
                        cmp> light_queue;

    std::unordered_map<size_t, long long> heavy_busy_;
    std::unordered_map<size_t, long long> light_busy_;
    for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      heavy_busy_[i] = 0;
      light_busy_[i] = 0;
    }
    

    // find minimal cost routing 
    for(size_t i = 0; i < metis_txns.size(); i ++ ){
      simpleTransaction* txn = metis_txns[i];
      txn_nodes_involved(txn);

      if((long long)txn->access_frequency > access_freq_aver){
        heavy_queue.push(txn);
        heavy_busy_[txn->destination_coordinator] += txn->access_frequency;
      } else {
        light_queue.push(txn);
        light_busy_[txn->destination_coordinator] += txn->access_frequency;
      }
    }

    LOG(INFO) << " before scheduler_transactions_ generate : " ;
    long long aver_val = cal_load_average(heavy_busy_);//; cur_thread_transaction_num / context.coordinator_num;
    for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      // 
      LOG(INFO) << i << " : " << heavy_busy_[i] << " " << (heavy_busy_[i] - aver_val) * (heavy_busy_[i] - aver_val) << " > ";
    }

    distributed_transactions(heavy_queue, heavy_busy_);
    // int cur_thread_transaction_num = metis_txns.size();
    // LOG(INFO) << "BEFORE";
    // for(int i = 0; i < txns.size(); i ++ ){
    //   LOG(INFO) << "txns[i]->destination_coordinator : " << i << " " << txns[i]->destination_coordinator;  
    // }
    // 100 * 110 workload * average 


    LOG(INFO) << " scheduler_transactions_ generate : " ;
    for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      heavy_busy_[i] += light_busy_[i];
    }
    aver_val = cal_load_average(heavy_busy_);
    for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      LOG(INFO) << i << " : " << heavy_busy_[i] << " " << (heavy_busy_[i] - aver_val) * (heavy_busy_[i] - aver_val);
    }

    distributed_transactions(light_queue, heavy_busy_);

    aver_val = cal_load_average(heavy_busy_);
    for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      LOG(INFO) << i << " : " << heavy_busy_[i] << " " << (heavy_busy_[i] - aver_val) * (heavy_busy_[i] - aver_val);
    }
    
    LOG(INFO) << "NEW";
    for(size_t i = 0; i < metis_txns.size(); i ++ ){
      // LOG(INFO) << "txns[i]->destination_coordinator : " << i << " " << metis_txns[i]->destination_coordinator;  
      // LOG(INFO) << txns[i]->idx_ << "\t" << txns[i]->destination_coordinator << "\t" << txns[i]->access_frequency << "\n";
    }
  }
  
  int scheduler_transactions(int dispatcher_num, int dispatcher_id){    

    if(schedule_meta.transactions_queue_self.size() < context.batch_size){
      return -1;
    }

    if(router_transaction_done.load() != router_transactions_send.load()){
      int a = router_transaction_done.load();
      int b = router_transactions_send.load();
      return -1;
    }

    router_transaction_done.store(0);
    router_transactions_send.store(0);

    int idx_offset = dispatcher_id * cur_txn_num;

    auto & txns              = schedule_meta.node_txns;
    auto & busy_             = schedule_meta.node_busy;
    // auto & node_replica_busy = schedule_meta.node_replica_busy;
    auto & txns_coord_cost   = schedule_meta.txns_coord_cost;
    
    auto staart = std::chrono::steady_clock::now();

    int real_distribute_num = 0;
    // which_workload_(crossPartition, (int)cur_timestamp);
    // find minimal cost routing 
    // LOG(INFO) << "txn_id.load() = " << schedule_meta.txn_id.load() << " " << cur_txn_num;
    
    std::vector<int> busy_local(context.coordinator_num, 0);
    std::vector<int> replicate_busy_local(context.coordinator_num, 0);
    
    int cnt = 0;

    while(cnt < context.batch_size){
      
      process_request();

      bool success = false;
      simpleTransaction* new_txn = schedule_meta.transactions_queue_self.pop_no_wait(success); 
      if(!success){
        break;
      }
      new_txn->is_transmit_request = true;

      int idx = cnt + idx_offset;

      txns[idx] = std::make_shared<simpleTransaction>(*new_txn);
      // if(i < 2){
      //   LOG(INFO) << i << " " << txns[idx]->is_distributed;
      // }
      auto& txn = txns[idx];
      txn->idx_ = idx;      

      

      if(WorkloadType::which_workload == myTestSet::YCSB){
        txn_nodes_involved(txn.get());
      } else {
        txn_nodes_involved_tpcc(txn.get());
      }

      if(txn->is_real_distributed){
        real_distribute_num += 1;
      }
      busy_local[txn->destination_coordinator] += 1;
      cnt += 1;
    }

    schedule_meta.txn_id.fetch_add(1);
    {
      std::lock_guard<std::mutex> l(schedule_meta.l);
      for(size_t i = 0; i < context.coordinator_num; i ++ ){
          busy_[i] += busy_local[i];
          // node_replica_busy[i] += replicate_busy_local[i];
      }
    }

    double cur_timestamp__ = std::chrono::duration_cast<std::chrono::microseconds>(
                 std::chrono::steady_clock::now() - staart)
                 .count() * 1.0 / 1000 ;

    LOG(INFO) << "scheduler : " << cur_timestamp__ << " " << schedule_meta.txn_id.load();
              
    while((int)schedule_meta.txn_id.load() < dispatcher_num){
      std::this_thread::sleep_for(std::chrono::microseconds(5));
    }


    long long threshold = 200 * 200; //200 / ((context.coordinator_num + 1) / 2) * 200 / ((context.coordinator_num + 1) / 2); // 2200 - 2800
    if(WorkloadType::which_workload == myTestSet::TPCC){
      threshold = 300 * 300;
    }
    int aver_val =  cnt * dispatcher_num / context.coordinator_num;
    long long cur_val = cal_load_distribute(aver_val, busy_);


    long long replica_threshold = 200 * 200 * context.keysPerPartition * context.keysPerPartition;
    int replica_aver_val = context.keysPerPartition * cnt * dispatcher_num / context.coordinator_num;
    // long long replica_cur_val = cal_load_distribute(replica_aver_val, node_replica_busy);


    if(dispatcher_id == 0){


    // LOG(INFO) << "replicate_busy: ";
    // for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
    //   LOG(INFO) <<" replicate_busy[" << i << "] = " << node_replica_busy[i];
    // }

    if(cur_val > threshold && context.random_router == 0){ 
      LOG(INFO) << "busy: ";
      for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
        LOG(INFO) << " busy[" << i   << "] = "   << busy_[i] << " " 
                  << cur_val  << " " << aver_val << " "      << cur_val;
      }

      // if(WorkloadType::which_workload == myTestSet::YCSB){
      //   balance_master(aver_val, threshold);  
      // } else {
        // balance_master_tpcc(aver_val, threshold);  
      // }
      LOG(INFO) << " after: ";
      for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
        LOG(INFO) <<" busy[" << i << "] = " << busy_[i];
      }
    } else if(real_distribute_num > 0) {
      // pass
    } else {
      return -1;
    }

    // if(replica_cur_val > replica_threshold && context.random_router == 0){
    //   // balance_replica(node_replica_busy, replica_cur_val);
    // }



    schedule_meta.reorder_done.store(true);
    }


    while(schedule_meta.reorder_done.load() == false){
      std::this_thread::sleep_for(std::chrono::microseconds(5));
    }
    cur_timestamp__ = std::chrono::duration_cast<std::chrono::microseconds>(
                 std::chrono::steady_clock::now() - staart)
                 .count() * 1.0 / 1000 ;
    LOG(INFO) << "scheduler + reorder : " << cur_timestamp__ << " " << cur_val << " " << aver_val << " " << threshold;

    return cnt;
  }

  void find_imbalanced_node(std::unordered_map<size_t, int>& overload_node,
                            std::unordered_map<size_t, int>& idle_node,
                            std::unordered_map<size_t, long long>& busy,
                            long long aver_val,
                            long long threshold
                            ){
    int overloaded_id = -1;
    int overloaded_num = 0;

    int idle_id = -1;
    int idle_num = 0;

    for(size_t j = 0 ; j < context.coordinator_num; j ++ ){
      // if((long long) (busy[j] - aver_val) * (busy[j] - aver_val) > threshold){
        if((busy[j] - aver_val) > 0){
          if(busy[j] - aver_val > overloaded_num){
            overloaded_num = busy[j] - aver_val;
            overloaded_id = j;
          }
          // overload_node[j] = busy[j] - aver_val;
        } else {
          if(aver_val - busy[j]  > idle_num){
            idle_num = aver_val - busy[j];
            idle_id = j;
          }
        } 
      // }
    }    
    if(overloaded_id != -1){
      overload_node[overloaded_id] = overloaded_num;
    }
    if(idle_id != -1){
      idle_node[idle_id] = idle_num;
    }
  }

  long long cal_load_distribute(int aver_val, 
                          std::unordered_map<size_t, long long>& busy_){
    long long cur_val = 0;
    for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      // 
      cur_val += (aver_val - busy_[i]) * (aver_val - busy_[i]);
    }
    cur_val /= context.coordinator_num;

    return cur_val;
  }
  

  std::pair<int, int> find_idle_node(const std::unordered_map<size_t, int>& idle_node,
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
    return std::make_pair(idle_coord_id, min_cost);
  }

  void balance_master_tpcc(long long aver_val, 
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

      Clumps clumps(txns_coord_cost);
      for(size_t i = 0 ; i < sz; i ++ ){
        clumps.AddTxn(txns[i].get());
      }

      bool is_ok = true;
      std::vector<int> used(clumps.Size(), 0);
      std::vector<int> priority = clumps.Sort();

      do {
        // start tradeoff for balancing
        bool useClump = false;
        for(size_t i = 0 ; i < priority.size(); i ++ ){ 
          int cur_idx = priority[i];
          if(used[cur_idx]) continue;
          auto& cur = clumps.At(cur_idx);

          std::unordered_map<size_t, int> overload_node;
          std::unordered_map<size_t, int> idle_node;

          find_imbalanced_node(overload_node, idle_node, 
                              busy, aver_val, threshold);
          if(overload_node.size() == 0 || idle_node.size() == 0){
            is_ok = false;
            break;
          }
          // std::shared_ptr<simpleTransaction> t = q_.top();
          // q_.pop();

          if(overload_node.count(cur.dest)){
            // find minial cost in idle_node
            auto [idle_coord_id, min_cost] = cur.CalIdleNodes(idle_node, 1);
            if(idle_coord_id == -1){
              continue;
            }
            used[cur_idx] = true;
            useClump = true;
            // minus busy
            busy[cur.dest] -= cur.hot;
            overload_node[cur.dest] -= cur.hot;
            if(overload_node[cur.dest] <= 0){
              overload_node.erase(cur.dest);
            }
            // 
            cur.UpdateDest(idle_coord_id);
            // 
            busy[idle_coord_id] += cur.hot;
            idle_node[idle_coord_id] -= cur.hot;
            if(idle_node[idle_coord_id] <= 0){
              idle_node.erase(idle_coord_id);
            }
            // LOG(INFO) << " after move : " << cur.hot;
            // for(size_t j = 0 ; j < context.coordinator_num; j ++ ){
            //   LOG(INFO) <<" busy[" << j << "] = " << busy[j];
            // }
          }
          if(cal_load_distribute(aver_val, busy) <= threshold) {
            break;
          }
          if(overload_node.size() == 0 || idle_node.size() == 0){
            break;
          }

        }
        if(!useClump){
          is_ok = false;
        }
      } while(cal_load_distribute(aver_val, busy) > threshold && is_ok);
  }

  void balance_master(long long aver_val, 
                      long long threshold){

      auto & txns              = schedule_meta.node_txns;
      auto & busy              = schedule_meta.node_busy;
      // auto & node_replica_busy = schedule_meta.node_replica_busy;
      auto & txns_coord_cost   = schedule_meta.txns_coord_cost;

      double cur_timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - start_time)
                  .count() * 1.0 / 1000 / 1000;
      int workload_type = ((int)cur_timestamp / context.workload_time) + 1;
      
      std::priority_queue<simpleTransaction*, 
                        std::vector<simpleTransaction*>, 
                        cmp> q_;
      size_t sz = cur_txn_num * dispatcher_num;
      for(size_t i = 0 ; i < sz; i ++ ){
        if(txns[i].get() == nullptr){
          DCHECK(false);
        }
        q_.push(txns[i].get());
      }
      // start tradeoff for balancing
      int batch_num = 50; // 

      bool is_ok = true;
      do {
        
        for(int i = 0 ; i < batch_num && q_.size() > 0; i ++ ){

          std::unordered_map<size_t, int> overload_node;
          std::unordered_map<size_t, int> idle_node;

          find_imbalanced_node(overload_node, idle_node, 
                               busy, aver_val, threshold);
          if(overload_node.size() == 0 || idle_node.size() == 0){
            is_ok = false;
            break;
          }
          simpleTransaction* t = q_.top();
          q_.pop();

          if(overload_node.count(t->destination_coordinator)){
            // find minial cost in idle_node
            auto [idle_coord_id, min_cost] = find_idle_node(idle_node, 
                                               txns_coord_cost[t->idx_], 
                                               (workload_type == 1 || context.lion_with_metis_init == 0));
            if(idle_coord_id == -1){
              continue;
            }

            // minus busy
            busy[t->destination_coordinator] -= 1;
            if(--overload_node[t->destination_coordinator] <= 0){
              overload_node.erase(t->destination_coordinator);
            }
            t->is_distributed = true;
            // add dest busy
            t->is_real_distributed = true;
            t->destination_coordinator = idle_coord_id;
            busy[t->destination_coordinator] += 1;
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
        
      } while(cal_load_distribute(aver_val, busy) > threshold && is_ok);
  }

  void router_request(std::vector<int>& router_send_txn_cnt, 
                      simpleTransaction* txn, 
                      RouterTxnOps op) {
    // router transaction to coordinators
    size_t coordinator_id_dst = txn->destination_coordinator;

    // messages_mutex[coordinator_id_dst]->lock();
    size_t router_size = ControlMessageFactory::new_router_transaction_message(
        *async_messages[coordinator_id_dst].get(), 0, *txn, 
        op);
    flush_message(async_messages, coordinator_id_dst);
    // messages_mutex[coordinator_id_dst]->unlock();

    // router_send_txn_cnt[coordinator_id_dst]++;
    n_network_size.fetch_add(router_size);
    router_transactions_send.fetch_add(1);
  };


  std::vector<size_t> debug_record_keys_master(std::vector<size_t>& q) {
    std::vector<size_t> query_master;

    for(int i = 0 ; i < q.size(); i ++ ){

      int w_c_id = db.get_dynamic_coordinator_id(context.coordinator_num, 0, 
                                                  (void*)& q[i]);

      query_master.push_back(w_c_id);
    }

    return query_master;
  }

  int clay_router_transmit_request(ShareQueue<std::shared_ptr<myMove<WorkloadType>>>& move_plans){
    // transmit_request_queue
    std::vector<int> router_send_txn_cnt(context.coordinator_num, 0);

    auto new_transmit_generate = [&](int idx){ // int n
      simpleTransaction* s = new simpleTransaction();
      s->idx_ = idx;
      s->is_transmit_request = true;
      s->is_distributed = true;
      // s->partition_id = n;
      return s;
    };


    // 一一对应
    std::vector<simpleTransaction*> metis_txns;
    std::vector<std::shared_ptr<myMove<WorkloadType>>> cur_moves;


    static int transmit_idx = 0; // split into sub-transactions
    static int metis_transmit_idx = 0;

    const int transmit_block_size = 10;

    int cur_move_size = move_plans.size();
    // pack up move-steps to transmit request
    double thresh_ratio = 1;
    for(int i = 0 ; i < thresh_ratio * cur_move_size ; i ++ ){ // 
      bool success = false;
      std::shared_ptr<myMove<WorkloadType>> cur_move;
      
      success = move_plans.pop_no_wait(cur_move);
      DCHECK(success == true);
      
      
      auto metis_new_txn = new_transmit_generate(metis_transmit_idx ++ );
      metis_new_txn->access_frequency = cur_move->access_frequency;
      metis_new_txn->destination_coordinator = cur_move->dest_coordinator_id;

      for(auto move_record: cur_move->records){
          metis_new_txn->keys.push_back(move_record.record_key_);
          metis_new_txn->update.push_back(true);
      }
      //

      metis_txns.push_back(metis_new_txn);
      cur_moves.push_back(cur_move);
      
    }

    // scheduler_transactions(metis_txns, router_send_txn_cnt);

    // pull request
    std::vector<simpleTransaction*> transmit_requests;
    // int64_t coordinator_id_dst = select_best_node(metis_new_txn);
    for(size_t i = 0 ; i < metis_txns.size(); i ++ ){
      // split into sub_transactions
      auto new_txn = new_transmit_generate(transmit_idx ++ );

      for(auto move_record: cur_moves[i]->records){
          new_txn->keys.push_back(move_record.record_key_);
          new_txn->update.push_back(true);
          new_txn->destination_coordinator = metis_txns[i]->destination_coordinator;
          new_txn->is_real_distributed = true; // metis_txns[i]->is_real_distributed;
          new_txn->is_distributed = true;
          new_txn->metis_idx_ = metis_txns[i]->idx_;

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


    move_plans.clear();
    
    for(size_t i = 0 ; i < transmit_requests.size(); i ++ ){ // 
      // transmit_request_queue.push_no_wait(transmit_requests[i]);
      // int64_t coordinator_id_dst = select_best_node(transmit_requests[i]);      
      // LOG(INFO) << "Send MyClay Metis migration transaction ID(" << transmit_requests[i]->idx_ << " " << transmit_requests[i]->metis_idx_ << " " <<  transmit_requests[i]->is_distributed << " " << transmit_requests[i]->keys[0] << " ) to " << transmit_requests[i]->destination_coordinator << "\n";

      router_request(router_send_txn_cnt, transmit_requests[i], RouterTxnOps::TRANSFER);
      // metis_migration_router_request(router_send_txn_cnt, transmit_requests[i]);        
      // if(i > 5){ // debug
      //   break;
      // }
    }
    LOG(INFO) << "OMG!! transmit_requests.size() : " << transmit_requests.size();

    return cur_move_size;
  }


  void start() override {

    LOG(INFO) << "ClaySSMetisGeneratorR " << id << " starts.";
    start_time = std::chrono::steady_clock::now();
    workload.start_time = start_time;
    
    StorageType storage;
    uint64_t last_seed = 0;

    // transaction only commit in a single group

    std::queue<std::unique_ptr<TransactionType>> q;
    std::size_t count = 0;

    // 
    // auto trace_log = std::chrono::steady_clock::now();

    std::unordered_map<int, std::string> map_;
    
    if(context.repartition_strategy == "lion"){
      LOG(INFO) << "lion!!!!!!";
      map_[0] = context.data_src_path_dir + "resultss_partition_30_60.xls";
      map_[1] = context.data_src_path_dir + "resultss_partition_60_90.xls";
      map_[2] = context.data_src_path_dir + "resultss_partition_90_120.xls";
      map_[3] = context.data_src_path_dir + "resultss_partition_0_30.xls";
    } else if(context.repartition_strategy == "clay"){
      LOG(INFO) << "clay!!!!!!";
      map_[0] = context.data_src_path_dir + "clay_resultss_partition_0_30.xls_0";
      map_[1] = context.data_src_path_dir + "clay_resultss_partition_30_60.xls_0";
      map_[2] = context.data_src_path_dir + "clay_resultss_partition_60_90.xls_0";
      map_[3] = context.data_src_path_dir + "clay_resultss_partition_90_120.xls_0";
    } else if(context.repartition_strategy == "metis"){
      LOG(INFO) << "metis!!!!!!";
      map_[0] = context.data_src_path_dir + "metis_resultss_partition_0_30.xls";
      map_[1] = context.data_src_path_dir + "metis_resultss_partition_30_60.xls";
      map_[2] = context.data_src_path_dir + "metis_resultss_partition_60_90.xls";
      map_[3] = context.data_src_path_dir + "metis_resultss_partition_90_120.xls";
    }

    std::unordered_map<int, std::string> map_2;

    map_2[0] = context.data_src_path_dir + "clay_resultss_partition_0_30.xls_1";
    map_2[1] = context.data_src_path_dir + "clay_resultss_partition_30_60.xls_1";
    map_2[2] = context.data_src_path_dir + "clay_resultss_partition_60_90.xls_1";
    map_2[3] = context.data_src_path_dir + "clay_resultss_partition_90_120.xls_1";


    my_clay = std::make_unique<Clay<WorkloadType>>(context, db, worker_status);

    ExecutorStatus status = static_cast<ExecutorStatus>(worker_status.load());

    int last_timestamp_int = 0;
    int workload_num = 4;
    int total_time = workload_num * context.workload_time;

    auto last_timestamp_ = start_time;
    int trigger_time_interval = context.workload_time * 1000; // unit sec.

    int start_offset = 30 * 1000; // 10 * 1000 * 2; // debug
    // 
    if(context.repartition_strategy == "clay" || context.repartition_strategy == "metis"){
      start_offset = 0; // 30 * 1000 ;
    }

    int cur_workload = 0;

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
    
    last_timestamp_ = std::chrono::steady_clock::now();
    // 

    while(status != ExecutorStatus::EXIT && 
          status != ExecutorStatus::CLEANUP){

      process_request();
      status = static_cast<ExecutorStatus>(worker_status.load());

      auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - last_timestamp_)
                              .count();


      if(last_timestamp_int != 0 && latency < trigger_time_interval){
        std::this_thread::sleep_for(std::chrono::microseconds(5));
        continue;
      }
      if(!context.lion_with_metis_init){
        continue;
      }
      // directly jump into first phase
      begin = std::chrono::steady_clock::now();
      

      migration(map_[cur_workload % workload_num]);

      last_timestamp_ = begin;
      last_timestamp_int += trigger_time_interval;
      begin = std::chrono::steady_clock::now();

      cur_workload = (cur_workload + 1) ;
      
      // if(context.lion_with_metis_init){
      //   break; // debug
      // }
    }

    while(status != ExecutorStatus::EXIT && 
          status != ExecutorStatus::CLEANUP){

      process_request();
      status = static_cast<ExecutorStatus>(worker_status.load());

      auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - last_timestamp_)
                              .count();

      if(last_timestamp_int != 0 && latency < trigger_time_interval){
        std::this_thread::sleep_for(std::chrono::microseconds(5));
        continue;
      }
    }
    LOG(INFO) << "transmiter " << " exits.";

      while(status != ExecutorStatus::EXIT && status != ExecutorStatus::CLEANUP){
        process_request();
        status = static_cast<ExecutorStatus>(worker_status.load());
      }
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

  int transmit_idx = 0; // split into sub-transactions
  int metis_transmit_idx = 0;

  DatabaseType &db;
  const ContextType &context;
  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;

  clayss::ScheduleMeta &schedule_meta;

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

  ShareQueue<simpleTransaction> router_transactions_queue;
  ShareQueue<simpleTransaction> migration_transactions_queue;

  std::deque<int> router_stop_queue;

  std::vector<
      std::function<void(MessagePiece, Message &, DatabaseType &, const ContextType &,  Partitioner *, TransactionType *)>>
      messageHandlers;      

  std::vector<
    std::function<void(MessagePiece, Message &, DatabaseType &, ShareQueue<simpleTransaction>* ,std::deque<int>* )>>
    controlMessageHandlers;    

  std::vector<std::size_t> message_stats, message_sizes;
  LockfreeQueue<Message *, 100860> in_queue, out_queue;

  std::ofstream outfile_excel;
  std::mutex out_;

  std::chrono::steady_clock::time_point begin;

  int dispatcher_num;// = 1;
  int dispatcher_id;// = 0;
  int cur_txn_num;//  = context.batch_size / dispatcher_num; 
  uint64_t global_id = 0;
};
} // namespace group_commit

} // namespace star