//
// Created by Yi Lu on 9/10/18.
//

#pragma once

#include "core/Context.h"
#include "core/ControlMessage.h"
#include "core/Defs.h"
#include "core/Delay.h"
#include "core/Worker.h"

#include "common/MyMove.h"

#include <iostream>
#include <unordered_map>
#include <unordered_set>

#include <map>
#include <functional>

#include <stdio.h>
#include <stdlib.h>

#include <thread>
#include <glog/logging.h>



namespace star {


template <class Workload> 
class LionRecorder : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using myKeyType = u_int64_t;

  LionRecorder(std::size_t coordinator_id, std::size_t id, const Context &context,
          std::atomic<bool> &stopFlag, DatabaseType& db,
          std::atomic<uint32_t>& recorder_status, 
          std::atomic<uint32_t>& transmit_status,
          std::atomic<uint32_t>& n_completed_workers,
          std::atomic<uint32_t>& n_started_workers)
      : Worker(coordinator_id, id), context(context), stopFlag(stopFlag), db(db),
        recorder_status(recorder_status),
        transmit_status(transmit_status),
        n_completed_workers(n_completed_workers),
        n_started_workers(n_started_workers),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)),
        // s_partitioner(std::make_unique<LionSPartitioner>(
        //     coordinator_id, context.coordinator_num)),
        l_partitioner(std::make_unique<LionDynamicPartitioner<Workload>>(
            context.coordinator_id, context.coordinator_num, db)) {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    
    my_clay = std::make_unique<Clay<WorkloadType>>(record_degree);
    worker_status.store(static_cast<uint32_t>(ExecutorStatus::STOP));

    // 
    if(coordinator_id == 1){
      std::ofstream outfile;
      outfile.open("/Users/lion/project/01_star/star/result_non_con.txt", std::ios::trunc); // ios::
      outfile << "nunu ye!\n";
      outfile.close();
    }
    std::ofstream outfile;
	  outfile.open("/Users/lion/project/01_star/star/result.txt", std::ios::trunc); // ios::trunc
    outfile << "lion is the best\n";
    outfile.close();

    std::ofstream outfile_excel;
	  outfile_excel.open("/Users/lion/project/01_star/star/result.xls", std::ios::trunc); // ios::trunc
    outfile_excel.close();

	  outfile_excel.open("/Users/lion/project/01_star/star/result_moves.xls", std::ios::trunc); // ios::trunc
    outfile_excel.close();
  }

// MoveRecord<WorkloadType> get_move_record(myMove<WorkloadType>& move, const Node& max){
//     size_t ycsbTableID = ycsb::ycsb::tableID;
//     auto partitionFrom = db.getPartitionID(context, ycsbTableID, max.from);
//     auto partitionEnd = db.getPartitionID(context, ycsbTableID, max.to);

//     MoveRecord<WorkloadType> move_record;

//     if(partitionFrom == partitionEnd){
//       move_record.field_size = -1;
//       return move_record;
//     }
//     move.dest_partition_id = partitionEnd;

//     // get ycsb key
//     move_record.key.Y_KEY = max.from;
//     move_record.src_partition_id = partitionFrom;
    
//     int tableId = ycsb::ycsb::tableID;

//     // get ycsb value
//     ITable *src_table = db.find_table(tableId, move_record.src_partition_id);
//     auto value = src_table->search_value(&move_record.key);
//     move_record.value = *((ycsb::ycsb::value*) value);
//     move_record.field_size = src_table->field_size();
//     move_record.key_size = src_table->key_size();

//     return move_record;
// }

void get_value_for_move_record(MoveRecord<WorkloadType>& cur){
    
    ITable *src_table = db.find_table(cur.table_id, cur.src_partition_id);
    
    if(WorkloadType::which_workload == myTestSet::YCSB){
      // 
      auto value = src_table->search_value(&cur.key.ycsb_key);
      cur.value.ycsb_val = *((ycsb::ycsb::value*) value);
    } else if(WorkloadType::which_workload == myTestSet::TPCC){
      switch (cur.table_id)
      {
      case tpcc::warehouse::tableID:{
        auto value = src_table->search_value(&cur.key.w_key);
        cur.value.w_val = *((tpcc::warehouse::value*) value);
        break;
      }
      case tpcc::district::tableID:{
        auto value = src_table->search_value(&cur.key.d_key);
        cur.value.d_val = *((tpcc::district::value*) value);
        break;
      }
      case tpcc::customer::tableID:{
        auto value = src_table->search_value(&cur.key.c_key);
        cur.value.c_val = *((tpcc::customer::value*) value);
        break;
      }
      case tpcc::stock::tableID:{
        auto value = src_table->search_value(&cur.key.s_key);
        cur.value.s_val = *((tpcc::stock::value*) value);
        break;
      }
      default:
        DCHECK(false);
        break;
      }
    } else {
      DCHECK(false);
    }
  
    return ;
}

bool prepare_for_transmit_clay(std::vector<myMove<WorkloadType> >& moves, 
                               std::vector<myMove<WorkloadType> >& moves_merged) {
    /**
     * @brief 找最大的策略... 最简单的策略
     * @note 单个move 大小不要超过30！ 单个message <= 4k
     * 
     */
    size_t message_key_threshold = 30;

    std::map<int32_t, int32_t> hash_for_index; // <dest_partition, index in moves_merged>
    std::map<int32_t, std::set<int32_t>> moves_key_unique; 

    for(auto itt = moves.begin(); itt != moves.end(); itt ++) {
      myMove<WorkloadType>& move = *itt;

      // 检查move是否为到本地，并取出其value
      for(auto it = move.records.begin(); it != move.records.end(); ){
        // 本地到本地，就不用迁移了
        if(it->src_partition_id == move.dest_partition_id){
          move.records.erase(it);
        } else {
          get_value_for_move_record(*it);
          // *it = get_move_record(*(T*)& it->key, table_id, it->src_partition_id);
          it ++ ;
        } 
      }
      if(move.records.empty()){
        continue;
      }
      //  src 和 dest 的放到一个move里，准备merge
      if(hash_for_index.find(move.dest_partition_id) == hash_for_index.end()){
        // have not inserted 
        hash_for_index.insert(std::make_pair(move.dest_partition_id, int32_t(moves_merged.size())));
        myMove<WorkloadType> tmp;
        tmp.dest_partition_id = move.dest_partition_id;
        moves_merged.push_back(tmp);
      }
      
      // 
      for(auto it = move.records.begin(); it != move.records.end();){
        int32_t index_ = hash_for_index[move.dest_partition_id];

        if(moves_merged[index_].records.size() < message_key_threshold){
          moves_merged[index_].records.push_back(*it);
          it ++ ;
        } else {
          // 超过阈值，拆分成多个
          auto old_it = hash_for_index.find(move.dest_partition_id);
          hash_for_index.erase(old_it);
          // 新建映射关系
          hash_for_index.insert(std::make_pair(move.dest_partition_id, int32_t(moves_merged.size())));
          myMove<WorkloadType> tmp;
          tmp.dest_partition_id = move.dest_partition_id;
          moves_merged.push_back(tmp);
        }
      }
      
      

    }

    for(size_t i = 0 ; i < moves_merged.size() ; i ++ ){
        myMove<WorkloadType>& move = moves_merged[i];
        if(!move.records.empty()){
          move_in_history.push_back(move); // For Debug
        }
        break;
    }


    return true;
  }
  
  void transmit_record(const myMove<WorkloadType>& move){
    //!TODO: 应该将tableID通过msg传过来
    
    

    for(size_t i = 0 ; i < move.records.size(); i ++ ){

      const auto & rec = move.records[i];
      ITable *src_table = db.find_table(move.records[i].table_id, rec.src_partition_id);
      ITable *dest_table =  db.find_table(move.records[i].table_id, move.dest_partition_id);
      if(WorkloadType::which_workload == myTestSet::YCSB){
        ycsb::ycsb::key ycsb_keys = rec.key.ycsb_key;
        ycsb::ycsb::value ycsb_value = rec.value.ycsb_val;

        // auto value = src_table->search_value(&ycsb_keys);
        // ycsb::ycsb::value ycsb_value = *((ycsb::ycsb::value*) value );
        // 
        // LOG(INFO) << *(int*)&(move.records[i].key) << "  " << move.records[i].src_partition_id << " " << move.dest_partition_id;
        dest_table->insert(&ycsb_keys, &ycsb_value);
        src_table->delete_(&ycsb_keys);
      } else {
        switch (move.records[i].table_id)
        {
        case tpcc::warehouse::tableID:
          dest_table->insert(&rec.key.w_key, &rec.value.w_val);
          src_table->delete_(&rec.key.w_key);
          break;
        case tpcc::district::tableID:
          dest_table->insert(&rec.key.d_key, &rec.value.d_val);
          src_table->delete_(&rec.key.d_key);
          break;
        case tpcc::customer::tableID:
          dest_table->insert(&rec.key.c_key, &rec.value.c_val);
          src_table->delete_(&rec.key.c_key);
          break;
        case tpcc::stock::tableID:
          dest_table->insert(&rec.key.s_key, &rec.value.s_val);
          src_table->delete_(&rec.key.s_key);
          break;
        default:
          DCHECK(false);
          break;
        }
      }

    }

    // if(coordinator_id == 0) {
    //   std::ofstream outfile;
    //   outfile.open("result.txt", std::ios::app); // ios::trunc

    //   for(size_t i = 0 ; i < limit; i ++ ){
        
    //     outfile << i << "  " << *(int32_t*)& move.records[i].key << " " << move.records[i].src_partition_id << " -> " << move.dest_partition_id << "\n";

    //   }
    //   outfile.close();
    // }


    
    
  }

  void signal_recorder(const myMove<WorkloadType>& move) {
    // only the coordinator node calls this function
    DCHECK(coordinator_id == 0);

    // signal to everyone
    for (auto i = 0u; i < context.coordinator_num; i++) {
      if (i == coordinator_id) {
        continue;
      }
      //!TODO 只发给有partition_id的副本
      myMove<WorkloadType> cur_move;
      cur_move.dest_partition_id = move.dest_partition_id;

      for(size_t j = 0 ; j < move.records.size(); j ++ ){
        if(l_partitioner->is_partition_replicated_on(move.records[j].src_partition_id, i)){
          cur_move.records.push_back(move.records[j]);
        } else if (l_partitioner->is_partition_replicated_on(move.dest_partition_id, i)) {
          // partition在副本上
          cur_move.records.push_back(move.records[j]);
        }
      }
      // LOG(INFO) << "to " << i; 
      ControlMessageFactory::new_transmit_message(*messages[i], cur_move);
    }
    flush_messages();
  }

  void wait_recorder_worker_transimit_start(){
    /**
     * @brief 
     * 
     */
    while(transmit_status.load() == static_cast<int32_t>(ExecutorStatus::STOP)){
      if(stopFlag.load()){
        break;
      }
      std::this_thread::yield();
    }
  }
  void coordinator_start()  {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    Percentile<int64_t> all_percentile, 
        clumpping_percentile, local_percentile, remote_percentile, 
        batch_size_percentile;

    while (!stopFlag.load()) {
      
      int64_t ack_wait_time_c = 0, ack_wait_time_s = 0;
      auto now = std::chrono::steady_clock::now();

      if(recorder_status.load() == static_cast<int32_t>(ExecutorStatus::START)){
        // 触发了数据迁移
        auto c_start = std::chrono::steady_clock::now();
        auto cur_head = c_start;

        LOG(INFO) << "start Transfroming!";
        // do data transforming 
        std::vector<myMove<WorkloadType> >  moves, moves_merged;
        // find the moves (only keys)
        // show_for_degree_set();

        my_clay->find_clump(moves);
        my_clay->reset();
        
        {
          auto now = std::chrono::steady_clock::now();
          auto all_time =
              std::chrono::duration_cast<std::chrono::microseconds>(now - cur_head)
                  .count();
          cur_head = now;
          clumpping_percentile.add(all_time);
        }

        bool is_ready = false;
        //
        wait_recorder_worker_transimit_start();

        if(moves.size() != 0) {
          // find the exact values for the key 
          is_ready = prepare_for_transmit_clay(moves, moves_merged);
         
          if(is_ready == true && moves_merged.size() > 0) {
            for(size_t cur_move = 0; cur_move < moves_merged.size(); cur_move ++ ){
              myMove<WorkloadType>& move = moves_merged[cur_move];
              DCHECK(move.records.size() > 0);
              // 全副本节点开始准备迁移

              // LOG(INFO) << "start transmit_record: " << cur_move; 
              transmit_record(move);

              // LOG(INFO) << "signal_recorder: " << cur_move; 

              // 部分副本节点开始准备replicate
              signal_recorder(move);
              // LOG(INFO) << "singnal_done: " << cur_move; 

            }
            ////// for debug 
            // for(int i = 0 ; i < 12; i ++ ){
            //   ITable *dest_table = db.find_table(ycsb::ycsb::tableID, i);
            //   LOG(INFO) << "TABLE [" << i << "]: " << dest_table->table_record_num();
            // }
           
            {
              auto now = std::chrono::steady_clock::now();
              auto all_time =
                  std::chrono::duration_cast<std::chrono::microseconds>(now - cur_head)
                      .count();
              cur_head = now;
              local_percentile.add(all_time);
            }
            LOG(INFO) << "RECORDER wait4_ack"; 
            wait4_ack(moves_merged.size());
            LOG(INFO) << "CONTINUE wait4_ack"; 
            {
              auto now = std::chrono::steady_clock::now();
              auto all_time =
                  std::chrono::duration_cast<std::chrono::microseconds>(now - cur_head)
                      .count();
              cur_head = now;
              remote_percentile.add(all_time);
            }
          }
        }
        



        {
          auto now = std::chrono::steady_clock::now();
          auto all_time =
              std::chrono::duration_cast<std::chrono::microseconds>(now - c_start)
                  .count();

          all_percentile.add(all_time);
        }
        // finish data transforming
        LOG(INFO) << "finish data Transfroming!";
        record_degree.clear();
        // txn_queue.clear();

        recorder_status.store(static_cast<int32_t>(ExecutorStatus::STOP));
        transmit_status.store(static_cast<int32_t>(ExecutorStatus::STOP));
      }
      else {
        auto record_start = std::chrono::steady_clock::now();
        // 防止 lock-free queue一直被占用

        while(txn_queue.empty()){
          txn_queue.nop_pause();
          if(recorder_status.load() == static_cast<int32_t>(ExecutorStatus::START) || 
             stopFlag.load() ){
               break;
          }
        }
        if(recorder_status.load() == static_cast<int32_t>(ExecutorStatus::START)){
          continue; 
        } else if(stopFlag.load()){
          break;
        }
        // LOG(INFO) << "txn_queue: " << txn_queue.read_available();
        record_txn_appearance();

      }
    }
    // for(int i = 0 ; i < 12; i ++ ){
    //   ITable *dest_table = db.find_table(ycsb::ycsb::tableID, i);
    //   LOG(INFO) << "TABLE [" << i << "]: " << dest_table->table_record_num();
    // }
    recorder_status.store(static_cast<int32_t>(ExecutorStatus::STOP));
    broadcast_stop();

    LOG(INFO) << "Average data-transforming length " << all_percentile.nth(50)
              << " us, average clumpping length " << clumpping_percentile.nth(50) 
              << " us, average local length " << local_percentile.nth(50) 
              << " us, average remote length " << remote_percentile.nth(50) 
              // << " us, average s phase length " << s_percentile.nth(50)
              // << " us, average batch size " << batch_size_percentile.nth(50)
              << "us .";
  }
  void remove_on_secondary_coordinator(const myMove<WorkloadType>& move) {
    /**
     * @brief 注意当前是c—phase
     * 
     */
    std::ofstream outfile;
	  // outfile.open("/Users/lion/project/01_star/star/result_non_con_.txt", std::ios::app); // ios::trunc
    // static int move_round = 1;
    // outfile << "move round" << move_round ++  << " -------------- \n";

    // if(1){
    //   outfile << "hahaa\n";
    //   for(int i = 0 ; i < 1200000; i ++ ){
    //     std::set<int32_t> id = db.getPartitionIDs(context, i);
    //     if(id.size() > 1){
    //       outfile << i << " abnormal \n";
    //       for(auto it = id.begin(); it != id.end(); it ++ ){
    //         outfile << *it << " ";
    //       }
    //       outfile << "\n";
    //       outfile.flush();
    //     }
    //   }
    // }
    // outfile.close();

    // return;

    // outfile.open("/Users/lion/project/01_star/star/result_non_con.txt", std::ios::app); // ios::trunc

    // insert 
    if(l_partitioner->is_partition_replicated_on(move.dest_partition_id, coordinator_id)){
      // 说明partition 在这个上面
      // outfile << "move round" << move_round << " -------------- \n";
      for(size_t i = 0 ; i < move.records.size(); i ++ ){
        const auto& rec = move.records[i]; 
        ITable *dest_table = db.find_table(rec.table_id, move.dest_partition_id);

        DCHECK(move.dest_partition_id != move.records[i].src_partition_id);
        // find and insert it  
        // outfile << "Insert [ " << *(int32_t*)& move.records[i].key.ycsb_key << "] " << move.records[i].src_partition_id << " -> " 
        // << move.dest_partition_id << "\n";
        // outfile.flush();
        if(WorkloadType::which_workload == myTestSet::YCSB){
          ycsb::ycsb::key ycsb_keys = rec.key.ycsb_key;
          ycsb::ycsb::value ycsb_value = rec.value.ycsb_val;
          std::set<int32_t> id = db.getPartitionIDs(context, move.records[i].key.ycsb_key);
          // for(auto it = id.begin(); it != id.end(); it ++ ){
          //   outfile << *it << " ";
          // }
          // outfile << "\n";
          // outfile.flush();
          // if(id.find(move.dest_partition_id) == id.end()){
          dest_table->insert(&ycsb_keys, &ycsb_value);
          // } else {
            // 挺神奇的...
          // }
          // src_table->delete_(&ycsb_keys);
        } else {
          switch (move.records[i].table_id)
          {
          case tpcc::warehouse::tableID:
            dest_table->insert(&rec.key.w_key, &rec.value.w_val);
            // src_table->delete_(&rec.key.w_key);
            break;
          case tpcc::district::tableID:
            dest_table->insert(&rec.key.d_key, &rec.value.d_val);
            // src_table->delete_(&rec.key.d_key);
            break;
          case tpcc::customer::tableID:
            dest_table->insert(&rec.key.c_key, &rec.value.c_val);
            // src_table->delete_(&rec.key.c_key);
            break;
          case tpcc::stock::tableID:
            dest_table->insert(&rec.key.s_key, &rec.value.s_val);
            // src_table->delete_(&rec.key.s_key);
            break;
          default:
            DCHECK(false);
            break;
          }
        }
        // outfile << "ok" << "\n";

        // dest_table->insert(&move.records[i].key, &move.records[i].value);
        // // std::set<int32_t> id = db.getPartitionIDs(context, *(int32_t*)& move.records[i].key);
        // if(id.find(move.dest_partition_id) == id.end()){
        //   dest_table->insert(&move.records[i].key, &move.records[i].value);
        //   // LOG(INFO) << "INSERT " << *(int*)& move.records[i].key << "-> P" << move.dest_partition_id; 
        // } else {
        //   // LOG(INFO) << "ERROR";
        // }
      }
    } 

    for(size_t i = 0 ; i < move.records.size(); i ++ ){
      const auto& rec = move.records[i];
      // remove
      if(l_partitioner->is_partition_replicated_on(rec.src_partition_id, coordinator_id)){
        // 说明partition 在这个上面
        ITable *src_table = db.find_table(rec.table_id, rec.src_partition_id);
        // ycsb::ycsb::key ycsb_keys = move.records[i].key;
        // src_table->delete_(&ycsb_keys);
        // LOG(INFO) << "DELETE " << *(int*)& move.records[i].key << "x P" << move.records[i].src_partition_id; 
        // find and insert it  
        // outfile << "Delete [ " << *(int32_t*)& move.records[i].key.ycsb_key << "] " << move.records[i].src_partition_id << " -> " 
        // << move.dest_partition_id << "\n";

        if(WorkloadType::which_workload == myTestSet::YCSB){
          ycsb::ycsb::key ycsb_keys = rec.key.ycsb_key;
          ycsb::ycsb::value ycsb_value = rec.value.ycsb_val;
          // dest_table->insert(&ycsb_keys, &ycsb_value);
          src_table->delete_(&ycsb_keys);
        } else {
          switch (move.records[i].table_id)
          {
          case tpcc::warehouse::tableID:
            // dest_table->insert(&rec.key.w_key, &rec.value.w_val);
            src_table->delete_(&rec.key.w_key);
            break;
          case tpcc::district::tableID:
            // dest_table->insert(&rec.key.d_key, &rec.value.d_val);
            src_table->delete_(&rec.key.d_key);
            break;
          case tpcc::customer::tableID:
            // dest_table->insert(&rec.key.c_key, &rec.value.c_val);
            src_table->delete_(&rec.key.c_key);
            break;
          case tpcc::stock::tableID:
            // dest_table->insert(&rec.key.s_key, &rec.value.s_val);
            src_table->delete_(&rec.key.s_key);
            break;
          default:
            DCHECK(false);
            break;
          }
        }

        // outfile << "ok" << "\n";
      } 
    }
    // outfile.close();
  }

  void non_coordinator_start()  {
    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;
  // std::ofstream outfile;
  // outfile.open("/Users/lion/project/01_star/star/result_non_con_.txt", std::ios::trunc); // ios::trunc
  // outfile.close();

    while (true) { 
      if(wait4_stop_nonblock()){
        break;
      }
      
      // 
      // std::this_thread::yield();
      myMove<WorkloadType> move;
      // 防止退不出来
      bool has_move = wait4_move(move);
      if(has_move == false){
        move_queue.nop_pause();// std::this_thread::yield();
        continue;
      }
      // 
      // show_for_moves(move);

 //     LOG(INFO) << "move comes!";
      if(move.records.size() > 0){
        remove_on_secondary_coordinator(move);
      }
      // for(int i = 0 ; i < 12; i ++ ){
      //   if(l_partitioner->is_partition_replicated_on(i, coordinator_id)) {
      //     ITable *dest_table = db.find_table(ycsb::ycsb::tableID, i);
      //     LOG(INFO) << "TABLE [" << i << "]: " << dest_table->table_record_num();
      //   }
      // }
//      LOG(INFO) << "send_ack!";
      send_ack();

    }

    
    // for(int i = 0 ; i < 12; i ++ ){
    //   if(l_partitioner->is_partition_replicated_on(i, coordinator_id)) {
    //     ITable *dest_table = db.find_table(ycsb::ycsb::tableID, i);
    //     LOG(INFO) << "TABLE [" << i << "]: " << dest_table->table_record_num();
    //   }
    // }
    // ExecutorStatus status = wait4_signal();
    // DCHECK(status == ExecutorStatus::START);
    // n_completed_workers.store(0);
    // n_started_workers.store(0);
    // set_worker_status(ExecutorStatus::START);
    // wait_all_workers_start();
    // wait4_stop(1);
    // set_worker_status(ExecutorStatus::STOP);
    // wait_all_workers_finish();
    // broadcast_stop();
    // wait4_stop(n_coordinators - 2);
    // process replication
    // n_completed_workers.store(0);
    // set_worker_status(ExecutorStatus::CLEANUP);
    // wait_all_workers_finish();
    // send_ack();
  }

  void wait_all_workers_finish() {
    std::size_t n_workers = context.worker_num;
    // wait for all workers to finish
    while (n_completed_workers.load() < n_workers) {
      // change to nop_pause()?
      std::this_thread::yield();
    }
  }

  void wait_all_workers_start() {
    std::size_t n_workers = context.worker_num;
    // wait for all workers to finish
    while (n_started_workers.load() < n_workers) {
      // change to nop_pause()?
      std::this_thread::yield();
    }
  }

  void set_worker_status(ExecutorStatus status) {
    worker_status.store(static_cast<uint32_t>(status));
  }

  void signal_worker(ExecutorStatus status) {

    // only the coordinator node calls this function
    DCHECK(coordinator_id == 0);
    set_worker_status(status);

    // signal to everyone
    for (auto i = 0u; i < context.coordinator_num; i++) {
      if (i == coordinator_id) {
        continue;
      }
      ControlMessageFactory::new_signal_message(*messages[i],
                                                static_cast<uint32_t>(status));
    }
    flush_messages();
  }

  ExecutorStatus wait4_signal() {
    // only non-coordinator calls this function
    DCHECK(coordinator_id != 0);

    signal_in_queue.wait_till_non_empty();

    std::unique_ptr<Message> message(signal_in_queue.front());
    bool ok = signal_in_queue.pop();
    CHECK(ok);

    CHECK(message->get_message_count() == 1);

    MessagePiece messagePiece = *(message->begin());
    auto type = static_cast<ControlMessage>(messagePiece.get_message_type());
    CHECK(type == ControlMessage::SIGNAL);

    uint32_t status;
    StringPiece stringPiece = messagePiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> status;

    return static_cast<ExecutorStatus>(status);
  }

  void wait4_stop(std::size_t n) {

    // wait for n stop messages

    for (auto i = 0u; i < n; i++) {

      stop_in_queue.wait_till_non_empty();

      std::unique_ptr<Message> message(stop_in_queue.front());
      bool ok = stop_in_queue.pop();
      CHECK(ok);

      CHECK(message->get_message_count() == 1);

      MessagePiece messagePiece = *(message->begin());
      auto type = static_cast<ControlMessage>(messagePiece.get_message_type());
      CHECK(type == ControlMessage::STOP);
    }
  }
  bool wait4_stop_nonblock(){
    if(stop_in_queue.empty()){
      stop_in_queue.nop_pause();
      return false;
    } else {
      return true;
    }
  }
  void wait4_ack(size_t num = 1) {

    std::chrono::steady_clock::time_point start;

    // only coordinator waits for ack
    DCHECK(coordinator_id == 0);

    std::size_t n_coordinators = context.coordinator_num;

    for (auto i = 0u; i < n_coordinators - 1; i++) {
      for(size_t j = 0 ; j < num; j ++ ){
        ack_in_queue.wait_till_non_empty();

        std::unique_ptr<Message> message(ack_in_queue.front());
        bool ok = ack_in_queue.pop();
        CHECK(ok);

        CHECK(message->get_message_count() == 1);

        MessagePiece messagePiece = *(message->begin());
        auto type = static_cast<ControlMessage>(messagePiece.get_message_type());
        CHECK(type == ControlMessage::ACK);
      }
    }
  }

  void get_move_decode(myMove<WorkloadType>& move, star::StringPiece& stringPiece){
    /**
     * @brief 
    auto cal_message_length = [&](const myMove<WorkloadType>& move){
      auto message_size = MessagePiece::get_header_size() + 
                          key_size + // len of keys
                          key_size;  // dest partition
      for(size_t i = 0 ; i < move.size(); i ++ ){
        // 
        message_size += key_size +   // src partition
                        key_size +   // key
                        key_size +   // field size
                        move[i].field_size; // field value
      }

      return message_size;
    };
    */
    auto normal_size = sizeof(int32_t);
    auto key_size = sizeof(u_int64_t);

    const void *total_len_size = stringPiece.data();
    const auto &total_len = *static_cast<const int32_t *>(total_len_size);
    stringPiece.remove_prefix(normal_size);

    if(total_len == 0)
      return;

    const void *dest_partition_id = stringPiece.data(); 
    const auto &dest_partition_id_ = *static_cast<const int32_t *>(dest_partition_id);
    move.dest_partition_id = dest_partition_id_;
    stringPiece.remove_prefix(normal_size);

    // 单个的key
    for(int32_t i = 0 ; i < total_len; i ++ ){
      MoveRecord<WorkloadType> move_rec;
      move_rec.key_size = key_size;

      /** 解压得到key的source_partition_id */
      const void *src_partition_id_ptr = stringPiece.data();  
      const auto &src_partition_id = *static_cast<const int32_t *>(src_partition_id_ptr);  
      stringPiece.remove_prefix(normal_size);
      move_rec.src_partition_id = src_partition_id;

      /** 解压得到单个的key */
      const void *record_key = stringPiece.data();  
      const auto &k = *static_cast<const myKeyType *>(record_key);  
      stringPiece.remove_prefix(key_size);
      move_rec.set_real_key(k);

      /** get */
      const void *field_size_ = stringPiece.data();
      const auto &field_size = *static_cast<const int32_t *>(field_size_);
      stringPiece.remove_prefix(normal_size);
      move_rec.field_size = field_size;

      /** key 的value */
      const void *record_value = stringPiece.data();
      move_rec.set_real_value(k, record_value);
      // const auto &v = *static_cast<const ValueType *>(record_value);  
      stringPiece.remove_prefix(field_size);
      move.records.push_back(move_rec);
    }

    // Decoder dec(stringPiece);
    // dec >> move.dest_partition_id;
    return;
  }

  bool wait4_move(myMove<WorkloadType>& move) {
    std::chrono::steady_clock::time_point start;

    // only secondary-coordinator waits for ack
    DCHECK(coordinator_id != 0);

    if(move_queue.empty()){
      return false;
    } // wait_till_non_empty()

    std::unique_ptr<Message> message(move_queue.front());
    bool ok = move_queue.pop();
    CHECK(ok);

    CHECK(message->get_message_count() == 1);

    MessagePiece messagePiece = *(message->begin());
    auto type = static_cast<ControlMessage>(messagePiece.get_message_type());
    CHECK(type == ControlMessage::TRANSMIT);

    auto stringPiece = messagePiece.toStringPiece();
   // LOG(INFO) << "decode!";
    get_move_decode(move, stringPiece);

    return true;
  }

  void send_stop(std::size_t node_id) {

    DCHECK(node_id != coordinator_id);

    ControlMessageFactory::new_stop_message(*messages[node_id]);

    flush_messages();
  }

  void broadcast_stop() {

    std::size_t n_coordinators = context.coordinator_num;

    for (auto i = 0u; i < n_coordinators; i++) {
      if (i == coordinator_id)
        continue;
      ControlMessageFactory::new_stop_message(*messages[i]);
    }

    flush_messages();
  }

  void send_ack() {

    // only non-coordinator calls this function
    DCHECK(coordinator_id != 0);

    ControlMessageFactory::new_ack_message(*messages[0]);
    flush_messages();
  }

  void start() override {

    if (coordinator_id == 0) {
      LOG(INFO) << "Recorder(worker id = " << id
                << ") on the coordinator node started.";
      coordinator_start();
      LOG(INFO) << "Recorder(worker id = " << id
                << ") on the coordinator node exits.";
    } else {
      LOG(INFO) << "Recorder(worker id = " << id
                << ") on the non-coordinator node started.";
      non_coordinator_start();
      LOG(INFO) << "Recorder(worker id = " << id
                << ") on the non-coordinator node exits.";
    }
  }

  void push_message(Message *message) override {

    // message will only be of type signal, COUNT
    MessagePiece messagePiece = *(message->begin());

    auto message_type =
      static_cast<ControlMessage>(messagePiece.get_message_type());

    if (message_type != ControlMessage::COUNT){
      CHECK(message->get_message_count() == 1);
      switch (message_type) {
      case ControlMessage::SIGNAL:
        signal_in_queue.push(message);
        break;
      case ControlMessage::ACK:
        ack_in_queue.push(message);
        break;
      case ControlMessage::STOP:
        stop_in_queue.push(message);
        break;
      case ControlMessage::TRANSMIT:
        // LOG(INFO) << "TRANSMIT" << id;
        move_queue.push(message);
        break;
      default:
        CHECK(false) << "Message type: " << static_cast<uint32_t>(message_type) << " " << messagePiece.get_message_length();
        break;
      }
    } else {
      // if(recorder_status.load() == static_cast<int32_t>(ExecutorStatus::STOP)){
      txn_queue.push(message);
      // }
    }
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
  void flush_messages() {

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

  void init_message(Message *message, std::size_t dest_node_id) {
    message->set_source_node_id(coordinator_id);
    message->set_dest_node_id(dest_node_id);
    message->set_worker_id(id);
  }


private:
  template<typename T = u_int64_t>
  void record_txn_appearance() {
    /**
     * @brief 统计txn涉及的record关联度
     */

    auto key_size = sizeof(T);
    auto handle_size = txn_queue.read_available();
    size_t i = 0;
    while(i ++ < handle_size){ //  !txn_queue.empty()){ // 
      std::unique_ptr<Message> message(txn_queue.front());
      bool ok = txn_queue.pop();
      CHECK(ok);
      auto iter = message->begin();

      for (; iter != message->end(); iter ++ ){
        MessagePiece messagePiece = *(iter);
        auto message_type =
        static_cast<ControlMessage>(messagePiece.get_message_type());
        CHECK(message_type == ControlMessage::COUNT);

        auto stringPiece = messagePiece.toStringPiece();

        std::vector<T> record_keys;
        // 提前退出
        if(recorder_status.load() == static_cast<int32_t>(ExecutorStatus::START) || stopFlag.load()){
          return;
        }
        // 
        get_txn_related_record_set(record_keys, stringPiece);

        update_record_degree_set(record_keys);
      }
    }
  }
  template<typename T = u_int64_t>
  void get_txn_related_record_set(std::vector<T>& record_keys, star::StringPiece& stringPiece){
    /**
     * @brief record_keys 有序的
    */
    auto key_size = sizeof(T);
    auto normal_size = sizeof(int32_t);

    const void *total_len_size = stringPiece.data();
    const auto &total_len = *static_cast<const int32_t *>(total_len_size);
    stringPiece.remove_prefix(normal_size);
    
    // 单个的key
    for(int32_t i = 0 ; i < total_len; i ++ ){
      /** 解压得到单个的key */
      const void *record_key = stringPiece.data();    
      stringPiece.remove_prefix(key_size);

      const auto &k = *static_cast<const T *>(record_key);
      record_keys.emplace_back(k);
    }
    sort(record_keys.begin(), record_keys.end());
  }


  int32_t get_partition_id_for_tpcc(const MoveRecord<WorkloadType>& r_k){
    int32_t ret = -1;
    switch (r_k.table_id)
    {
    case tpcc::warehouse::tableID:
      ret = db.getPartitionID(context, r_k.table_id, r_k.key.w_key);
      break;
    case tpcc::district::tableID:
      ret = db.getPartitionID(context, r_k.table_id, r_k.key.d_key);
      break;
    case tpcc::customer::tableID:
      ret = db.getPartitionID(context, r_k.table_id, r_k.key.c_key);
      break;
    case tpcc::stock::tableID:
      ret = db.getPartitionID(context, r_k.table_id, r_k.key.s_key);
      break;
    }
    return ret;
  }


  template<typename T = u_int64_t> 
  void update_record_degree_set(const std::vector<T>& record_keys){
    /**
     * @brief 更新权重
     * @param record_keys 递增的key
     * @note 双向图
    */
    auto select_tpcc = [&](u_int64_t key){
      // only transform STOCK_TABLE
      bool is_jumped = false;
      if(WorkloadType::which_workload == myTestSet::TPCC){
        int32_t table_id = key >> RECORD_COUNT_TABLE_ID_OFFSET;
        if(table_id != tpcc::stock::tableID){
          is_jumped = true;
        }
      }
      return is_jumped;
    };

    for(size_t i = 0; i < record_keys.size(); i ++ ){
      std::unordered_map<myKeyType, std::unordered_map<myKeyType, Node>>::iterator it;
      auto key_one = record_keys[i];
      if(select_tpcc(key_one)){
        continue;
      }
      it = record_degree.find(key_one);

      MoveRecord<WorkloadType> key_one_real;
      if(WorkloadType::which_workload == myTestSet::TPCC){
        key_one_real.set_real_key(key_one); 
      }
      
      if (it == record_degree.end()){
        // [key_one -> [key_two, Node]]
        std::unordered_map<myKeyType, Node> tmp;
        record_degree.insert(std::make_pair(key_one, tmp));
        it = record_degree.find(key_one);
      }
      for(size_t j = 0; j < record_keys.size(); j ++ ){
        // j = i + 1
        if(j == i)
          continue;
        std::unordered_map<myKeyType, Node>::iterator itt;
        auto key_two = record_keys[j];
        if(select_tpcc(key_two)){
          continue;
        }
        itt = it->second.find(key_two);

        if (itt == it->second.end()){
          // [key_one -> [key_two, Node]]
          Node n;
          n.degree = 0; 

          int32_t key_one_table_id = key_one >> RECORD_COUNT_TABLE_ID_OFFSET;
          int32_t key_two_table_id = key_two >> RECORD_COUNT_TABLE_ID_OFFSET;
          if(WorkloadType::which_workload == myTestSet::YCSB){
            n.from_p_id = db.getPartitionID(context, key_one_table_id, key_one);
            n.to_p_id = db.getPartitionID(context, key_two_table_id, key_two);
          } else if(WorkloadType::which_workload == myTestSet::TPCC){

            MoveRecord<WorkloadType> key_two_real;
            key_two_real.set_real_key(key_two);

            n.from_p_id = get_partition_id_for_tpcc(key_one_real); // ;db.getPartitionID(context, key_one_table_id, key_one_real);
            n.to_p_id = get_partition_id_for_tpcc(key_two_real); // db.getPartitionID(context, key_two_table_id, key_two_real);
          } else {
            DCHECK(false);
          }

          n.on_same_coordi = n.from_p_id == n.to_p_id; // context.;

          it->second.insert(std::pair<T, Node>(key_two, n));
          itt = it->second.find(key_two);
        }

        Node& cur_node = record_degree[key_one][key_two];
        cur_node.from = key_one; 
        cur_node.to = key_two;
        cur_node.degree += (cur_node.on_same_coordi == 1? 1: 50);

        my_clay->update_hottest_edge(myTuple(cur_node.from, cur_node.from_p_id, cur_node.degree));
        my_clay->update_hottest_edge(myTuple(cur_node.to, cur_node.to_p_id, cur_node.degree));

        my_clay->update_load_partition(cur_node);      
      }

      my_clay->update_hottest_tuple(key_one);

    
    }
    return ;
  }

  void show_for_degree_set(){
    /**
     * @brief
    */
   using myKeyType = u_int64_t;
    if(WorkloadType::which_workload == myTestSet::YCSB){
    std::ofstream outfile;
	  outfile.open("/Users/lion/project/01_star/star/result.txt", std::ios::app); // ios::trunc
    for (std::unordered_map<myKeyType, std::unordered_map<myKeyType, Node>>::iterator it=record_degree.begin(); 
         it!=record_degree.end(); ++it){
        std::unordered_map<myKeyType, Node>::iterator itt_begin = it->second.begin();
        std::unordered_map<myKeyType, Node>::iterator itt_end = it->second.end();
        for(; itt_begin!= itt_end; itt_begin ++ ){

          outfile << "[" << it->first << ", " << itt_begin->first << "]: " << itt_begin->second.degree << " [ "<< itt_begin->second.from_p_id << "->" << itt_begin->second.to_p_id <<"] single partition =" << itt_begin->second.on_same_coordi << "\n";
        }
    }
    outfile << "-----------------------\n";

    for(size_t i = 0; i < move_in_history.size(); i ++ ){
      i = move_in_history.size() - 1;
      myMove<WorkloadType>& cur = move_in_history[i];
      outfile << "move " << i << " -> P" << cur.dest_partition_id << "\n";
      for(size_t j = 0 ; j < cur.records.size(); j ++ ){
        MoveRecord<WorkloadType>& cur_rec = cur.records[j];
        outfile << "   " << *(int32_t*)&cur_rec.key << " from " << cur_rec.src_partition_id << "\n"; 
      }
    }
    outfile.close();



    std::ofstream outfile_excel;
	  outfile_excel.open("/Users/lion/project/01_star/star/result.xls", std::ios::app); // ios::trunc

    static int round = 0;
    if(round == 0){
      outfile_excel << "from" << "\t" << "to" << "\t" << "degree" << "\t"<< "from_p_id" << "\t" << "to_p_id" <<"\t" << "is_on_same" << "\t" << "round" << "\n";
    }
    round ++ ;
    for (std::unordered_map<myKeyType, std::unordered_map<myKeyType, Node>>::iterator it=record_degree.begin(); 
         it!=record_degree.end(); ++it){
        std::unordered_map<myKeyType, Node>::iterator itt_begin = it->second.begin();
        std::unordered_map<myKeyType, Node>::iterator itt_end = it->second.end();
        for(; itt_begin!= itt_end; itt_begin ++ ){

          outfile_excel << it->first << "\t" << itt_begin->first << "\t" << itt_begin->second.degree << "\t"<< itt_begin->second.from_p_id << "\t" << itt_begin->second.to_p_id <<"\t" << itt_begin->second.on_same_coordi << "\t" << round << "\n";
        }
    }
    outfile_excel.close();

    } else {
    
    std::ofstream outfile_excel;
	  outfile_excel.open("/Users/lion/project/01_star/star/result.xls", std::ios::app); // ios::trunc

    static int round = 0;
    if(round == 0){
      outfile_excel << 
      "f table_id" << "\t" << "f w_id" << "\t" << "f d_id" << "\t" << "f c_id" << "\t" << "f s_id" << "\t" <<
      "t table_id" << "\t" << "t w_id" << "\t" << "t d_id" << "\t" << "t c_id" << "\t" << "t s_id" << "\t" <<
      "degree" << "\t"<< "from_p_id" << "\t" << "to_p_id" <<"\t" << "is_on_same" << "\t" << "round" << "\n";
    }
    round ++ ;
    for (std::unordered_map<myKeyType, std::unordered_map<myKeyType, Node>>::iterator it=record_degree.begin(); 
         it!=record_degree.end(); ++it){
        std::unordered_map<myKeyType, Node>::iterator itt_begin = it->second.begin();
        std::unordered_map<myKeyType, Node>::iterator itt_end = it->second.end();
        
   
        for(; itt_begin!= itt_end; itt_begin ++ ){
        u_int64_t record_key = it->first;
        int32_t f_table_id = (record_key >> RECORD_COUNT_TABLE_ID_OFFSET);

        int32_t f_w_id = (record_key & RECORD_COUNT_W_ID_VALID) >> RECORD_COUNT_W_ID_OFFSET;
        int32_t f_d_id = (record_key & RECORD_COUNT_D_ID_VALID) >> RECORD_COUNT_D_ID_OFFSET;
        int32_t f_c_id = (record_key & RECORD_COUNT_C_ID_VALID) >> RECORD_COUNT_C_ID_OFFSET;
        int32_t f_s_id = (record_key & RECORD_COUNT_OL_ID_VALID);

        record_key = itt_begin->first;
        int32_t t_table_id = (record_key >> RECORD_COUNT_TABLE_ID_OFFSET);

        int32_t t_w_id = (record_key & RECORD_COUNT_W_ID_VALID) >> RECORD_COUNT_W_ID_OFFSET;
        int32_t t_d_id = (record_key & RECORD_COUNT_D_ID_VALID) >> RECORD_COUNT_D_ID_OFFSET;
        int32_t t_c_id = (record_key & RECORD_COUNT_C_ID_VALID) >> RECORD_COUNT_C_ID_OFFSET;
        int32_t t_s_id = (record_key & RECORD_COUNT_OL_ID_VALID);

          outfile_excel <<
          f_table_id << "\t" << f_w_id << "\t" << f_d_id << "\t" << f_c_id << "\t" << f_s_id << "\t" <<
          t_table_id << "\t" << t_w_id << "\t" << t_d_id << "\t" << t_c_id << "\t" << t_s_id << "\t" <<
           itt_begin->second.degree << "\t"<< itt_begin->second.from_p_id << "\t" << itt_begin->second.to_p_id <<"\t" << itt_begin->second.on_same_coordi << "\t" << round << "\n";
        }
    }
    outfile_excel.close();
    }

  }
  void show_for_moves(const myMove<WorkloadType>& move){
    std::ofstream outfile_excel;
	  outfile_excel.open("/Users/lion/project/01_star/star/result_moves.xls", std::ios::app); // ios::trunc

    static int rounds = 0;
    if(rounds == 0){
      outfile_excel << 
      "f table_id" << "\t" << "f w_id" << "\t" << "f d_id" << "\t" << "f c_id" << "\t" << "f s_id" << "\t" <<
      "from_p_id" << "\t"<< "to_p_id" << "\t" << "round" << "\n";
    }
    rounds ++ ;

    auto dest = move.dest_partition_id;
    for(auto i: move.records){
        u_int64_t record_key = i.record_key_;
        int32_t f_table_id = (record_key >> RECORD_COUNT_TABLE_ID_OFFSET);

        int32_t f_w_id = (record_key & RECORD_COUNT_W_ID_VALID) >> RECORD_COUNT_W_ID_OFFSET;
        int32_t f_d_id = (record_key & RECORD_COUNT_D_ID_VALID) >> RECORD_COUNT_D_ID_OFFSET;
        int32_t f_c_id = (record_key & RECORD_COUNT_C_ID_VALID) >> RECORD_COUNT_C_ID_OFFSET;
        int32_t f_s_id = (record_key & RECORD_COUNT_OL_ID_VALID);

      outfile_excel << 
      f_table_id << "\t" << f_w_id << "\t" << f_d_id << "\t" << f_c_id << "\t" << f_s_id << "\t" <<
      i.src_partition_id << "\t"<< dest << "\t" << rounds << "\n";

    }
    outfile_excel.close();
  }
private:
  std::vector<myMove<WorkloadType>> move_in_history;

protected:
  const Context &context;
  std::atomic<bool> &stopFlag;
  LockfreeQueue<Message *> ack_in_queue, signal_in_queue, stop_in_queue,
      out_queue, 
      move_queue;
  LockfreeQueue<Message *, 1<<15 >     txn_queue; // 记录事务的queue
  std::vector<std::unique_ptr<Message>> messages;

public:
  DatabaseType& db;
  
  std::atomic<uint32_t>& recorder_status;
  std::atomic<uint32_t>& transmit_status;
  std::atomic<uint32_t> worker_status;

  std::atomic<uint32_t>& n_completed_workers;
  std::atomic<uint32_t>& n_started_workers;
  std::unique_ptr<Delay> delay;  

  std::unordered_map<myKeyType, std::unordered_map<myKeyType, Node>> record_degree;
  std::unique_ptr<Partitioner> l_partitioner; //  s_partitioner,

  std::unique_ptr<Clay<WorkloadType> > my_clay;
};

} // namespace star
