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

#include <thread>
#include <glog/logging.h>

namespace star {


template <class Workload, typename key_type, typename value_type> 
class StarRecorder : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using KeyType = key_type;
  using ValueType = value_type; 

  StarRecorder(std::size_t coordinator_id, std::size_t id, const Context &context,
          std::atomic<bool> &stopFlag, DatabaseType& db,
          std::atomic<uint32_t>& recorder_status, 
          std::atomic<uint32_t>& n_completed_workers,
          std::atomic<uint32_t>& n_started_workers)
      : Worker(coordinator_id, id), context(context), stopFlag(stopFlag), db(db),
        recorder_status(recorder_status),
        n_completed_workers(n_completed_workers),
        n_started_workers(n_started_workers),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)),
        // s_partitioner(std::make_unique<StarSPartitioner>(
        //     coordinator_id, context.coordinator_num)),
        c_partitioner(std::make_unique<StarCPartitioner>(
            coordinator_id, context.coordinator_num)) {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    
    my_clay = std::make_unique<Clay<KeyType, ValueType>>(record_degree);
    worker_status.store(static_cast<uint32_t>(ExecutorStatus::STOP));

    // 
    std::ofstream outfile;
	  outfile.open("result.txt", std::ios::trunc); // ios::trunc
    outfile << "lion is the best\n";
    outfile.close();

    std::ofstream outfile_excel;
	  outfile_excel.open("result.xls", std::ios::trunc); // ios::trunc
    outfile_excel.close();
  }

MoveRecord<KeyType, ValueType> get_move_record(myMove<KeyType, ValueType>& move, const Node& max){
    auto partitionFrom = db.getPartitionID(context, max.from);
    auto partitionEnd = db.getPartitionID(context,  max.to);


    MoveRecord<ycsb::ycsb::key, ycsb::ycsb::value> move_record;

    if(partitionFrom == partitionEnd){
      move_record.field_size = -1;
      return move_record;
    }
    move.dest_partition_id = partitionEnd;

    // get ycsb key
    move_record.key.Y_KEY = max.from;
    move_record.src_partition_id = partitionFrom;
    
    int tableId = ycsb::ycsb::tableID;

    // get ycsb value
    ITable *src_table = db.find_table(tableId, move_record.src_partition_id);
    auto value = src_table->search_value(&move_record.key);
    move_record.value = *((ycsb::ycsb::value*) value);
    move_record.field_size = src_table->field_size();
    move_record.key_size = src_table->key_size();

    return move_record;
}

MoveRecord<KeyType, ValueType> get_move_record(uint32_t key, int32_t src_partition_id){
    
    MoveRecord<ycsb::ycsb::key, ycsb::ycsb::value> move_record;

    int tableId = ycsb::ycsb::tableID;
    // set ycsb key
    move_record.key.Y_KEY = key;
    move_record.src_partition_id = src_partition_id;
    // get ycsb value
    ITable *src_table = db.find_table(tableId, src_partition_id);
    auto value = src_table->search_value(&move_record.key);
    move_record.value = *((ycsb::ycsb::value*) value);
    move_record.field_size = src_table->field_size();
    move_record.key_size = src_table->key_size();

    return move_record;
}

bool prepare_for_transmit_clay(std::vector<myMove<KeyType, ValueType> >& moves, 
                               std::vector<myMove<KeyType, ValueType> >& moves_merged) {
    /**
     * @brief 找最大的策略... 最简单的策略
     * @note 单个move 大小不要超过30！ 单个message <= 4k
     * 
     */
    size_t message_key_threshold = 30;

    std::map<int32_t, int32_t> hash_for_index;
    std::map<int32_t, std::set<int32_t>> moves_key_unique;

    for(auto itt = moves.begin(); itt != moves.end(); itt ++) {
      myMove<KeyType, ValueType>& move = *itt;

      for(auto it = move.records.begin(); it != move.records.end(); ){
        // 本地到本地，就不用迁移了
        if(it->src_partition_id == move.dest_partition_id){
          move.records.erase(it);
        } else {
          *it = get_move_record(*(int32_t*)& it->key, it->src_partition_id);
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
        myMove<KeyType, ValueType> tmp;
        tmp.dest_partition_id = move.dest_partition_id;
        moves_merged.push_back(tmp);
      }
      
        
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
            myMove<KeyType, ValueType> tmp;
            tmp.dest_partition_id = move.dest_partition_id;
            moves_merged.push_back(tmp);
          }
        }
      
      

    }

    for(size_t i = 0 ; i < moves_merged.size() ; i ++ ){
        myMove<KeyType, ValueType>& move = moves_merged[i];

        if(!move.records.empty()){
          move_in_history.push_back(move); // For Debug
        }
        break;
    }


    return true;
  }
  
  void transmit_record(const myMove<KeyType, ValueType>& move){
    //!TODO: 应该将tableID通过msg传过来
    int tableId = ycsb::ycsb::tableID;
    ITable *dest_table = db.find_table(tableId, move.dest_partition_id);
    size_t limit  = move.records.size(); // > 30 ? 30 : move.records.size();
    for(size_t i = 0 ; i < limit; i ++ ){
      ITable *src_table = db.find_table(tableId, move.records[i].src_partition_id);

      ycsb::ycsb::key ycsb_keys = move.records[i].key;
      ycsb::ycsb::value ycsb_value = move.records[i].value;

      // auto value = src_table->search_value(&ycsb_keys);
      // ycsb::ycsb::value ycsb_value = *((ycsb::ycsb::value*) value );
      // 
      // LOG(INFO) << *(int*)&(move.records[i].key) << "  " << move.records[i].src_partition_id << " " << move.dest_partition_id;
      dest_table->insert(&ycsb_keys, &ycsb_value);
      src_table->delete_(&ycsb_keys);
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

  void signal_recorder(const myMove<KeyType, ValueType>& move) {
    // only the coordinator node calls this function
    DCHECK(coordinator_id == 0);

    // signal to everyone
    for (auto i = 0u; i < context.coordinator_num; i++) {
      if (i == coordinator_id) {
        continue;
      }
      //!TODO 只发给有partition_id的副本
      myMove<KeyType, ValueType> cur_move;
      cur_move.dest_partition_id = move.dest_partition_id;

      for(size_t j = 0 ; j < move.records.size(); j ++ ){
        if(c_partitioner->is_partition_replicated_on(move.records[j].src_partition_id, i)){
          cur_move.records.push_back(move.records[j]);
        } else if (c_partitioner->is_partition_replicated_on(move.dest_partition_id, i)) {
          // partition在副本上
          cur_move.records.push_back(move.records[j]);
        }
      }
      // LOG(INFO) << "to " << i; 
      ControlMessageFactory::new_transmit_message(*messages[i], cur_move);
    }
    flush_messages();
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
        std::vector<myMove<KeyType, ValueType> >  moves, moves_merged;
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
        if(moves.size() != 0) {
          
          //!TODO 先只迁移一个move
          is_ready = prepare_for_transmit_clay(moves, moves_merged);
         

          if(is_ready == true && moves_merged.size() > 0) {
            for(size_t cur_move = 0; cur_move < moves_merged.size(); cur_move ++ ){
              myMove<KeyType, ValueType>& move = moves_merged[cur_move];
              DCHECK(move.records.size() > 0);
              // 全副本节点开始准备迁移
              // LOG(INFO) << cur_move ;
              transmit_record(move);

              // 部分副本节点开始准备replicate
              signal_recorder(move);
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
        txn_queue.clear();

        recorder_status.store(static_cast<int32_t>(ExecutorStatus::STOP));
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
  void remove_on_secondary_coordinator(const myMove<KeyType, ValueType>& move) {
    /**
     * @brief 注意当前是c—phase
     * 
     */

    int tableId = ycsb::ycsb::tableID;
    // 
    // TODO

    // insert 
    if(c_partitioner->is_partition_replicated_on(move.dest_partition_id, coordinator_id)){
      // 说明partition 在这个上面
      ITable *dest_table = db.find_table(tableId, move.dest_partition_id);

      for(size_t i = 0 ; i < move.records.size(); i ++ ){
        DCHECK(move.dest_partition_id != move.records[i].src_partition_id);

        std::set<int32_t> id = db.getPartitionIDs(context, *(int32_t*)& move.records[i].key);
        if(id.find(move.dest_partition_id) == id.end()){
          dest_table->insert(&move.records[i].key, &move.records[i].value);
          // LOG(INFO) << "INSERT " << *(int*)& move.records[i].key << "-> P" << move.dest_partition_id; 
        } else {
          // LOG(INFO) << "ERROR";
        }
      }
    } 

    for(size_t i = 0 ; i < move.records.size(); i ++ ){
      // remove
      if(c_partitioner->is_partition_replicated_on(move.records[i].src_partition_id, coordinator_id)){
        // 说明partition 在这个上面
        ITable *src_table = db.find_table(tableId, move.records[i].src_partition_id);
        ycsb::ycsb::key ycsb_keys = move.records[i].key;
        src_table->delete_(&ycsb_keys);
        // LOG(INFO) << "DELETE " << *(int*)& move.records[i].key << "x P" << move.records[i].src_partition_id; 

      } 
    }

  }
  void non_coordinator_start()  {
    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;
    
    while (true) { 
      if(wait4_stop_nonblock()){
        break;
      }
      
      // 
      // std::this_thread::yield();
      myMove<KeyType, ValueType> move;
      // 防止退不出来
      bool has_move = wait4_move(move);
      if(has_move == false){
        move_queue.nop_pause();// std::this_thread::yield();
        continue;
      }
      // 
      // LOG(INFO) << "move comes!";
      if(move.records.size() > 0){
        remove_on_secondary_coordinator(move);
      }
      // for(int i = 0 ; i < 12; i ++ ){
      //   if(c_partitioner->is_partition_replicated_on(i, coordinator_id)) {
      //     ITable *dest_table = db.find_table(ycsb::ycsb::tableID, i);
      //     LOG(INFO) << "TABLE [" << i << "]: " << dest_table->table_record_num();
      //   }
      // }
      // LOG(INFO) << "send_ack!";
      send_ack();

    }

    
    // for(int i = 0 ; i < 12; i ++ ){
    //   if(c_partitioner->is_partition_replicated_on(i, coordinator_id)) {
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

  void get_move_decode(myMove<KeyType, ValueType>& move, star::StringPiece& stringPiece){
    /**
     * @brief 
     *  key_size
     *  int[key_size]
     *  src_partition
     *  dst_partition 
    */
    auto key_size = sizeof(int32_t);

    const void *total_len_size = stringPiece.data();
    const auto &total_len = *static_cast<const int32_t *>(total_len_size);
    stringPiece.remove_prefix(key_size);

    if(total_len == 0)
      return;

    const void *field_size_ = stringPiece.data();
    const auto &field_size = *static_cast<const int32_t *>(field_size_);
    stringPiece.remove_prefix(key_size);

    // 单个的key
    for(int32_t i = 0 ; i < total_len; i ++ ){
      MoveRecord<KeyType, ValueType> move_rec;
      move_rec.key_size = key_size;
      move_rec.field_size = field_size;

      /** 解压得到key的source_partition_id */
      const void *src_partition_id_ptr = stringPiece.data();  
      const auto &src_partition_id = *static_cast<const int32_t *>(src_partition_id_ptr);  
      stringPiece.remove_prefix(key_size);
      move_rec.src_partition_id = src_partition_id;

      /** 解压得到单个的key */
      const void *record_key = stringPiece.data();  
      const auto &k = *static_cast<const KeyType *>(record_key);  
      stringPiece.remove_prefix(key_size);
      move_rec.key = k;

      /** key 的value */
      const void *record_value = stringPiece.data();
      const auto &v = *static_cast<const ValueType *>(record_value);  
      stringPiece.remove_prefix(field_size);
      move_rec.value = v;
      // Decoder dec(valueStringPiece);
      // dec >> move_rec.value;
      move.records.push_back(move_rec);
    }

    const void *dest_partition_id = stringPiece.data();    
    stringPiece.remove_prefix(key_size);
    move.dest_partition_id = *static_cast<const int32_t *>(dest_partition_id);
    // Decoder dec(stringPiece);
    // dec >> move.dest_partition_id;
    return;
  }

  bool wait4_move(myMove<KeyType, ValueType>& move) {
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
      txn_queue.push(message);
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

  void record_txn_appearance() {
    /**
     * @brief 统计txn涉及的record关联度
     */

    auto key_size = sizeof(int32_t);
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

        std::vector<int32_t> record_keys;
        // 提前退出
        if(recorder_status.load() == static_cast<int32_t>(ExecutorStatus::START) || stopFlag.load()){
          return;
        }
        get_txn_related_record_set(record_keys, stringPiece);
        update_record_degree_set(record_keys);


      }
    }
  }

  void get_txn_related_record_set(std::vector<int32_t>& record_keys, star::StringPiece& stringPiece){
    /**
     * @brief record_keys 有序的
    */
    auto key_size = sizeof(int32_t);

    const void *total_len_size = stringPiece.data();
    const auto &total_len = *static_cast<const int32_t *>(total_len_size);

    stringPiece.remove_prefix(key_size);
    // 单个的key
    for(int32_t i = 0 ; i < total_len; i ++ ){
      /** 解压得到单个的key */
      const void *record_key = stringPiece.data();    
      stringPiece.remove_prefix(key_size);

      const auto &k = *static_cast<const int32_t *>(record_key);
      record_keys.emplace_back(k);
    }
    sort(record_keys.begin(), record_keys.end());
  }


  void update_record_degree_set(const std::vector<int32_t>& record_keys){
    /**
     * @brief 更新权重
     * @param record_keys 递增的key
     * @note 双向图
    */

    for(size_t i = 0; i < record_keys.size(); i ++ ){
      std::unordered_map<int32_t, std::unordered_map<int32_t, Node>>::iterator it;
      int32_t key_one = record_keys[i];

      it = record_degree.find(key_one);
      if (it == record_degree.end()){
        // [key_one -> [key_two, Node]]
        std::unordered_map<int32_t, Node> tmp;
        record_degree.insert(std::make_pair(key_one, tmp));
        it = record_degree.find(key_one);
      }
      for(size_t j = 0; j < record_keys.size(); j ++ ){
        // j = i + 1
        if(j == i)
          continue;
        std::unordered_map<int32_t, Node>::iterator itt;
        int32_t key_two = record_keys[j];

        itt = it->second.find(key_two);
        if (itt == it->second.end()){
          // [key_one -> [key_two, Node]]
          Node n;
          n.degree = 0; 

          n.from_p_id = db.getPartitionID(context, key_one);
          n.to_p_id = db.getPartitionID(context, key_two);

          n.on_same_coordi = n.from_p_id == n.to_p_id; // context.;

          it->second.insert(std::pair<int32_t, Node>(key_two, n));
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
    std::ofstream outfile;
	  outfile.open("result.txt", std::ios::app); // ios::trunc
    for (std::unordered_map<int32_t, std::unordered_map<int32_t, Node>>::iterator it=record_degree.begin(); 
         it!=record_degree.end(); ++it){
        std::unordered_map<int32_t, Node>::iterator itt_begin = it->second.begin();
        std::unordered_map<int32_t, Node>::iterator itt_end = it->second.end();
        for(; itt_begin!= itt_end; itt_begin ++ ){

          outfile << "[" << it->first << ", " << itt_begin->first << "]: " << itt_begin->second.degree << " [ "<< itt_begin->second.from_p_id << "->" << itt_begin->second.to_p_id <<"] single partition =" << itt_begin->second.on_same_coordi << "\n";
        }
    }
    outfile << "-----------------------\n";

    for(size_t i = 0; i < move_in_history.size(); i ++ ){
      i = move_in_history.size() - 1;
      myMove<KeyType, ValueType>& cur = move_in_history[i];
      outfile << "move " << i << " -> P" << cur.dest_partition_id << "\n";
      for(size_t j = 0 ; j < cur.records.size(); j ++ ){
        MoveRecord<KeyType, ValueType>& cur_rec = cur.records[j];
        outfile << "   " << *(int32_t*)&cur_rec.key << " from " << cur_rec.src_partition_id << "\n"; 
      }
    }
    outfile.close();



    std::ofstream outfile_excel;
	  outfile_excel.open("result.xls", std::ios::app); // ios::trunc

    static int round = 0;
    if(round == 0){
      outfile_excel << "from" << "\t" << "to" << "\t" << "degree" << "\t"<< "from_p_id" << "\t" << "to_p_id" <<"\t" << "is_on_same" << "\t" << "round" << "\n";
    }
    round ++ ;
    for (std::unordered_map<int32_t, std::unordered_map<int32_t, Node>>::iterator it=record_degree.begin(); 
         it!=record_degree.end(); ++it){
        std::unordered_map<int32_t, Node>::iterator itt_begin = it->second.begin();
        std::unordered_map<int32_t, Node>::iterator itt_end = it->second.end();
        for(; itt_begin!= itt_end; itt_begin ++ ){

          outfile_excel << it->first << "\t" << itt_begin->first << "\t" << itt_begin->second.degree << "\t"<< itt_begin->second.from_p_id << "\t" << itt_begin->second.to_p_id <<"\t" << itt_begin->second.on_same_coordi << "\t" << round << "\n";
        }
    }
    outfile_excel.close();

  }

private:
  std::vector<myMove<KeyType, ValueType>> move_in_history;

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
  std::atomic<uint32_t> worker_status;

  std::atomic<uint32_t>& n_completed_workers;
  std::atomic<uint32_t>& n_started_workers;
  std::unique_ptr<Delay> delay;  

  std::unordered_map<int32_t, std::unordered_map<int32_t, Node>> record_degree;
  std::unique_ptr<Partitioner> c_partitioner; //  s_partitioner,

  std::unique_ptr<Clay<KeyType, ValueType>> my_clay;
};

} // namespace star
