//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include <chrono>

#include "common/Percentile.h"
#include "core/Manager.h"

#include <iostream>
#include <map>
namespace star {


struct Node {

  int degree;
  int on_same_coordi;
};

class StarManager : public star::Manager {
public:
  using base_type = star::Manager;

  StarManager(std::size_t coordinator_id, std::size_t id,
              const Context &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag) {

    batch_size = context.batch_size;
  }

  ExecutorStatus merge_value_to_signal(uint32_t value, ExecutorStatus signal) {
    // the value is put into the most significant 24 bits
    uint32_t offset = 8;
    return static_cast<ExecutorStatus>((value << offset) |
                                       static_cast<uint32_t>(signal));
  }

  std::tuple<uint32_t, ExecutorStatus> split_signal(ExecutorStatus signal) {
    // the value is put into the most significant 24 bits
    uint32_t offset = 8, mask = 0xff;
    uint32_t value = static_cast<uint32_t>(signal);
    // return value and ``real" signal
    return std::make_tuple(value >> offset,
                           static_cast<ExecutorStatus>(value & mask));
  }

  void update_batch_size(uint64_t running_time) {
    // running_time in microseconds
    // context.group_time in ms
    batch_size = batch_size * (context.group_time * 1000) / running_time;

    if (batch_size % 10 != 0) {
      batch_size += (10 - batch_size % 10);
    }
  }

  void signal_worker(ExecutorStatus status) {

    // only the coordinator node calls this function
    DCHECK(coordinator_id == 0);
    std::tuple<uint32_t, ExecutorStatus> split = split_signal(status);
    set_worker_status(std::get<1>(split));

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
  void record_txn_appearance() {
    /**
     * @brief 统计txn涉及的record关联度
     */

    auto key_size = sizeof(int32_t);
    while(!txn_queue.empty()){
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
        get_txn_related_record_set(record_keys, stringPiece);
        update_record_degree_set(record_keys);


      }
    }
  }
  void coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    Percentile<int64_t> all_percentile, c_percentile, s_percentile,
        batch_size_percentile;

    while (!stopFlag.load()) {

      int64_t ack_wait_time_c = 0, ack_wait_time_s = 0;
      auto c_start = std::chrono::steady_clock::now();
      // start c-phase
      LOG(INFO) << "start C-Phase";

      n_completed_workers.store(0);
      n_started_workers.store(0);
      batch_size_percentile.add(batch_size);
      signal_worker(merge_value_to_signal(batch_size, ExecutorStatus::C_PHASE));
      wait_all_workers_start();
      wait_all_workers_finish();
      set_worker_status(ExecutorStatus::STOP);
      broadcast_stop();
      wait4_ack();

      {
        auto now = std::chrono::steady_clock::now();
        c_percentile.add(
            std::chrono::duration_cast<std::chrono::microseconds>(now - c_start)
                .count());
      }
      // 统计txn_queue的情况
      record_txn_appearance();

      // 
      auto s_start = std::chrono::steady_clock::now();
      // start s-phase

      LOG(INFO) << "start S-Phase";

      n_completed_workers.store(0);
      n_started_workers.store(0);
      signal_worker(ExecutorStatus::S_PHASE);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      wait4_ack();
      {
        auto now = std::chrono::steady_clock::now();

        s_percentile.add(
            std::chrono::duration_cast<std::chrono::microseconds>(now - s_start)
                .count());

        auto all_time =
            std::chrono::duration_cast<std::chrono::microseconds>(now - c_start)
                .count();

        all_percentile.add(all_time);
        if (context.star_dynamic_batch_size) {
          update_batch_size(all_time);
        }
      }
      record_txn_appearance();
    }
    // // for debug
    show_for_degree_set();
    
    signal_worker(ExecutorStatus::EXIT);

    LOG(INFO) << "Average phase switch length " << all_percentile.nth(50)
              << " us, average c phase length " << c_percentile.nth(50)
              << " us, average s phase length " << s_percentile.nth(50)
              << " us, average batch size " << batch_size_percentile.nth(50)
              << " .";
  }

  void non_coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    for (;;) {

      ExecutorStatus signal;
      std::tie(batch_size, signal) = split_signal(wait4_signal());

      if (signal == ExecutorStatus::EXIT) {
        set_worker_status(ExecutorStatus::EXIT);
        break;
      }

      // LOG(INFO) << "start C-Phase";

      // start c-phase

      DCHECK(signal == ExecutorStatus::C_PHASE);
      n_completed_workers.store(0);
      n_started_workers.store(0);
      set_worker_status(ExecutorStatus::C_PHASE);
      wait_all_workers_start();
      wait4_stop(1);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      send_ack();

      // 统计txn_queue的情况
      record_txn_appearance();

      // LOG(INFO) << "start S-Phase";

      // start s-phase

      signal = wait4_signal();
      DCHECK(signal == ExecutorStatus::S_PHASE);
      n_completed_workers.store(0);
      n_started_workers.store(0);
      set_worker_status(ExecutorStatus::S_PHASE);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      send_ack();

      record_txn_appearance();
    }
  }

public:
  uint32_t batch_size;

  std::map<int32_t, std::map<int32_t, Node>> record_degree;

private:
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
      record_keys.push_back(k);
    }
    sort(record_keys.begin(), record_keys.end());
  }


  void update_record_degree_set(const std::vector<int32_t>& record_keys){
    /**
     * @brief 更新权重
    */

    for(size_t i = 0; i < record_keys.size(); i ++ ){
      std::map<int32_t, std::map<int32_t, Node>>::iterator it;
      int32_t key_one = record_keys[i];

      it = record_degree.find(key_one);
      if (it == record_degree.end()){
        std::map<int32_t, Node> tmp;
        record_degree.insert(std::make_pair(key_one, tmp));
        it = record_degree.find(key_one);
      }
      for(size_t j = i + 1; j < record_keys.size(); j ++ ){
        std::map<int32_t, Node>::iterator itt;
        int32_t key_two = record_keys[j];

        itt = it->second.find(key_two);
        if (itt == it->second.end()){
          Node n;
          n.degree = 0; 
          n.on_same_coordi = 0; // context.getPartitionID(key_one) == context.getPartitionID(key_two); // context.;
          it->second.insert(std::pair<int32_t, Node>(key_two, n));
          itt = it->second.find(key_two);
        }
        Node& cur_node = record_degree[key_one][key_two];
        cur_node.degree ++ ;
      }
    }
    return ;
  }

  void show_for_degree_set(){
    /**
     * @brief
    */
    std::ofstream outfile;
	  outfile.open("result.txt");
    for (std::map<int32_t, std::map<int32_t, Node>>::iterator it=record_degree.begin(); 
         it!=record_degree.end(); ++it){
        std::map<int32_t, Node>::iterator itt_begin = it->second.begin();
        std::map<int32_t, Node>::iterator itt_end = it->second.end();
        for(; itt_begin!= itt_end; itt_begin ++ ){
          
          outfile << "[" << it->first << ", " << itt_begin->first << "]: " << itt_begin->second.degree << " " << itt_begin->second.on_same_coordi << "\n";
        }
    }
    outfile.close();
  }
};
} // namespace star