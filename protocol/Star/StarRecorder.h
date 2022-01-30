//
// Created by Yi Lu on 9/10/18.
//

#pragma once

#include "core/Context.h"
#include "core/ControlMessage.h"
#include "core/Defs.h"
#include "core/Delay.h"
#include "core/Worker.h"

#include <iostream>
#include <map>

#include <thread>

namespace star {

struct Node {
  int degree;
  int on_same_coordi;
};

template <class Workload> class StarRecorder : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;

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
            coordinator_id, context.coordinator_num, context.delay_time)) {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    worker_status.store(static_cast<uint32_t>(ExecutorStatus::STOP));
  }

  void coordinator_start()  {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    Percentile<int64_t> all_percentile, c_percentile, s_percentile,
        batch_size_percentile;

    // wait_all_workers_start();

    while (!stopFlag.load()) {
      
      int64_t ack_wait_time_c = 0, ack_wait_time_s = 0;
      
      if(recorder_status.load() == 1){
        auto c_start = std::chrono::steady_clock::now();
        LOG(INFO) << "start Transfroming!";
        // do data transforming 
        std::this_thread::sleep_for(std::chrono::seconds(1));
        

        auto now = std::chrono::steady_clock::now();
        {
          auto all_time =
              std::chrono::duration_cast<std::chrono::microseconds>(now - c_start)
                  .count();

          c_percentile.add(all_time);
        }
        // finish data transforming
        recorder_status.store(0);
      }

        // LOG(INFO) << "Record: "<< id << " " << 

        // ack_in_queue.read_available() << " " << signal_in_queue.read_available() << " " << stop_in_queue.read_available() << " "
        // << out_queue.read_available() << " " << txn_queue.read_available();

      //LOG(INFO) << "start recording!";
      auto record_start = std::chrono::steady_clock::now();
      std::this_thread::sleep_for(std::chrono::seconds(1));
      LOG(INFO) << "txn_queue: " << txn_queue.read_available();
      record_txn_appearance();

      auto now = std::chrono::steady_clock::now();

      {
        auto all_time =
            std::chrono::duration_cast<std::chrono::microseconds>(now - record_start)
                .count();

        all_percentile.add(all_time);
      }
    }
    
    show_for_degree_set();

    // wait_all_workers_finish();
    broadcast_stop();
    // wait4_stop(n_coordinators - 1);
    // process replication
    // n_completed_workers.store(0);
    // set_worker_status(ExecutorStatus::CLEANUP);
    // wait_all_workers_finish();
    // wait4_ack();

    LOG(INFO) << "Average record-update length " << all_percentile.nth(50)
              << " us, average data-transforming length " << c_percentile.nth(50)
              // << " us, average s phase length " << s_percentile.nth(50)
              // << " us, average batch size " << batch_size_percentile.nth(50)
              << " .";
  }

  void non_coordinator_start()  {
    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    // ExecutorStatus status = wait4_signal();
    // DCHECK(status == ExecutorStatus::START);
    // n_completed_workers.store(0);
    // n_started_workers.store(0);
    // set_worker_status(ExecutorStatus::START);
    // wait_all_workers_start();
    wait4_stop(1);
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

  void wait4_ack() {

    std::chrono::steady_clock::time_point start;

    // only coordinator waits for ack
    DCHECK(coordinator_id == 0);

    std::size_t n_coordinators = context.coordinator_num;

    for (auto i = 0u; i < n_coordinators - 1; i++) {

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
      default:
        CHECK(false) << "Message type: " << static_cast<uint32_t>(message_type);
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
    */

    for(size_t i = 0; i < record_keys.size(); i ++ ){
      std::map<int32_t, std::map<int32_t, Node>>::iterator it;
      int32_t key_one = record_keys[i];
      // std::this_thread::sleep_for(std::chrono::seconds(1));
      it = record_degree.find(key_one);
      if (it == record_degree.end()){
        // LOG(INFO) << "NOT FIND " << key_one;
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
          n.on_same_coordi = db.getPartitionID(context, key_one) == db.getPartitionID(context, key_two); // context.;
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


protected:
  const Context &context;
  std::atomic<bool> &stopFlag;
  LockfreeQueue<Message *> ack_in_queue, signal_in_queue, stop_in_queue,
      out_queue;
  LockfreeQueue<Message *, 1<<15 >     txn_queue; // 记录事务的queue
  std::vector<std::unique_ptr<Message>> messages;

public:
  DatabaseType& db;
  
  std::atomic<uint32_t>& recorder_status;
  std::atomic<uint32_t> worker_status;

  std::atomic<uint32_t>& n_completed_workers;
  std::atomic<uint32_t>& n_started_workers;
  std::unique_ptr<Delay> delay;

  std::map<int32_t, std::map<int32_t, Node>> record_degree;

};

} // namespace star
