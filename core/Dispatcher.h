//
// Created by Yi Lu on 8/29/18.
//

#pragma once

#include "common/BufferedReader.h"
#include "common/LockfreeQueue.h"
#include "common/Message.h"
#include "common/Socket.h"
#include "core/ControlMessage.h"
#include "protocol/Lion/LionMessage.h"

#include "core/Worker.h"
#include <atomic>
#include <glog/logging.h>
#include <thread>
#include <vector>

// #include "protocol/Hermes/HermesMessage.h"
namespace star {
class IncomingDispatcher {

public:
  IncomingDispatcher(std::size_t id, std::size_t group_id,
                     std::size_t io_thread_num, 
                     std::vector<std::string> &peers_ip, 
                     std::vector<Socket> &sockets,
                     const std::vector<std::shared_ptr<Worker>> &workers,
                     LockfreeQueue<Message *> &coordinator_queue,
                     std::atomic<bool> &stopFlag)
      : id(id), group_id(group_id), io_thread_num(io_thread_num),
        network_size(0), peers_ip(peers_ip), workers(workers), coordinator_queue(coordinator_queue),
        stopFlag(stopFlag) {

    for (auto i = 0u; i < sockets.size(); i++) {
      buffered_readers.emplace_back(sockets[i]);
    }
  }

  void start() {
    auto numCoordinators = buffered_readers.size();
    auto numWorkers = workers.size();

    // single node test mode
    if (numCoordinators == 1) {
      return;
    }

    LOG(INFO) << "Incoming Dispatcher started, numCoordinators = "
              << numCoordinators << ", numWorkers = " << numWorkers
              << ", group id = " << group_id;

    while (!stopFlag.load()) {

      for (auto i = 0u; i < numCoordinators; i++) {
        if (i == id) {
          continue;
        }

        auto message = buffered_readers[i].next_message();

        if (message == nullptr) {
          std::this_thread::yield();
          continue;
        }

        network_size += message->get_message_length();

        // check coordinator message
        if (is_coordinator_message(message.get())) {
          coordinator_queue.push(message.release());
          CHECK(group_id == 0);
          continue;
        }
        
        // if (is_record_txn_message_for_recorder(message.get())){
        //   // 最后一个是manager
        //   workers[numWorkers - 1]->push_message(message.release());
        //   continue;
        // }

        auto workerId = message->get_worker_id();
        CHECK(workerId % io_thread_num == group_id);
        // release the unique ptr

      //  LOG(INFO) << i << " get message : " << (*(message->begin())).get_message_type() << " from " << i << " workId: " << workerId;

        workers[workerId]->push_message(message.release());
        DCHECK(message == nullptr);
      }
    }

    LOG(INFO) << "Incoming Dispatcher exits, network size: " << network_size;
  }

  bool is_coordinator_message(Message *message) {
    return (*(message->begin())).get_message_type() ==
           static_cast<uint32_t>(ControlMessage::STATISTICS);
  }

  // bool is_transaction_message(Message *message) {
  //   return (*(message->begin())).get_message_type() ==
  //          static_cast<uint32_t>(HermesMessage::TRANSFER_REQUEST);
  // }

  bool is_record_txn_message_for_recorder(Message *message) {
    // worker -> recorder 的 COUNT message
    uint32_t tmp = (*(message->begin())).get_message_type();
    return tmp ==
           static_cast<uint32_t>(ControlMessage::COUNT);
  }
  
  std::unique_ptr<Message> fetchMessage(Socket &socket) { return nullptr; }

private:
  std::size_t id;
  std::size_t group_id;
  std::size_t io_thread_num;
  std::size_t network_size;
  std::vector<BufferedReader> buffered_readers;
  std::vector<std::string> peers_ip;
  std::vector<std::shared_ptr<Worker>> workers;
  LockfreeQueue<Message *> &coordinator_queue;
  std::atomic<bool> &stopFlag;
};

class OutgoingDispatcher {
public:
  OutgoingDispatcher(std::size_t id, std::size_t group_id,
                     std::size_t io_thread_num, 
                     std::vector<std::string> &peers_ip,
                     std::vector<Socket> &sockets,
                     const std::vector<std::shared_ptr<Worker>> &workers,
                     LockfreeQueue<Message *> &coordinator_queue,
                     std::atomic<bool> &stopFlag)
      : id(id), group_id(group_id), io_thread_num(io_thread_num),
        network_size(0), peers_ip(peers_ip), sockets(sockets), workers(workers),
        coordinator_queue(coordinator_queue), stopFlag(stopFlag) {}

  void start() {

    auto numCoordinators = sockets.size();
    auto numWorkers = workers.size();
    // single node test mode
    if (numCoordinators == 1) {
      return;
    }

    LOG(INFO) << "Outgoing Dispatcher started, numCoordinators = "
              << numCoordinators << ", numWorkers = " << numWorkers
              << ", group id = " << group_id;

    while (!stopFlag.load()) {

      // check coordinator

      if (group_id == 0 && !coordinator_queue.empty()) {
        std::unique_ptr<Message> message(coordinator_queue.front());
        bool ok = coordinator_queue.pop();
        CHECK(ok);
        sendMessage(message.get());
      }

      size_t recorder_id = numWorkers - 1;
      for (auto i = group_id; i < numWorkers; i += io_thread_num) {
        dispatchMessage(workers, i, recorder_id); // dispatchMessage(workers[i]);// 
      }
      std::this_thread::yield();
    }

    LOG(INFO) << "Outgoing Dispatcher exits, network size: " << network_size;
  }

  void sendMessage(Message *message) {
    auto src_node_id = message->get_source_node_id();
    auto dest_node_id = message->get_dest_node_id();
    DCHECK(dest_node_id >= 0 && dest_node_id < sockets.size() &&
           dest_node_id != id);


    if(peers_ip[src_node_id] == peers_ip[dest_node_id]){
      // same node, simulate latency
      std::this_thread::sleep_for(std::chrono::nanoseconds(5));
    }

    MessagePiece messagePiece = *(message->begin());
    auto message_type = static_cast<int>(messagePiece.get_message_type());

    // DCHECK(message->get_message_length() == message->data.length()) << message->get_message_length() << " " << message->data.length() << " type: " << message_type << " " << static_cast<int>(LionMessage::METIS_SEARCH_REQUEST);

    sockets[dest_node_id].write_n_bytes(message->get_raw_ptr(),
                                        message->get_message_length());

    network_size += message->get_message_length();

    // if(is_transaction_message(message)){
    //   VLOG(DEBUG_V8) << "send TRANSFER_REQUEST";
    // }
  }
  // bool is_transaction_message(Message *message) {
  //   return (*(message->begin())).get_message_type() ==
  //          static_cast<uint32_t>(HermesMessage::TRANSFER_REQUEST);
  // }
  // void dispatchMessage(const std::shared_ptr<Worker> &worker) {
  void dispatchMessage(const std::vector<std::shared_ptr<Worker>> &workers, size_t cur_id, size_t recorder_id) {
    /**
     * @brief 
     * @note modified by truth 本地worker同步txn到recorder
     */
    // bool from_exe_to_man = cur_id != recorder_id;
    const std::shared_ptr<Worker> worker = workers[cur_id];

    Message *raw_message = worker->pop_message();
    if (raw_message == nullptr) {
      raw_message = worker->delay_pop_message();
      if(raw_message == nullptr){
        return;
      } 
    }
    // wrap the message with a unique pointer.
    std::unique_ptr<Message> message(raw_message);

    auto cur_message = message.release();
    if(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count() > cur_message->delay_time){
      sendMessage(cur_message);
    } else {
      worker->delay_push_message(cur_message);
      // static int a = 0;
      // a += 1;
      // LOG(INFO) << "RETRY" << a;
    }
    // if(from_exe_to_man){
    //       // 是executor, 需要送到本地的manager
    //       if((*(cur_message->begin())).get_message_type() == static_cast<int32_t>(ControlMessage::COUNT)){
    //         // 是 count ， 给本地发一个
    //         workers[recorder_id]->push_message(cur_message);
    //       }
    // }
    // send the message
  //  LOG(INFO) << cur_id << " send message : " << (*(cur_message->begin())).get_message_type() << " to " << cur_message->get_dest_node_id();
    
    // message.get());
    // 
  }

private:
  std::size_t id;
  std::size_t group_id;
  std::size_t io_thread_num;
  std::size_t network_size;
  std::vector<std::string> peers_ip;
  std::vector<Socket> &sockets;
  std::vector<std::shared_ptr<Worker>> workers;
  LockfreeQueue<Message *> &coordinator_queue;
  std::atomic<bool> &stopFlag;
};

} // namespace star
