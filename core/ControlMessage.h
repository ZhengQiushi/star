//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "common/MyMove.h"
#include "common/ShareQueue.h"
#include <deque>

#include <thread>

namespace star {

enum class ControlMessage { STATISTICS, SIGNAL, ACK, STOP, COUNT, TRANSMIT, 
                            ROUTER_TRANSACTION_REQUEST, ROUTER_TRANSACTION_RESPONSE, ROUTER_STOP, NFIELDS };
// COUNT means 统计事务涉及到的record关联性

class ControlMessageFactory {

public:
  static int pin_thread_to_core(const Context &context, std::thread &t) {
#ifndef __APPLE__
    static std::size_t core_id = context.cpu_core_id;
    std::size_t core_id_ = core_id;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id++, &cpuset);
    int rc =
        pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
    // CHECK(rc == 0) << rc;
    return core_id_;
#endif
  }

  static int pin_process_to_core(const Context &context, std::size_t core_id) {
#ifndef __APPLE__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    int rc =
        sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);
    // CHECK(rc == 0) << rc;
    return core_id;
#endif
  }

  static void pin_thread_to_core(const Context &context, std::thread &t, std::size_t core_id) {
#ifndef __APPLE__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    int rc =
        pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
    // CHECK(rc == 0) << rc;
    return;
#endif
  }
  static std::size_t new_router_transaction_message(Message &message, int table_id, 
                                                    simpleTransaction& txn, 
                                                    RouterTxnOps op){
    // 
    // op = src_coordinator_id
    auto& update_ = txn.update; // txn->get_query_update();
    auto& key_ = txn.keys; // txn->get_query();
    uint64_t txn_size = (uint64_t)key_.size();
    auto key_size = sizeof(uint64_t);
    uint64_t is_distributed = txn.is_distributed;
    uint64_t is_real_distributed = txn.is_real_distributed;
    uint64_t is_transmit_request = txn.is_transmit_request;
    uint64_t txn_id = txn.idx_;
    uint64_t global_txn_id = txn.global_id_;

    int on_replica_id = txn.on_replica_id;
    int destination_coordinator  = txn.destination_coordinator;

    auto message_size =
        MessagePiece::get_header_size() + sizeof(op) + 
                      sizeof(is_distributed) + 
                      sizeof(is_real_distributed) + 
                      sizeof(is_transmit_request) + 
                      sizeof(txn_id) + 
                      sizeof(global_txn_id) + 
                      sizeof(on_replica_id) + 
                      sizeof(destination_coordinator) + 
                      sizeof(txn_size) + (key_size + sizeof(bool)) * txn_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::ROUTER_TRANSACTION_REQUEST), message_size,
        table_id, 0);

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << op 
            << is_distributed 
            << is_real_distributed 
            << is_transmit_request 
            << txn_id         
            << global_txn_id     
            << on_replica_id 
            << destination_coordinator 
            << txn_size;

    for(size_t i = 0 ; i < txn_size; i ++ ){
      uint64_t key = key_[i];
      bool update = update_[i];
      encoder.write_n_bytes((void*) &key, key_size);
      encoder.write_n_bytes((void*) &update, sizeof(bool));
//      LOG(INFO) <<  key_[i] << " " << update_[i];
    }
    message.flush();
    // VLOG(DEBUG_V14) << " SEND ROUTER " << message.get_source_node_id() << " " << message.get_dest_node_id() << is_distributed << "  " << is_transmit_request << " " << txn.keys[0] << " " << txn.keys[1];
    return message_size;
  }

  static std::size_t router_transaction_response_message(Message &message){
    // prepare response message header
    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::ROUTER_TRANSACTION_RESPONSE), message_size,
        0, 0);
    star::Encoder encoder(message.data);
    encoder << message_piece_header;
    message.flush();
    return message_size;
  }

  static std::size_t router_stop_message(Message &message, int send_txn_num){
    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + sizeof(send_txn_num);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::ROUTER_STOP), message_size,
        0, 0);
    star::Encoder encoder(message.data);
    encoder << message_piece_header << send_txn_num;
    message.flush();
    
    return message_size;
  }

  static std::size_t new_statistics_message(Message &message, double value) {
    /*
     * The structure of a statistics message: (statistics value : double)
     *
     */

    // the message is not associated with a table or a partition, use 0.
    auto message_size = MessagePiece::get_header_size() + sizeof(double);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::STATISTICS), message_size, 0, 0);

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << value;
    message.flush();
    return message_size;
  }

  static std::size_t new_signal_message(Message &message, uint32_t value) {

    /*
     * The structure of a signal message: (signal value : uint32_t)
     */

    // the message is not associated with a table or a partition, use 0.
    auto message_size = MessagePiece::get_header_size() + sizeof(uint32_t);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::SIGNAL), message_size, 0, 0);

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << value;
    message.flush();
    return message_size;
  }

  static std::size_t new_ack_message(Message &message) {
    /*
     * The structure of an ack message: ()
     */

    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::ACK), message_size, 0, 0);
    Encoder encoder(message.data);
    encoder << message_piece_header;
    message.flush();
    return message_size;
  }

  static std::size_t new_stop_message(Message &message) {
    /*
     * The structure of a stop message: ()
     */

    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::STOP), message_size, 0, 0);
    Encoder encoder(message.data);
    encoder << message_piece_header;
    message.flush();
    return message_size;
  }

  static std::size_t new_transmit_request_message(Message &message, int table_id, 
                                                    simpleTransaction& txn){
    // 
    auto& key_ = txn.keys; // txn->get_query();
    uint64_t txn_size = (uint64_t)key_.size();
    auto key_size = sizeof(uint64_t);

    auto message_size =
        MessagePiece::get_header_size() + sizeof(txn_size) + (key_size) * txn_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::TRANSMIT), message_size,
        table_id, 0);

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << txn_size;
    for(size_t i = 0 ; i < txn_size; i ++ ){
      uint64_t key = key_[i];
      encoder.write_n_bytes((void*) &key, key_size);
//      LOG(INFO) <<  key_[i] << " " << update_[i];
    }
    message.flush();
    return message_size;
  }


  // template <class WorkloadType>
  // static std::size_t new_transmit_message(Message &message, 
  //                                         const myMove<WorkloadType>& move) {
  //   /**
  //    * @brief 同一个表的迁移
  //    * @note 可能为零
  //    * 
  //    */
  //   // int tableId = ycsb::tableID;

    
  //   int32_t total_key_len = (int32_t)move.records.size();
  //   if(total_key_len > 30){
  //     LOG(INFO) << "too large";
  //   }
  //   // DCHECK(move.records.size() >= 0);
  
  //   int32_t key_size = sizeof(u_int64_t);// move.records[0].key_size;
  //   int32_t normal_size = sizeof(int32_t);

  //   // int32_t field_size = (total_key_len > 0) ? move.records[0].field_size: 0;

  //   auto cal_message_length = [&](const myMove<WorkloadType>& move){
  //     auto message_size = MessagePiece::get_header_size() + 
  //                         normal_size + // len of keys
  //                         normal_size;  // dest partition
  //     // LOG(INFO) << "New message send: ";
  //     for(size_t i = 0 ; i < move.records.size(); i ++ ){
  //       // 
  //       message_size += normal_size +   // src partition
  //                       key_size +      // key
  //                       normal_size +   // field size
  //                       move.records[i].field_size; // field value
  //       // LOG(INFO) << "[ " << *(int32_t*)& move.records[i].table_id << "] " << move.records[i].src_partition_id << " -> " << move.dest_partition_id;
  //     }

  //     return message_size;
  //   };
  //   auto message_size = cal_message_length(move);
  //   // MessagePiece::get_header_size() + 
  //   //                     key_size +                 // len of keys
  //   //                     key_size +                 // field_size
  //   //                     (key_size + key_size + field_size) * total_key_len + // src partition + keys + value 
  //   //                     key_size;       // dest partition

  //   // LOG(INFO) << "message_size: " << message_size << " = " << field_size << " * " << total_key_len;

  //   auto message_piece_header = MessagePiece::construct_message_piece_header(
  //       static_cast<uint32_t>(ControlMessage::TRANSMIT), message_size, 0, 0);
  //   Encoder encoder(message.data);
  //   encoder << message_piece_header;

  //   encoder.write_n_bytes((void*)&total_key_len, normal_size);
  //   encoder.write_n_bytes((void*)&move.dest_partition_id, normal_size);

  //   // encoder.write_n_bytes((void*)&field_size, key_size);

  //   for (size_t i = 0 ; i < static_cast<size_t>(total_key_len); i ++ ){
  //     // ITable* table = db.find_table(tableId, move.records[i].src_partition_id);
  //     auto field_size = move.records[i].field_size;
  //     encoder.write_n_bytes((void*)&move.records[i].src_partition_id, normal_size);
  //     encoder.write_n_bytes((void*)&move.records[i].record_key_  , key_size);
  //     encoder.write_n_bytes((void*)&field_size                      , normal_size);
  //     encoder.write_n_bytes((void*)&move.records[i].value           , field_size);
  //   }

  //   message.flush();
  //   return message_size;
  // }

};

template <class Database> class ControlMessageHandler {
public:
  static void router_transaction_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db,
                                      ShareQueue<simpleTransaction>* router_txn_queue,
                                      std::deque<int>* stop_queue
) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(ControlMessage::ROUTER_TRANSACTION_REQUEST));

    auto stringPiece = inputPiece.toStringPiece();
    uint64_t txn_size, is_distributed, is_real_distributed, is_transmit_request;
    RouterTxnOps op;
    uint64_t txn_id, global_txn_id;
    
    int on_replica_id;
    int destination_coordinator;
    simpleTransaction new_router_txn;

    // get op
    op = *(RouterTxnOps*)stringPiece.data();
    stringPiece.remove_prefix(sizeof(op));

    // 
    is_distributed = *(uint64_t*)stringPiece.data();
    stringPiece.remove_prefix(sizeof(is_distributed));

    is_real_distributed = *(uint64_t*)stringPiece.data();
    stringPiece.remove_prefix(sizeof(is_real_distributed));

    is_transmit_request = *(uint64_t*)stringPiece.data();
    stringPiece.remove_prefix(sizeof(is_transmit_request));

    txn_id = *(uint64_t*)stringPiece.data();
    stringPiece.remove_prefix(sizeof(txn_id));

    global_txn_id = *(uint64_t*)stringPiece.data();
    stringPiece.remove_prefix(sizeof(global_txn_id));

    on_replica_id = *(int*)stringPiece.data();
    stringPiece.remove_prefix(sizeof(on_replica_id));

    destination_coordinator = *(int*)stringPiece.data();
    stringPiece.remove_prefix(sizeof(destination_coordinator));

    // get key_size
    txn_size = *(uint64_t*)stringPiece.data();
    stringPiece.remove_prefix(sizeof(txn_size));

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + 
           sizeof(op) + 
           sizeof(is_distributed) + 
           sizeof(is_real_distributed) + 
           sizeof(is_transmit_request) + 
           sizeof(txn_id) + 
           sizeof(global_txn_id) + 
           sizeof(on_replica_id) + 
           sizeof(destination_coordinator) + 
           sizeof(txn_size) + 
           (sizeof(uint64_t) + sizeof(bool)) * txn_size) ;

    star::Decoder dec(stringPiece);
    for(uint64_t i = 0 ; i < txn_size; i ++ ){
      uint64_t key;
      bool update;

      key = *(uint64_t*)stringPiece.data();
      stringPiece.remove_prefix(sizeof(key));

      update = *(bool*)stringPiece.data();
      stringPiece.remove_prefix(sizeof(update));

      new_router_txn.keys.push_back(key);
      new_router_txn.update.push_back(update);
    }

    new_router_txn.op = static_cast<RouterTxnOps>(op);
    new_router_txn.size = inputPiece.get_message_length();
    new_router_txn.is_distributed = is_distributed;
    new_router_txn.is_real_distributed = is_real_distributed;
    new_router_txn.is_transmit_request = is_transmit_request;
    new_router_txn.idx_ = txn_id;
    new_router_txn.global_id_ = global_txn_id;
    new_router_txn.on_replica_id = on_replica_id;
    new_router_txn.destination_coordinator = destination_coordinator;
    // VLOG(DEBUG_V14) << " GET ROUTER " << is_transmit_request << " " << 
    //                                      is_distributed      << " " << 
    //                                      on_replica_id       << " " <<
    //                                      destination_coordinator << " " <<
    //                                      new_router_txn.keys[0]  << " " << 
    //                                      new_router_txn.keys[1];
    router_txn_queue->push_no_wait(new_router_txn);
    // DCHECK(ok == true);

  }
  
  static void router_transaction_response_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db,
                                      ShareQueue<simpleTransaction>* router_txn_queue,
                                      std::deque<int>* stop_queue
) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(ControlMessage::ROUTER_TRANSACTION_RESPONSE));
    return;

}

  static void router_stop_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db,
                                      ShareQueue<simpleTransaction>* router_txn_queue,
                                      std::deque<int>* stop_queue
) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(ControlMessage::ROUTER_STOP));
    int send_txn_cnt;
    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + sizeof(send_txn_cnt));
    auto stringPiece = inputPiece.toStringPiece();
    star::Decoder dec(stringPiece);
    dec >> send_txn_cnt;
    stop_queue->push_back(send_txn_cnt);
    // LOG(INFO) << "GET ROUTER_STOP " << stop_queue->size();
    return;

}
  static std::vector<
      std::function<void(MessagePiece, Message &, Database &, ShareQueue<simpleTransaction>*, std::deque<int>* )>>
  get_message_handlers() {
    std::vector<
        std::function<void(MessagePiece, Message &, Database &, ShareQueue<simpleTransaction>*,  std::deque<int>* )>>
        v;
    v.resize(static_cast<int>(ControlMessage::NFIELDS) - 3);
    v.push_back(ControlMessageHandler::router_transaction_handler);
    v.push_back(ControlMessageHandler::router_transaction_response_handler);
    v.push_back(ControlMessageHandler::router_stop_handler);
    return v;
  }
};


} // namespace star
