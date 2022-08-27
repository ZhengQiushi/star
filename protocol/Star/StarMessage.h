//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "common/Operation.h"
#include "core/ControlMessage.h"
#include "core/Table.h"
#include "protocol/Silo/SiloHelper.h"
#include "protocol/Silo/SiloTransaction.h"

namespace star {

enum class StarMessage {
  ASYNC_VALUE_REPLICATION_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
  SYNC_VALUE_REPLICATION_REQUEST,
  SYNC_VALUE_REPLICATION_RESPONSE,
  OPERATION_REPLICATION_REQUEST,
  ROUTER_TRANSACTION_REQUEST,
  ROUTER_TRANSACTION_RESPONSE,
  NFIELDS
};

class StarMessageFactory {
using Transaction = SiloTransaction;
public:
  static std::size_t new_async_value_replication_message(Message &message,
                                                         ITable &table,
                                                         const void *key,
                                                         const void *value,
                                                         uint64_t commit_tid) {

    /*
     * The structure of an async value replication request: (primary key, field
     * value, commit_tid)
     */

    auto key_size = table.key_size();
    auto field_size = table.field_size();

    auto message_size = MessagePiece::get_header_size() + key_size +
                        field_size + sizeof(commit_tid);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(StarMessage::ASYNC_VALUE_REPLICATION_REQUEST),
        message_size, table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
    encoder << commit_tid;
    message.flush();
    return message_size;
  }

  static std::size_t new_sync_value_replication_message(Message &message,
                                                        ITable &table,
                                                        const void *key,
                                                        const void *value,
                                                        uint64_t commit_tid) {
    /*
     * The structure of a sync value replication request: (primary key, field
     * value, commit_tid)
     */

    auto key_size = table.key_size();
    auto field_size = table.field_size();

    auto message_size = MessagePiece::get_header_size() + key_size +
                        field_size + sizeof(commit_tid);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(StarMessage::SYNC_VALUE_REPLICATION_REQUEST),
        message_size, table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
    encoder << commit_tid;
    message.flush();
    return message_size;
  }

  template<typename T = u_int64_t>
  static std::size_t new_async_txn_of_record_message(Message &message,
                                                     const std::vector<T> record_key_in_this_txn){
    /**
     * @brief 记录txn的record
     * @note key_size increased from 32bits to 64bits
    */
    auto key_size = sizeof(u_int64_t);
    auto normal_size = sizeof(int32_t);

    int32_t total_key_len = (int32_t)record_key_in_this_txn.size();

    auto message_size = MessagePiece::get_header_size() +
                        normal_size +             // total length
                        key_size * total_key_len;

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::COUNT),
        message_size, 0, 0);

    Encoder encoder(message.data);
    encoder << message_piece_header;

    encoder.write_n_bytes((void*)&total_key_len, normal_size);
    for (size_t i = 0 ; i < record_key_in_this_txn.size(); i ++ ){
      auto key = record_key_in_this_txn[i];
      encoder.write_n_bytes((void*)&key, key_size);
    }
    message.flush();
    // LOG(INFO) << "message.get_message_count(): " << message.get_message_count() << "\n";
    return message_size;
  }
  static std::size_t
  new_operation_replication_message(Message &message,
                                    const Operation &operation) {

    /*
     * The structure of an operation replication message: (tid, data)
     */

    auto message_size = MessagePiece::get_header_size() + sizeof(uint64_t) +
                        operation.data.size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(StarMessage::OPERATION_REPLICATION_REQUEST),
        message_size, 0, 0);

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << operation.tid;
    encoder.write_n_bytes(operation.data.c_str(), operation.data.size());
    message.flush();
    return message_size;
  }

  static std::size_t new_router_transaction_message(Message &message, int table_id, 
                                                    Transaction *txn, uint64_t op){
    // 
    // op = src_coordinator_id
    auto update_ = txn->get_query_update();
    auto key_ = txn->get_query();
    uint64_t txn_size = (uint64_t)key_.size();
    auto key_size = sizeof(uint64_t);
    
    auto message_size =
        MessagePiece::get_header_size() + sizeof(op) + sizeof(txn_size) + (key_size + sizeof(bool)) * txn_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(StarMessage::ROUTER_TRANSACTION_REQUEST), message_size,
        table_id, 0);

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << op << txn_size;
    for(size_t i = 0 ; i < txn_size; i ++ ){
      uint64_t key = key_[i];
      bool update = update_[i];
      encoder.write_n_bytes((void*) &key, key_size);
      encoder.write_n_bytes((void*) &update, sizeof(bool));
//      LOG(INFO) <<  key_[i] << " " << update_[i];
    }
    message.flush();
    return message_size;
  }

  static std::size_t router_transaction_response_message(Message &message){
    // prepare response message header
    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(StarMessage::ROUTER_TRANSACTION_RESPONSE), message_size,
        0, 0);

    star::Encoder encoder(message.data);
    encoder << message_piece_header;
    message.flush();
    return message_size;
  }
};


template <class Database> class StarMessageHandler {

  using Transaction = SiloTransaction;

public:
  static void async_value_replication_request_handler(MessagePiece inputPiece,
                                                      Message &responseMessage,
                                                      Database &db,
                                                      Transaction *txn,
                                      std::deque<simpleTransaction>* router_txn_queue) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(StarMessage::ASYNC_VALUE_REPLICATION_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    /*
     * The structure of an async value replication request:
     *      (primary key, field value, commit_tid).
     * The structure of an async value replication response: null
     */

    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                  key_size + field_size +
                                                  sizeof(uint64_t));

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);
    auto valueStringPiece = stringPiece;
    stringPiece.remove_prefix(field_size);

    uint64_t commit_tid;
    Decoder dec(stringPiece);
    dec >> commit_tid;

    DCHECK(dec.size() == 0);

    std::atomic<uint64_t> &tid = table.search_metadata(key);

    bool success = false;
    uint64_t last_tid = SiloHelper::lock(tid, success);
    while(success != true){
      std::this_thread::yield();
      // 说明local data 本来就已经被locked，会被 Thomas write rule 覆盖... 
      last_tid = SiloHelper::lock(tid, success);
    }
    const auto &k = *static_cast<const int32_t *>(key);
    // LOG(WARNING) << "KEY: " << k << " TID: " << tid;
    if (commit_tid > last_tid) {
      table.deserialize_value(key, valueStringPiece); 
      SiloHelper::unlock(tid, commit_tid);
    } else {
      // if(SiloHelper::is_locked(tid)){
      SiloHelper::unlock(tid);
    }
  }

  static void sync_value_replication_request_handler(MessagePiece inputPiece,
                                                     Message &responseMessage,
                                                     Database &db,
                                                     Transaction *txn,
                                      std::deque<simpleTransaction>* router_txn_queue) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(StarMessage::SYNC_VALUE_REPLICATION_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    /*
     * The structure of a sync value replication request:
     *      (primary key, field value, commit_tid).
     * The structure of a sync value replication response: ()
     */

    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                  key_size + field_size +
                                                  sizeof(uint64_t));

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);
    auto valueStringPiece = stringPiece;
    stringPiece.remove_prefix(field_size);

    uint64_t commit_tid;
    Decoder dec(stringPiece);
    dec >> commit_tid;

    DCHECK(dec.size() == 0);

    std::atomic<uint64_t> &tid = table.search_metadata(key);

    bool success = false;
    uint64_t last_tid = SiloHelper::lock(tid, success);
    while(success != true){
      std::this_thread::yield();
      // 说明local data 本来就已经被locked，会被 Thomas write rule 覆盖... 
      last_tid = SiloHelper::lock(tid, success);
    }

    // DCHECK(last_tid < commit_tid);
    if(last_tid < commit_tid){
      table.deserialize_value(key, valueStringPiece);
    }
    SiloHelper::unlock(tid, commit_tid);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(StarMessage::SYNC_VALUE_REPLICATION_RESPONSE),
        message_size, table_id, partition_id);

    static int recv_ = 0;
    // LOG(INFO) << "recv : " << ++recv_;
    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    responseMessage.flush();
  }

  static void sync_value_replication_response_handler(MessagePiece inputPiece,
                                                      Message &responseMessage,
                                                      Database &db,
                                                      Transaction *txn,
                                      std::deque<simpleTransaction>* router_txn_queue) {
    return ;
    // DCHECK(false);
    // never come in
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(StarMessage::SYNC_VALUE_REPLICATION_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    /*
     * The structure of a sync value replication response: ()
     */

    // txn->pendingResponses--;
    // txn->network_size += inputPiece.get_message_length();
  }

  static void operation_replication_request_handler(MessagePiece inputPiece,
                                                    Message &responseMessage,
                                                    Database &db,
                                                    Transaction *txn,
                                      std::deque<simpleTransaction>* router_txn_queue) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(StarMessage::OPERATION_REPLICATION_REQUEST));

    auto message_size = inputPiece.get_message_length();
    Decoder dec(inputPiece.toStringPiece());
    Operation operation;
    dec >> operation.tid;

    auto data_size =
        message_size - MessagePiece::get_header_size() - sizeof(uint64_t);
    DCHECK(data_size > 0);

    operation.data.resize(data_size);
    dec.read_n_bytes(&operation.data[0], data_size);

    DCHECK(dec.size() == 0);
    db.apply_operation(operation);
  }

  static void router_transaction_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db,
                                      Transaction *txn,
                                      std::deque<simpleTransaction>* router_txn_queue
) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(StarMessage::ROUTER_TRANSACTION_REQUEST));

    auto stringPiece = inputPiece.toStringPiece();
    uint64_t txn_size, op;
    simpleTransaction new_router_txn;

    // get op
    op = *(uint64_t*)stringPiece.data();
    stringPiece.remove_prefix(sizeof(op));

    // get key_size
    txn_size = *(uint64_t*)stringPiece.data();
    stringPiece.remove_prefix(sizeof(txn_size));

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + sizeof(op) + sizeof(txn_size) + 
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
    router_txn_queue->push_back(new_router_txn);

  }
  static void router_transaction_response_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db,
                                      Transaction *txn,
                                      std::deque<simpleTransaction>* router_txn_queue
) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(StarMessage::ROUTER_TRANSACTION_RESPONSE));
    return;

}
  static std::vector<
      std::function<void(MessagePiece, Message &, Database &, Transaction *, std::deque<simpleTransaction>* )>>
  get_message_handlers() {
    std::vector<
        std::function<void(MessagePiece, Message &, Database &, Transaction *, std::deque<simpleTransaction>* )>>
        v;
    v.resize(static_cast<int>(ControlMessage::NFIELDS));
    v.push_back(StarMessageHandler::async_value_replication_request_handler);
    v.push_back(StarMessageHandler::sync_value_replication_request_handler);
    v.push_back(StarMessageHandler::sync_value_replication_response_handler);
    v.push_back(StarMessageHandler::operation_replication_request_handler);
    v.push_back(StarMessageHandler::router_transaction_handler);
    v.push_back(StarMessageHandler::router_transaction_response_handler);
    return v;
  }
};


} // namespace star