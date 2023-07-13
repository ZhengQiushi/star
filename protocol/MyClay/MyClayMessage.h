//
// Created by Yi Lu on 9/10/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"
#include "protocol/TwoPL/TwoPLHelper.h"
#include "protocol/MyClay/MyClayRWKey.h"
#include "protocol/MyClay/MyClayTransaction.h"

namespace star {

enum class MyClayMessage {
  TRANSMIT_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
  TRANSMIT_RESPONSE,
  TRANSMIT_ROUTER_ONLY_REQUEST,
  TRANSMIT_ROUTER_ONLY_RESPONSE,
  // 
  READ_LOCK_REQUEST,
  READ_LOCK_RESPONSE,
  WRITE_LOCK_REQUEST,
  WRITE_LOCK_RESPONSE,
  ABORT_REQUEST,
  WRITE_REQUEST,
  WRITE_RESPONSE,
  // 
  REPLICATION_REQUEST,
  REPLICATION_RESPONSE,
  // 
  RELEASE_READ_LOCK_REQUEST,
  // RELEASE_READ_LOCK_RESPONSE,
  RELEASE_WRITE_LOCK_REQUEST,
  // RELEASE_WRITE_LOCK_RESPONSE,

  NFIELDS
};

class MyClayMessageFactory {

public:
  static std::size_t new_read_lock_message(Message &message, ITable &table,
                                           const void *key,
                                           uint32_t key_offset) {

    /*
     * The structure of a read lock request: (primary key, key offset)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::READ_LOCK_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset;
    message.flush();
    return message_size;
  }

  static std::size_t new_write_lock_message(Message &message, ITable &table,
                                            const void *key,
                                            uint32_t key_offset) {

    /*
     * The structure of a write lock request: (primary key, key offset)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::WRITE_LOCK_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset;
    message.flush();
    return message_size;
  }

  static std::size_t new_abort_message(Message &message, ITable &table,
                                       const void *key, bool write_lock) {
    /*
     * The structure of an abort request: (primary key, wrtie lock)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(bool);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::ABORT_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << write_lock;
    message.flush();
    return message_size;
  }

  static std::size_t new_write_message(Message &message, ITable &table,
                                       const void *key, const void *value) {

    /*
     * The structure of a write request: (primary key, field value)
     */

    auto key_size = table.key_size();
    auto field_size = table.field_size();

    auto message_size = MessagePiece::get_header_size() + key_size + field_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::WRITE_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
    message.flush();
    return message_size;
  }

  static std::size_t new_release_read_lock_message(Message &message,
                                                   ITable &table,
                                                   const void *key) {
    /*
     * The structure of a release read lock request: (primary key)
     */

    auto key_size = table.key_size();

    auto message_size = MessagePiece::get_header_size() + key_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::RELEASE_READ_LOCK_REQUEST),
        message_size, table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    message.flush();
    return message_size;
  }

  static std::size_t new_release_write_lock_message(Message &message,
                                                    ITable &table,
                                                    const void *key,
                                                    uint64_t commit_tid) {

    /*
     * The structure of a release write lock request: (primary key, commit tid)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(commit_tid);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::RELEASE_WRITE_LOCK_REQUEST),
        message_size, table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << commit_tid;
    message.flush();
    return message_size;
  }

  static std::size_t new_transmit_message(Message &message, ITable &table,
                                        const void *key, uint32_t key_offset, bool remaster) {

    /*
     * The structure of a search request: (primary key, read key offset)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(key_offset) + sizeof(remaster);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::TRANSMIT_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset << remaster;
    message.flush();
    return message_size;
  }

  // 
  static std::size_t new_transmit_router_only_message(Message &message, ITable &table,
                                        const void *key, uint32_t key_offset) {

    /*
     * The structure of a search request: (primary key, read key offset)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::TRANSMIT_ROUTER_ONLY_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset;
    message.flush();
    return message_size;
  }


  static std::size_t new_replication_message(Message &message, ITable &table,
                                             const void *key, const void *value,
                                             uint64_t commit_tid) {

    /*
     * The structure of a replication request: (primary key, field value,
     * commit_tid)
     */

    auto key_size = table.key_size();
    auto field_size = table.field_size();

    auto message_size = MessagePiece::get_header_size() + key_size +
                        field_size + sizeof(commit_tid);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::REPLICATION_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
    encoder << commit_tid;
    message.flush();
    return message_size;
  }
};

template <class Database> class MyClayMessageHandler {
  using Transaction = MyClayTransaction;
  using Context = typename Database::ContextType;

public:
  // 
  static void transmit_request_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                      Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::TRANSMIT_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);    
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a read request: (primary key, read key offset)
     * The structure of a read response: (value, tid, read key offset)
     */

    auto stringPiece = inputPiece.toStringPiece();
    uint32_t key_offset;
    bool remaster, success; // add by truth 22-04-22

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(key_offset) + sizeof(remaster));

    // get row and offset
    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset >> remaster; // index offset in the readSet from source request node
    // LOG(INFO) << "TRANSMIT_REQUEST " << *(int*)key;
    DCHECK(dec.size() == 0);

    if(remaster == true){
      // remaster, not transfer
      value_size = 0;
    }

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + 
                        sizeof(uint64_t) + sizeof(key_offset) + sizeof(success) + sizeof(remaster) + 
                        value_size;
    
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::TRANSMIT_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;

    uint64_t latest_tid;

    success = table.contains(key);
    if(!success){
      VLOG(DEBUG_V12) << "  dont Exist " << *(int*)key ; // << " " << tid_int;
      encoder << latest_tid << key_offset << success << remaster;
      responseMessage.data.append(value_size, 0);
      responseMessage.flush();
      return;
    }

    std::atomic<uint64_t> &tid = table.search_metadata(key);
    // try to lock tuple. Ensure not locked by current node
    latest_tid = TwoPLHelper::write_lock(tid, success); // be locked 

    if(!success){
      VLOG(DEBUG_V12) << "  can't Lock " << *(int*)key; // << " " << tid_int;
      encoder << latest_tid << key_offset << success << remaster;
      responseMessage.data.append(value_size, 0);
      responseMessage.flush();
      return;
    } else {
      VLOG(DEBUG_V12) << " Lock " << *(int*)key << " " << tid << " " << latest_tid;
    }

    if(remaster == false || (remaster == true && context.migration_only > 0)) {
      // simulate cost of transmit data
      for (auto i = 0u; i < context.n_nop * 2; i++) {
        asm("nop");
      }
    }

    // lock the router_table 
    auto router_table = db.find_router_table(table_id); // , coordinator_id_old);
    auto router_val = (RouterValue*)router_table->search_value(key);
    
    // 数据所在节点
    auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key);
    uint64_t static_coordinator_id = partition_id % context.coordinator_num;
    // create the new tuple in global router of source request node
    auto coordinator_id_new = responseMessage.get_dest_node_id(); 

    if(coordinator_id_new != coordinator_id_old){
      // 数据更新到 发req的对面
      // LOG(INFO) << table_id <<" " << *(int*) key << " transmit request switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << tid.load() << " " << latest_tid << " static: " << static_coordinator_id << " remaster: " << remaster;
      
      // update the router 
      router_val->set_dynamic_coordinator_id(coordinator_id_new);
      router_val->set_secondary_coordinator_id(coordinator_id_new);

      encoder << latest_tid << key_offset << success << remaster;
      // reserve size for read
      responseMessage.data.append(value_size, 0);
      
      // auto value = table.search_value(key);
      // LOG(INFO) << *(int*)key << " " << (char*)value << " success: " << success << " " << " remaster: " << remaster << " " << new_secondary_coordinator_id;
      
      if(success == true && remaster == false){
        // transfer: read from db and load data into message buffer
        void *dest =
            &responseMessage.data[0] + responseMessage.data.size() - value_size;
        auto row = table.search(key);
        TwoPLHelper::read(row, dest, value_size);
      }
      responseMessage.flush();

    } else if(coordinator_id_new == coordinator_id_old) {
      success = false;
      VLOG(DEBUG_V12) << " Same coordi : " << coordinator_id_new << " " <<coordinator_id_old << " " << *(int*)key << " " << tid;
      encoder << latest_tid << key_offset << success << remaster;
      responseMessage.data.append(value_size, 0);
      responseMessage.flush();
    
    } else {
      DCHECK(false);
    }

    // wait for the commit / abort to unlock
    TwoPLHelper::write_lock_release(tid);
  }
  static void transmit_response_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                      Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::TRANSMIT_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a read response: (value, tid, read key offset)
     */

    uint64_t tid;
    uint32_t key_offset;
    bool success, remaster;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> tid >> key_offset >> success >> remaster;

    if(remaster == true){
      value_size = 0;
    }

    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                  sizeof(tid) +
                                                  sizeof(key_offset) + sizeof(success) + sizeof(remaster) + 
                                                  value_size);

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
    
    MyClayRWKey &readKey = txn->readSet[key_offset];
    auto key = readKey.get_key();

    // LOG(INFO) << "TRANSMIT_RESPONSE " << *(int*)key;

    uint64_t last_tid = 0;


    if(remaster == false || (remaster == true && context.migration_only > 0)) {
      // simulate cost of transmit data
      for (auto i = 0u; i < context.n_nop * 2; i++) {
        asm("nop");
      }
    }

    if(success == true){
      // update router 
      auto key = readKey.get_key();
      auto value = readKey.get_value();

      if(!remaster){
        // read value message piece
        stringPiece = inputPiece.toStringPiece();
        stringPiece.remove_prefix(sizeof(tid) + sizeof(key_offset) + sizeof(success) + sizeof(remaster));
        // insert into local node
        dec = Decoder(stringPiece);
        dec.read_n_bytes(readKey.get_value(), value_size);
        DCHECK(strlen((char*)readKey.get_value()) > 0);

        VLOG(DEBUG_V12) << *(int*) key << " " << (char*) value << " insert ";
        table.insert(key, value, (void*)& tid);
      }

      // lock the respond tid and key
      std::atomic<uint64_t> &tid_ = table.search_metadata(key);
      bool success = false;

      last_tid = TwoPLHelper::write_lock(tid_, success);
        VLOG(DEBUG_V14) << "LOCK-write " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " " << last_tid;

      if(!success){
        VLOG(DEBUG_V14) << "AFTER REMASETER, FAILED TO GET LOCK : " << *(int*)key << " " << tid; // 
        txn->abort_lock = true;
        return;
      } 

      VLOG(DEBUG_V12) << table_id <<" " << *(int*) key << " " << (char*)readKey.get_value() << " reponse switch " << " " << " " << tid << "  " << remaster << " | " << success << " ";

      auto router_table = db.find_router_table(table_id); // , coordinator_id_old);
      auto router_val = (RouterValue*)router_table->search_value(key);

      // create the new tuple in global router of source request node
      auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key);
      uint64_t static_coordinator_id = partition_id % context.coordinator_num; // static replica never moves only remastered
      auto coordinator_id_new = responseMessage.get_source_node_id(); 

      // LOG(INFO) << table_id <<" " << *(int*) key << " " << (char*)value << " transmit reponse switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << tid << "  " << remaster;
      // update router
      router_val->set_dynamic_coordinator_id(coordinator_id_new);
      router_val->set_secondary_coordinator_id(coordinator_id_new);

      readKey.set_dynamic_coordinator_id(coordinator_id_new);
      readKey.set_router_value(router_val->get_dynamic_coordinator_id(),router_val->get_secondary_coordinator_id());
      // readKey.set_Read();
      readKey.set_tid(tid); // original tid for lock release
      readKey.set_write_lock_bit();
      
      // txn->tids[key_offset] = &tid_;
    } else {
      VLOG(DEBUG_V14) << "FAILED TO GET LOCK : " << *(int*)key << " " << tid; // 
      txn->abort_lock = true;
    }

  }
  static void transmit_router_only_request_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                      Transaction *txn) {
    /**
     * @brief directly move the data to the request node!
     * 修改其他机器上的路由情况， 当前机器不涉及该事务的处理，可以认为事务涉及的数据主节点都不在此，直接处理就可以
     * Transaction *txn unused
     */
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::TRANSMIT_ROUTER_ONLY_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);    
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a read request: (primary key, read key offset)
     * The structure of a read response: (value, tid, read key offset)
     */

    auto stringPiece = inputPiece.toStringPiece();
    uint32_t key_offset;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(key_offset));

    // get row and offset
    const void *key = stringPiece.data();
    // auto row = table.search(key);
     // LOG(INFO) << "TRANSMIT_ROUTER_ONLY_REQUEST " << *(int*)key;
    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset; // index offset in the readSet from source request node

    DCHECK(dec.size() == 0);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + value_size +
                        sizeof(uint64_t) + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::TRANSMIT_ROUTER_ONLY_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;

    // reserve size for read
    responseMessage.data.append(value_size, 0);
    uint64_t tid = 0; // auto tid = TwoPLHelper::read(row, dest, value_size);
    encoder << tid << key_offset;

    // lock the router_table 
    auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key);
    // create the new tuple in global router of source request node
    auto coordinator_id_new = responseMessage.get_dest_node_id(); 
    if(coordinator_id_new != coordinator_id_old){
      auto router_table = db.find_router_table(table_id); // , coordinator_id_new);
      auto router_val = (RouterValue*)router_table->search_value(key);
      // // LOG(INFO) << *(int*) key << " delete " << coordinator_id_old << " --> " << coordinator_id_new;

      router_val->set_dynamic_coordinator_id(coordinator_id_new);// (key, &coordinator_id_new);
      router_val->set_secondary_coordinator_id(coordinator_id_new);
    }

    // if(context.coordinator_id == context.coordinator_num){
    //   // LOG(INFO) << "transmit_router_only_request_handler : " << *(int*)key << " " << coordinator_id_old << " -> " << coordinator_id_new;
    // }
    responseMessage.flush();
  }

  static void transmit_router_only_response_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                      Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::TRANSMIT_ROUTER_ONLY_RESPONSE));
    // LOG(INFO) << "TRANSMIT_ROUTER_ONLY_RESPONSE";
    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
  }

  static void read_lock_request_handler(MessagePiece inputPiece,
                                        Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                        Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::READ_LOCK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();

    ITable &table = *db.find_table(table_id, partition_id);    
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();
    /*
     * The structure of a read lock request: (primary key, key offset)
     * The structure of a read lock response: (success?, key offset, value?,
     * tid?)
     */

    auto stringPiece = inputPiece.toStringPiece();
    uint32_t key_offset;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(key_offset));

    const void *key = stringPiece.data();

    bool success;
    uint64_t latest_tid;
    
    auto& test = table.search_metadata(key, success);

    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset;

    DCHECK(dec.size() == 0);

    if(success){
      auto row = table.search(key);
      std::atomic<uint64_t> &tid = *std::get<0>(row);
      VLOG(DEBUG_V16) << "READ_LOCK_REQUEST " << *(int*)key;
      latest_tid = TwoPLHelper::read_lock(tid, success);
    }
    

    // prepare response message header
    auto message_size =
        MessagePiece::get_header_size() + sizeof(bool) + sizeof(key_offset);

    if (success) {
      message_size += value_size + sizeof(uint64_t);
    }

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::READ_LOCK_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    encoder << success << key_offset;

    if (success) {
      // reserve size for read
      responseMessage.data.append(value_size, 0);
      void *dest =
          &responseMessage.data[0] + responseMessage.data.size() - value_size;
      // read to message buffer
      auto row = table.search(key);
      TwoPLHelper::read(row, dest, value_size);
      encoder << latest_tid;
    }

    responseMessage.flush();
  }

  static void read_lock_response_handler(MessagePiece inputPiece,
                                         Message &responseMessage,
                                         Database &db, const Context &context,  Partitioner *partitioner, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::READ_LOCK_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();

    ITable &table = *db.find_table(table_id, partition_id);   

    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a read lock response: (success?, key offset, value?,
     * tid?)
     */

    bool success;
    uint32_t key_offset;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> success >> key_offset;
    MyClayRWKey &readKey = txn->readSet[key_offset];

    VLOG(DEBUG_V16) << " READ_LOCK_RESPONSE  " << *(int*)readKey.get_key() << " " << success;

    if (success) {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success) +
                 sizeof(key_offset) + value_size + sizeof(uint64_t));

      
      dec.read_n_bytes(readKey.get_value(), value_size);
      uint64_t tid;
      dec >> tid;
      readKey.set_read_lock_bit();
      readKey.set_tid(tid);
    } else {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success) +
                 sizeof(key_offset));

      txn->abort_lock = true;
    }

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
  }

  static void write_lock_request_handler(MessagePiece inputPiece,
                                         Message &responseMessage,
                                         Database &db, const Context &context,  Partitioner *partitioner, Transaction *txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::WRITE_LOCK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();

    ITable &table = *db.find_table(table_id, partition_id);   

    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a write lock request: (primary key, key offset)
     * The structure of a write lock response: (success?, key offset, value?,
     * tid?)
     */

    auto stringPiece = inputPiece.toStringPiece();
    uint32_t key_offset;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(key_offset));

    const void *key = stringPiece.data();

    bool success;
    uint64_t latest_tid;
    
    auto& test = table.search_metadata(key, success);


    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset;

    DCHECK(dec.size() == 0);

    if(success){
      auto row = table.search(key);
      std::atomic<uint64_t> &tid = *std::get<0>(row);
      latest_tid = TwoPLHelper::write_lock(tid, success);

    }
    // prepare response message header
    auto message_size =
        MessagePiece::get_header_size() + sizeof(bool) + sizeof(key_offset);

    if (success) {
      message_size += value_size + sizeof(uint64_t);
    }

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::WRITE_LOCK_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    encoder << success << key_offset;

    VLOG(DEBUG_V16) << "WRITE_LOCK_REQUEST " << *(int*)key << " " << success ;

    if (success) {
      // reserve size for read
      responseMessage.data.append(value_size, 0);
      void *dest =
          &responseMessage.data[0] + responseMessage.data.size() - value_size;
      // read to message buffer
      auto row = table.search(key);
      TwoPLHelper::read(row, dest, value_size);
      encoder << latest_tid;
    }

    responseMessage.flush();
  }

  static void write_lock_response_handler(MessagePiece inputPiece,
                                          Message &responseMessage,
                                          Database &db, const Context &context,  Partitioner *partitioner, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::WRITE_LOCK_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    
    ITable &table = *db.find_table(table_id, partition_id);   

    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a read lock response: (success?, key offset, value?,
     * tid?)
     */

    bool success;
    uint32_t key_offset;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> success >> key_offset;

    MyClayRWKey &readKey = txn->readSet[key_offset];

    VLOG(DEBUG_V16) << " WRITE_LOCK_RESPONSE  " << *(int*)readKey.get_key() << " " << success;


    if (success) {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success) +
                 sizeof(key_offset) + value_size + sizeof(uint64_t));

      dec.read_n_bytes(readKey.get_value(), value_size);
      uint64_t tid;
      dec >> tid;
      readKey.set_write_lock_bit();
      readKey.set_tid(tid);
    } else {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success) +
                 sizeof(key_offset));

      txn->abort_lock = true;
    }

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
  }

  static void abort_request_handler(MessagePiece inputPiece,
                                    Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                    Transaction *txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::ABORT_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();

    ITable &table = *db.find_table(table_id, partition_id);   

    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of an abort request: (primary key, write_lock)
     * The structure of an abort response: null
     */

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(bool));

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);

    bool write_lock;
    Decoder dec(stringPiece);
    dec >> write_lock;

    std::atomic<uint64_t> &tid = table.search_metadata(key);
    // LOG(INFO) << "ABORT_REQUEST " << *(int*)key << " " << write_lock;
    if (write_lock) {
      TwoPLHelper::write_lock_release(tid);
    } else {
      TwoPLHelper::read_lock_release(tid);
    }
  }

  static void write_request_handler(MessagePiece inputPiece,
                                    Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                    Transaction *txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::WRITE_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);       


    auto key_size = table.key_size();
    auto field_size = table.field_size();

    /*
     * The structure of a write request: (primary key, field value)
     * The structure of a write response: ()
     */

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + field_size);

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);

    table.deserialize_value(key, stringPiece);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::WRITE_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    responseMessage.flush();

    VLOG(DEBUG_V16) << " WRITE_REQUEST  " << *(int*)key; // << " " << success;
  }

  static void write_response_handler(MessagePiece inputPiece,
                                     Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                     Transaction *txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::WRITE_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);   

    auto key_size = table.key_size();

    /*
     * The structure of a write response: ()
     */

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
    VLOG(DEBUG_V16) << " WRITE_RESPONSE  "; //  << *(int*)key; // << " " << success;
  }





  static void replication_request_handler(MessagePiece inputPiece,
                                          Message &responseMessage,
                                          Database &db, const Context &context,  Partitioner *partitioner, Transaction *txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::REPLICATION_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);    
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    /*
     * The structure of a replication request: (primary key, field value,
     * commit_tid) The structure of a replication response: null
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
    bool success = false;
    std::atomic<uint64_t> &tid = table.search_metadata(key, success);
    uint32_t txn_id = 0;
    int debug_key = 0;

    if(success){
      TwoPLHelper::write_lock(tid, success);
      if(success){
        // auto test = table.search_value(key);

        // VLOG(DEBUG_V11) << " respond send to : " << responseMessage.get_source_node_id() << " -> " << responseMessage.get_dest_node_id() << "  with " << debug_key << " " << (char*)test;

        //! TODO logic needs to be checked
        // DCHECK(last_tid < commit_tid);
        table.deserialize_value(key, valueStringPiece);
        TwoPLHelper::write_lock_release(tid, commit_tid);
      }
    } else {
      LOG(INFO) << " replication failed: " << debug_key;
    }



    auto message_size = MessagePiece::get_header_size() + 
                        sizeof(txn_id) + 
                        sizeof(debug_key);
                        
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::REPLICATION_RESPONSE), message_size,
        table_id, partition_id);
    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header 
            << txn_id
            << debug_key;
            
    responseMessage.flush();
  }

  static void replication_response_handler(MessagePiece inputPiece,
                                          Message &responseMessage,
                                          Database &db, const Context &context,  Partitioner *partitioner, Transaction *txn

) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::REPLICATION_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);

    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    int key = 0;
    uint32_t txn_id;
    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + 
          sizeof(txn_id) + 
          sizeof(key));

    // auto stringPiece = inputPiece.toStringPiece();
    // Decoder dec(stringPiece);
    // dec >> txn_id;

    // stringPiece = inputPiece.toStringPiece();
    // stringPiece.remove_prefix(sizeof(txn_id));

    // const void *key_ = stringPiece.data();
    // key = *(int*) key_;

    // VLOG(DEBUG_V16) << "replication_response_handler: " << responseMessage.get_source_node_id() << "->" << responseMessage.get_dest_node_id() << " " << key;
  }

  static void release_read_lock_request_handler(MessagePiece inputPiece,
                                                Message &responseMessage,
                                                Database &db, const Context &context,  Partitioner *partitioner,
                                                Transaction *txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::RELEASE_READ_LOCK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);   

    auto key_size = table.key_size();

    /*
     * The structure of a release read lock request: (primary key)
     * The structure of a release read lock response: ()
     */

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size);

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);

    std::atomic<uint64_t> &tid = table.search_metadata(key);

    TwoPLHelper::read_lock_release(tid);

    // prepare response message header
    // auto message_size = MessagePiece::get_header_size();
    // auto message_piece_header = MessagePiece::construct_message_piece_header(
    //     static_cast<uint32_t>(MyClayMessage::RELEASE_READ_LOCK_RESPONSE),
    //     message_size, table_id, partition_id);

    // star::Encoder encoder(responseMessage.data);
    // encoder << message_piece_header;
    // responseMessage.flush();

    VLOG(DEBUG_V16) << " RELEASE_READ_LOCK_REQUEST  " << *(int*)key; // << " " << success;

  }

  // static void release_read_lock_response_handler(MessagePiece inputPiece,
  //                                                Message &responseMessage,
  //                                                Database &db, const Context &context,  Partitioner *partitioner,
  //                                                Transaction *txn) {

  //   DCHECK(inputPiece.get_message_type() ==
  //          static_cast<uint32_t>(MyClayMessage::RELEASE_READ_LOCK_RESPONSE));
  //   auto table_id = inputPiece.get_table_id();
  //   auto partition_id = inputPiece.get_partition_id();

  //   ITable &table = *db.find_table(table_id, partition_id);   

  //   auto key_size = table.key_size();

  //   /*
  //    * The structure of a release read lock response: ()
  //    */

  //   txn->pendingResponses--;
  //   txn->network_size += inputPiece.get_message_length();
  //   VLOG(DEBUG_V16) << " RELEASE_READ_LOCK_RESPONSE  "; // << *(int*)key; // << " " << success;

  // }

  static void release_write_lock_request_handler(MessagePiece inputPiece,
                                                 Message &responseMessage,
                                                 Database &db, const Context &context,  Partitioner *partitioner,
                                                 Transaction *txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::RELEASE_WRITE_LOCK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);   

    auto key_size = table.key_size();
    /*
     * The structure of a release write lock request: (primary key, commit tid)
     * The structure of a release write lock response: ()
     */

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(uint64_t));

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);

    uint64_t commit_tid;
    Decoder dec(stringPiece);
    dec >> commit_tid;

    std::atomic<uint64_t> &tid = table.search_metadata(key);
    TwoPLHelper::write_lock_release(tid, commit_tid);

    // prepare response message header
    // auto message_size = MessagePiece::get_header_size();
    // auto message_piece_header = MessagePiece::construct_message_piece_header(
    //     static_cast<uint32_t>(MyClayMessage::RELEASE_WRITE_LOCK_RESPONSE),
    //     message_size, table_id, partition_id);

    // star::Encoder encoder(responseMessage.data);
    // encoder << message_piece_header;
    // responseMessage.flush();

    VLOG(DEBUG_V16) << " RELEASE_WRITE_LOCK_REQUEST  " << *(int*)key; // << " " << success;
  }

  // static void release_write_lock_response_handler(MessagePiece inputPiece,
  //                                                 Message &responseMessage,
  //                                                 Database &db, const Context &context,  Partitioner *partitioner,
  //                                                 Transaction *txn) {
  //   DCHECK(inputPiece.get_message_type() ==
  //          static_cast<uint32_t>(MyClayMessage::RELEASE_WRITE_LOCK_RESPONSE));
  //   auto table_id = inputPiece.get_table_id();
  //   auto partition_id = inputPiece.get_partition_id();
  //   ITable &table = *db.find_table(table_id, partition_id);   

  //   auto key_size = table.key_size();

  //   /*
  //    * The structure of a release write lock response: ()
  //    */

  //   txn->pendingResponses--;
  //   txn->network_size += inputPiece.get_message_length();

  //   VLOG(DEBUG_V16) << " RELEASE_WRITE_LOCK_RESPONSE  ";//  << *(int*)key; // << " " << success;

  // }



  static std::vector<
      std::function<void(MessagePiece, Message &, Database &, const Context &,  Partitioner *, Transaction *)>>
  get_message_handlers() {
    std::vector<
        std::function<void(MessagePiece, Message &, Database &, const Context &,  Partitioner *, Transaction *)>>
        v;
    v.resize(static_cast<int>(ControlMessage::NFIELDS));
    v.push_back(MyClayMessageHandler::transmit_request_handler);
    v.push_back(MyClayMessageHandler::transmit_response_handler);
    v.push_back(MyClayMessageHandler::transmit_router_only_request_handler);
    v.push_back(MyClayMessageHandler::transmit_router_only_response_handler);
    // 
    v.push_back(MyClayMessageHandler::read_lock_request_handler);
    v.push_back(MyClayMessageHandler::read_lock_response_handler);
    v.push_back(MyClayMessageHandler::write_lock_request_handler);
    v.push_back(MyClayMessageHandler::write_lock_response_handler);
    v.push_back(MyClayMessageHandler::abort_request_handler);
    v.push_back(MyClayMessageHandler::write_request_handler);
    v.push_back(MyClayMessageHandler::write_response_handler);
    // 
    v.push_back(MyClayMessageHandler::replication_request_handler);
    v.push_back(MyClayMessageHandler::replication_response_handler);
    //
    v.push_back(MyClayMessageHandler::release_read_lock_request_handler);
    // v.push_back(MyClayMessageHandler::release_read_lock_response_handler);
    v.push_back(MyClayMessageHandler::release_write_lock_request_handler);
    // v.push_back(MyClayMessageHandler::release_write_lock_response_handler);

    return v;
  }
};

} // namespace star
