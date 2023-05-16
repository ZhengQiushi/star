//
// Created by Yi Lu on 9/10/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"
#include "protocol/Silo/SiloHelper.h"
#include "protocol/Silo/SiloRWKey.h"
#include "protocol/Silo/SiloTransaction.h"

namespace star {

enum class MyClayMessage {
  SEARCH_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
  SEARCH_RESPONSE,
  // 
  TRANSMIT_REQUEST,
  TRANSMIT_RESPONSE,
  TRANSMIT_ROUTER_ONLY_REQUEST,
  TRANSMIT_ROUTER_ONLY_RESPONSE,
  // 
  LOCK_REQUEST,
  LOCK_RESPONSE,
  READ_VALIDATION_REQUEST,
  READ_VALIDATION_RESPONSE,
  ABORT_REQUEST,
  WRITE_REQUEST,
  REPLICATION_REQUEST,
  REPLICATION_RESPONSE,
  NFIELDS
};

class MyClayMessageFactory {

public:
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

  static std::size_t new_search_message(Message &message, ITable &table,
                                        const void *key, uint32_t key_offset) {

    /*
     * The structure of a search request: (primary key, read key offset)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::SEARCH_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset;
    message.flush();
    return message_size;
  }

  static std::size_t new_lock_message(Message &message, ITable &table,
                                      const void *key, uint32_t key_offset) {

    /*
     * The structure of a lock request: (primary key, write key offset)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::LOCK_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset;
    message.flush();
    return message_size;
  }

  static std::size_t new_read_validation_message(Message &message,
                                                 ITable &table, const void *key,
                                                 uint32_t key_offset,
                                                 uint64_t tid) {

    /*
     * The structure of a read validation request: (primary key, read key
     * offset, tid)
     */

    auto key_size = table.key_size();

    auto message_size = MessagePiece::get_header_size() + key_size +
                        sizeof(key_offset) + sizeof(tid);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::READ_VALIDATION_REQUEST),
        message_size, table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset << tid;
    message.flush();
    return message_size;
  }

  static std::size_t new_abort_message(Message &message, ITable &table,
                                       const void *key) {

    /*
     * The structure of an abort request: (primary key)
     */

    auto key_size = table.key_size();

    auto message_size = MessagePiece::get_header_size() + key_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::ABORT_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    message.flush();
    return message_size;
  }

  static std::size_t new_write_message(Message &message, ITable &table,
                                       const void *key, const void *value,
                                       uint64_t commit_tid) {

    /*
     * The structure of a write request: (primary key, field value, commit_tid)
     */

    auto key_size = table.key_size();
    auto field_size = table.field_size();

    auto message_size = MessagePiece::get_header_size() + key_size +
                        field_size + sizeof(commit_tid);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::WRITE_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
    encoder << commit_tid;
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
  using Transaction = SiloTransaction;
  using Context = typename Database::ContextType;

public:
  static void search_request_handler(MessagePiece inputPiece,
                                     Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                     Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::SEARCH_REQUEST));
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
    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset;

    DCHECK(dec.size() == 0);

    bool success = true;
    uint64_t tid = 0;

    if(!table.contains(key)){
      success = false;
    }
    VLOG(DEBUG_V14) << "request handler : " << *(int*)key << "success = " << success << " " << responseMessage.get_source_node_id() << " -> " << responseMessage.get_dest_node_id();
    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + value_size +
                        sizeof(success) +
                        sizeof(uint64_t) + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::SEARCH_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;

    // reserve size for read
    responseMessage.data.append(value_size, 0);
    if(success){
      auto row = table.search(key);
      void *dest =
          &responseMessage.data[0] + responseMessage.data.size() - value_size;
      // read to message buffer
      tid = SiloHelper::read(row, dest, value_size);
    }

    encoder << success << tid << key_offset;
    responseMessage.flush();

  }

  static void search_response_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                      Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::SEARCH_RESPONSE));
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
    bool success;

    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                  value_size + sizeof(success) + sizeof(tid) +
                                                  sizeof(key_offset));

    StringPiece stringPiece = inputPiece.toStringPiece();
    stringPiece.remove_prefix(value_size);
    Decoder dec(stringPiece);
    dec >> success >> tid >> key_offset;

    SiloRWKey &readKey = txn->readSet[key_offset];
    if(success){
      dec = Decoder(inputPiece.toStringPiece());
      dec.read_n_bytes(readKey.get_value(), value_size);
      readKey.set_tid(tid);
    }
    VLOG(DEBUG_V14) << "response handler : " << *(int*)readKey.get_key() << " success = " << success << " " << responseMessage.get_source_node_id() << " -> " << responseMessage.get_dest_node_id();

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
    
    if(!success){
      txn->abort_lock = true;
    }
  }

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
    latest_tid = SiloHelper::lock(tid, success); // be locked 

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
        SiloHelper::read(row, dest, value_size);
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
    SiloHelper::unlock(tid);
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
    
    SiloRWKey &readKey = txn->readSet[key_offset];
    auto key = readKey.get_key();

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

      last_tid = SiloHelper::lock(tid_, success);
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
      DCHECK(coordinator_id_new != coordinator_id_old);

      // LOG(INFO) << table_id <<" " << *(int*) key << " " << (char*)value << " transmit reponse switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << tid << "  " << remaster;

      // update router
      router_val->set_dynamic_coordinator_id(coordinator_id_new);
      router_val->set_secondary_coordinator_id(coordinator_id_new);

      readKey.set_dynamic_coordinator_id(coordinator_id_new);
      readKey.set_router_value(router_val->get_dynamic_coordinator_id(), router_val->get_secondary_coordinator_id());
      readKey.set_read_respond_bit();
      readKey.set_tid(tid); // original tid for lock release

      SiloHelper::unlock(tid_);
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
      // LOG(INFO) << *(int*) key << " delete " << coordinator_id_old << " --> " << coordinator_id_new;

      router_val->set_dynamic_coordinator_id(coordinator_id_new);// (key, &coordinator_id_new);
      router_val->set_secondary_coordinator_id(coordinator_id_new);
    }

    // if(context.coordinator_id == context.coordinator_num){
    //   LOG(INFO) << "transmit_router_only_request_handler : " << *(int*)key << " " << coordinator_id_old << " -> " << coordinator_id_new;
    // }
    responseMessage.flush();
  }

  static void transmit_router_only_response_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                      Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::TRANSMIT_ROUTER_ONLY_RESPONSE));

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
  }

  static void lock_request_handler(MessagePiece inputPiece,
                                   Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                   Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::LOCK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);    
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of a lock request: (primary key, write key offset)
     * The structure of a lock response: (success?, tid, write key offset)
     */

    auto stringPiece = inputPiece.toStringPiece();

    uint32_t key_offset;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(key_offset));

    const void *key = stringPiece.data();
    std::atomic<uint64_t> &tid = table.search_metadata(key);

    bool success;
    uint64_t latest_tid = SiloHelper::lock(tid, success);

    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset;

    DCHECK(dec.size() == 0);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + sizeof(bool) +
                        sizeof(uint64_t) + sizeof(uint32_t);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::LOCK_RESPONSE), message_size,
        table_id, partition_id);

    VLOG(DEBUG_V14) << "      lock_request_handler : " << *(int*)key << " " << responseMessage.get_source_node_id() << " -> " << responseMessage.get_dest_node_id();

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    encoder << success << latest_tid << key_offset;
    responseMessage.flush();
  }

  static void lock_response_handler(MessagePiece inputPiece,
                                    Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                    Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::LOCK_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);    
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of a lock response: (success?, tid, write key offset)
     */

    bool success;
    uint64_t latest_tid;
    uint32_t key_offset;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + sizeof(success) +
               sizeof(latest_tid) + sizeof(key_offset));

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> success >> latest_tid >> key_offset;

    DCHECK(dec.size() == 0);

    SiloRWKey &readKey = txn->readSet[key_offset];

    bool tid_changed = false;
    VLOG(DEBUG_V14) << "      lock_response_handler : " << *(int*)readKey.get_key() << " " << responseMessage.get_source_node_id() << " -> " << responseMessage.get_dest_node_id() << " " << txn->pendingResponses;

    if (success) {

      // SiloRWKey *readKey = txn->get_read_key(readKey.get_key());

      // DCHECK(readKey != nullptr);

      uint64_t tid_on_read = readKey.get_tid();

      if (latest_tid != tid_on_read) {
        tid_changed = true;
      }

      readKey.set_tid(latest_tid);
      readKey.set_write_lock_bit();
    }

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();

    if (!success || tid_changed) {
      txn->abort_lock = true;
    }
  }

  static void read_validation_request_handler(MessagePiece inputPiece,
                                              Message &responseMessage,
                                              Database &db, const Context &context,  Partitioner *partitioner, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::READ_VALIDATION_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);    
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of a read validation request: (primary key, read key
     * offset, tid) The structure of a read validation response: (success?, read
     * key offset)
     */

    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                  key_size + sizeof(uint32_t) +
                                                  sizeof(uint64_t));

    auto stringPiece = inputPiece.toStringPiece();
    const void *key = stringPiece.data();
    auto latest_tid = table.search_metadata(key).load();
    stringPiece.remove_prefix(key_size);

    uint32_t key_offset;
    uint64_t tid;
    Decoder dec(stringPiece);
    dec >> key_offset >> tid;

    bool success = true;

    if (SiloHelper::remove_lock_bit(latest_tid) != tid) {
      success = false;
    }

    if (SiloHelper::is_locked(latest_tid)) { // must be locked by others
      success = false;
    }

    // prepare response message header
    auto message_size =
        MessagePiece::get_header_size() + sizeof(bool) + sizeof(uint32_t);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(MyClayMessage::READ_VALIDATION_RESPONSE),
        message_size, table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    encoder << success << key_offset;

    responseMessage.flush();
  }

  static void read_validation_response_handler(MessagePiece inputPiece,
                                               Message &responseMessage,
                                               Database &db, const Context &context,  Partitioner *partitioner,
                                               Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(MyClayMessage::READ_VALIDATION_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);    
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of a read validation response: (success?, read key offset)
     */

    bool success;
    uint32_t key_offset;

    Decoder dec(inputPiece.toStringPiece());

    dec >> success >> key_offset;

    SiloRWKey &readKey = txn->readSet[key_offset];

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();

    if (!success) {
      txn->abort_read_validation = true;
    }
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
     * The structure of an abort request: (primary key)
     * The structure of an abort response: null
     */

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size);

    auto stringPiece = inputPiece.toStringPiece();
    const void *key = stringPiece.data();
    std::atomic<uint64_t> &tid = table.search_metadata(key);
    if(SiloHelper::is_locked(tid)){
      // unlock the key
      SiloHelper::unlock(tid);
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
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    /*
     * The structure of a write request: (primary key, field value, commit_tid)
     * The structure of a write response: null
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
    table.deserialize_value(key, valueStringPiece);

    SiloHelper::unlock(tid, commit_tid);
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
    if(!success){
      return;
    }
    uint64_t last_tid = SiloHelper::lock(tid);

    if (commit_tid > last_tid) {
      table.deserialize_value(key, valueStringPiece);
      SiloHelper::unlock(tid, commit_tid);
    } else {
      SiloHelper::unlock(tid);
    }

    uint32_t txn_id = 0;
    int debug_key = 0;

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




  static std::vector<
      std::function<void(MessagePiece, Message &, Database &, const Context &,  Partitioner *, Transaction *)>>
  get_message_handlers() {
    std::vector<
        std::function<void(MessagePiece, Message &, Database &, const Context &,  Partitioner *, Transaction *)>>
        v;
    v.resize(static_cast<int>(ControlMessage::NFIELDS));
    v.push_back(MyClayMessageHandler::search_request_handler);
    v.push_back(MyClayMessageHandler::search_response_handler);
    // 
    v.push_back(MyClayMessageHandler::transmit_request_handler);
    v.push_back(MyClayMessageHandler::transmit_response_handler);
    v.push_back(MyClayMessageHandler::transmit_router_only_request_handler);
    v.push_back(MyClayMessageHandler::transmit_router_only_response_handler);
    // 
    v.push_back(MyClayMessageHandler::lock_request_handler);
    v.push_back(MyClayMessageHandler::lock_response_handler);
    v.push_back(MyClayMessageHandler::read_validation_request_handler);
    v.push_back(MyClayMessageHandler::read_validation_response_handler);
    v.push_back(MyClayMessageHandler::abort_request_handler);
    v.push_back(MyClayMessageHandler::write_request_handler);
    v.push_back(MyClayMessageHandler::replication_request_handler);
    v.push_back(MyClayMessageHandler::replication_response_handler);
    return v;
  }
};

} // namespace star
