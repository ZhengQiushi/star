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
#include "core/Partitioner.h"

#include "protocol/Silo/SiloHelper.h"
#include "protocol/Silo/SiloTransaction.h"


// #include "protocol/TwoPL/TwoPLHelper.h"
// #include "protocol/TwoPL/TwoPLTransaction.h"

namespace star {


enum class LionMessage {
  SEARCH_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
  SEARCH_RESPONSE,
  LOCK_REQUEST,
  LOCK_RESPONSE,
  READ_VALIDATION_REQUEST,
  READ_VALIDATION_RESPONSE,
  ABORT_REQUEST,
  WRITE_REQUEST,
  WRITE_RESPONSE,
  REPLICATION_REQUEST,
  REPLICATION_RESPONSE,
  RELEASE_LOCK_REQUEST,
  SEARCH_REQUEST_ROUTER_ONLY,
  SEARCH_RESPONSE_ROUTER_ONLY,
  SEARCH_REQUEST_READ_ONLY,
  SEARCH_RESPONSE_READ_ONLY,
  ROUTER_TRANSACTION,
  IGNORE,
  NFIELDS
};

class LionMessageFactory {
using Transaction = SiloTransaction;
public:
  static std::size_t new_search_message(Message &message, ITable &table,
                                        const void *key, uint32_t key_offset,
                                        bool remaster) {

    /*
     * The structure of a search request: (primary key, read key offset)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(key_offset) + sizeof(remaster);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionMessage::SEARCH_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset << remaster;
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
        static_cast<uint32_t>(LionMessage::LOCK_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset;
    message.flush();
    return message_size;
  }

  static std::size_t new_read_validation_message(Message &message, ITable &table, 
                                                 const void *key,
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
        static_cast<uint32_t>(LionMessage::READ_VALIDATION_REQUEST),
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
        static_cast<uint32_t>(LionMessage::ABORT_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
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
        static_cast<uint32_t>(LionMessage::WRITE_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
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
        static_cast<uint32_t>(LionMessage::REPLICATION_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
    encoder << commit_tid;
    message.flush();
    return message_size;
  }

  static std::size_t ignore_message(Message &message, ITable &table,
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
        static_cast<uint32_t>(LionMessage::IGNORE), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
    encoder << commit_tid;
    message.flush();
    return message_size;
  }

  static std::size_t new_release_lock_message(Message &message, ITable &table,
                                              const void *key,
                                              uint64_t commit_tid) {
    /*
     * The structure of a replication request: (primary key, commit tid)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(commit_tid);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionMessage::RELEASE_LOCK_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << commit_tid;
    message.flush();
    return message_size;
  }


  static std::size_t new_search_router_only_message(Message &message, ITable &table,
                                        const void *key, uint32_t key_offset) {

    /*
     * The structure of a search request: (primary key, read key offset)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionMessage::SEARCH_REQUEST_ROUTER_ONLY), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset;
    message.flush();
    return message_size;
  }

  static std::size_t new_search_read_only_message(Message &message, ITable &table,
                                        const void *key, uint32_t key_offset) {

    /*
     * The structure of a search request: (primary key, read key offset)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionMessage::SEARCH_REQUEST_READ_ONLY), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset;
    message.flush();
    return message_size;
  }


  static std::size_t new_router_transaction_message(Message &message, int table_id, 
                                                    Transaction *txn, uint64_t op){
    // 
    auto update_ = txn->get_query_update();
    auto key_ = txn->get_query();
    uint64_t txn_size = (uint64_t)key_.size();
    auto key_size = sizeof(uint64_t);
    
    auto message_size =
        MessagePiece::get_header_size() + sizeof(op) + sizeof(txn_size) + (key_size + sizeof(bool)) * txn_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionMessage::ROUTER_TRANSACTION), message_size,
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
};

template <class Database> class LionMessageHandler {
  using Transaction = SiloTransaction;
  using Context = typename Database::ContextType;

public:
  static void search_request_handler(MessagePiece inputPiece,
                                     Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                     Transaction *txn,
                                     std::deque<simpleTransaction>* router_txn_queue
) {
    /**
     * @brief directly move the data to the request node!
     *
     * | 0 | 1 | 2 |    x => static replica; o => dynamic replica
     * | x | o |   | <- imagine current coordinator_id = 1
     * |   | x | o |    request from N0
     * | o |   | x |      remaster / transfer both ok
     *                  request from N2
     *                    remaster NO! transfer only
     *                    
     */
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::SEARCH_REQUEST));
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
    bool remaster; // add by truth 22-04-22
    bool success;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(key_offset) + sizeof(remaster));

    // get row and offset
    const void *key = stringPiece.data();
    auto row = table.search(key);

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
        static_cast<uint32_t>(LionMessage::SEARCH_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;


    std::atomic<uint64_t> &tid = table.search_metadata(key);
    // try to lock tuple. Ensure not locked by current node
    uint64_t latest_tid = SiloHelper::lock(tid, success);



    if(success == true){
      // lock the router_table 
      if(partitioner->is_dynamic()){
        // 该数据原来的位置
        auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key);
        auto router_table_old = db.find_router_table(table_id, coordinator_id_old);
        
        uint64_t old_secondary_coordinator_id = *(uint64_t*)router_table_old->search_value(key);
        uint64_t static_coordinator_id = partition_id % context.coordinator_num; //; partitioner->secondary_coordinator(partition_id);

        // create the new tuple in global router of source request node
        auto coordinator_id_new = responseMessage.get_dest_node_id(); 
        if(coordinator_id_new != coordinator_id_old){
          DCHECK(coordinator_id_new != coordinator_id_old);

          auto router_table_new = db.find_router_table(table_id, coordinator_id_new);

          VLOG(DEBUG_V8) << table_id <<" " << *(int*) key << " request switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << tid.load() << " " << latest_tid;

          size_t new_secondary_coordinator_id;
          // be related with static replica
          // 涉及 静态副本 
          if(coordinator_id_new == static_coordinator_id || 
            coordinator_id_old == static_coordinator_id ){ 
            // local is or new destination has a replica 
            if(coordinator_id_new == static_coordinator_id) {
              // move to static replica 迁移到 静态副本, 
              // 动态从副本 为 原来的位置
              new_secondary_coordinator_id = coordinator_id_old;
            } else {
              // 从静态副本迁出
              // 动态从副本 为 静态副本的位置
              new_secondary_coordinator_id = static_coordinator_id;
            }

            // 从静态副本迁出，且迁出地不是动态从副本的地方！
            //!TODO 需要删除原从副本 
            //

          } else {
            // only transfer can reach
            if(remaster == true){
              DCHECK(false);
            }
            // 非静态 迁到 新的非静态
            // 1. delete old
            table.delete_(key);
            // LOG(INFO) << *(int*) key << " delete " << coordinator_id_old << " --> " << coordinator_id_new;
            // 2. 
            new_secondary_coordinator_id = static_coordinator_id;

          }
          // 修改路由表
          router_table_new->insert(key, &new_secondary_coordinator_id); // 
          // delete old value in router and real-replica
          router_table_old->delete_(key);
        } else if(coordinator_id_new == coordinator_id_old) {
          success = false;
          VLOG(DEBUG_V12) << "same coordi : " << coordinator_id_new << " " <<coordinator_id_old << " " << *(int*)key << " " << tid;
        } else {
          DCHECK(false);
        }

      } else {
        // LOG(INFO) << *(int*) key << "s-delete "; // coordinator_id_old << " --> " << coordinator_id_new;
      }
      // wait for the commit / abort to unlock
      SiloHelper::unlock(tid);
    } else {
      VLOG(DEBUG_V12) << "  can't Lock " << *(int*)key << " " << tid;
    }

    encoder << tid << key_offset << success << remaster;

    // reserve size for read
    responseMessage.data.append(value_size, 0);

    if(remaster == false){
      // transfer: read from db and load data into message buffer
      void *dest =
          &responseMessage.data[0] + responseMessage.data.size() - value_size;
      SiloHelper::read(row, dest, value_size);
    }
    responseMessage.flush();
  }

  static void search_response_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                      Transaction *txn,
std::deque<simpleTransaction>* router_txn_queue
) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::SEARCH_RESPONSE));
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
    bool remaster;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> tid >> key_offset >> success >> remaster;

    if(remaster == true){
      value_size = 0;
    }

    stringPiece = inputPiece.toStringPiece();
    stringPiece.remove_prefix(value_size);

    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                  sizeof(tid) +
                                                  sizeof(key_offset) + sizeof(success) + sizeof(remaster) + 
                                                  value_size);

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
    
    SiloRWKey &readKey = txn->readSet[key_offset];
    auto key = readKey.get_key();

    bool is_exist = table.contains(key);
    bool can_be_locked = true;

    uint64_t tidd = 0;

    if(is_exist){
      tidd = table.search_metadata(key).load();
      can_be_locked = !SiloHelper::is_locked(tidd);
    }

    if(success == true && can_be_locked){

      if(remaster == true){
        // read from local
        auto value = table.search_value(key);
        readKey.set_value(value);
        readKey.set_tid(tid);
      } else {
        // transfer read from message piece
        dec = Decoder(inputPiece.toStringPiece());
        dec.read_n_bytes(readKey.get_value(), value_size);
        readKey.set_tid(tid);
      }
      
      // insert remote tuple to local replica 
      if(partitioner->is_dynamic()){
        // key && value
        auto key = readKey.get_key();
        auto value = readKey.get_value();

        auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key);
        auto router_table_old = db.find_router_table(table_id, coordinator_id_old);
        
        uint64_t old_secondary_coordinator_id = *(uint64_t*)router_table_old->search_value(key);
        uint64_t static_coordinator_id = partition_id % context.coordinator_num; //; partitioner->secondary_coordinator(partition_id);

        // create the new tuple in global router of source request node
        auto coordinator_id_new = responseMessage.get_source_node_id(); 
        DCHECK(coordinator_id_new != coordinator_id_old);
        auto router_table_new = db.find_router_table(table_id, coordinator_id_new);

        VLOG(DEBUG_V8) << table_id <<" " << *(int*) key << " reponse switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << tidd;
        // for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
        //   ITable* tab = db.find_router_table(table_id, i);
        //   LOG(INFO) << i << ": " << tab->table_record_num();
        // }


        // 
        size_t new_secondary_coordinator_id;
        // be related with static replica
        // 涉及 静态副本 
        if(coordinator_id_new == static_coordinator_id || 
           coordinator_id_old == static_coordinator_id ){ 
          // local is or new destination has a replica 
          if(coordinator_id_new == static_coordinator_id) {
            // move to static replica 迁移到 静态副本, 
            // 动态从副本 为 原来的位置
            new_secondary_coordinator_id = coordinator_id_old;
          } else {
            // 从静态副本迁出
            // 动态从副本 为 静态副本的位置
            new_secondary_coordinator_id = static_coordinator_id;
          }

          // 从静态副本迁出，且迁出地不是动态从副本的地方！
          if(coordinator_id_old == static_coordinator_id && 
             coordinator_id_new != old_secondary_coordinator_id){
            //!TODO 现在可能会重复插入
            if(remaster == true){
              // 不应该进入
              DCHECK(false);
            }
            table.insert(key, value); 
            // LOG(INFO) << *(int*) key << " insert " << coordinator_id_old << " --> " << coordinator_id_new;

          }

        } else {
          // 非静态 迁到 新的非静态
          // 1. insert new
          if(remaster == true){
            // 不应该进入
            DCHECK(false);
          }
          table.insert(key, value); 
          // LOG(INFO) << *(int*) key << " insert " << coordinator_id_old << " --> " << coordinator_id_new;

          // 2. 
          new_secondary_coordinator_id = static_coordinator_id;
        }

        // 修改路由表
        router_table_new->insert(key, &new_secondary_coordinator_id);
        router_table_old->delete_(key);

        // update txn->writekey dynamic coordinator_id
        readKey.set_dynamic_coordinator_id(coordinator_id_new);
        SiloRWKey *writeKey = txn->get_write_key(readKey.get_key());
        if(writeKey != nullptr){
          writeKey->set_dynamic_coordinator_id(coordinator_id_new);
          writeKey->set_dynamic_secondary_coordinator_id(new_secondary_coordinator_id);
        }
      }

    } else {
      VLOG(DEBUG_V14) << "FAILED TO GET LOCK : " << *(int*)key << " " << tidd;
      txn->abort_lock = true;
    }

  }

  static void lock_request_handler(MessagePiece inputPiece,
                                   Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                   Transaction *txn,
std::deque<simpleTransaction>* router_txn_queue
) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::LOCK_REQUEST));
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
    // LOG(INFO) << "lock_request_handler : " << *(int*)key ;

    bool success;
    uint64_t latest_tid = SiloHelper::lock(tid, success);
    VLOG(DEBUG_V14) << "LOCK " << *(int*)key;
    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset;

    DCHECK(dec.size() == 0);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + sizeof(bool) +
                        sizeof(uint64_t) + sizeof(uint32_t);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionMessage::LOCK_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    encoder << success << latest_tid << key_offset;
    responseMessage.flush();
  }

  static void lock_response_handler(MessagePiece inputPiece,
                                    Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                    Transaction *txn,
std::deque<simpleTransaction>* router_txn_queue
) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::LOCK_RESPONSE));
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

    SiloRWKey &writeKey = txn->writeSet[key_offset];

    bool tid_changed = false;

    if (success) {

      SiloRWKey *readKey = txn->get_read_key(writeKey.get_key());

      DCHECK(readKey != nullptr);

      uint64_t tid_on_read = readKey->get_tid();

      if (latest_tid != tid_on_read) {
        tid_changed = true;
      }

      writeKey.set_tid(latest_tid);
      writeKey.set_write_lock_bit();
    }
    txn->pendingResponses--;

    // LOG(INFO) << "lock_response_handler : " << *(int*)key << " " << txn->pendingResponses;
    txn->network_size += inputPiece.get_message_length();

    if (!success || tid_changed) {
      txn->abort_lock = true;
    }
  }

  static void read_validation_request_handler(MessagePiece inputPiece,
                                              Message &responseMessage,
                                              Database &db, const Context &context,  Partitioner *partitioner, 
                                              Transaction *txn,
std::deque<simpleTransaction>* router_txn_queue
) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::READ_VALIDATION_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);

    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    // LOG(INFO) << "read_validation_request_handler : " << *(int*)key ;
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
        static_cast<uint32_t>(LionMessage::READ_VALIDATION_RESPONSE),
        message_size, table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    encoder << success << key_offset;

    responseMessage.flush();
  }

  static void read_validation_response_handler(MessagePiece inputPiece,
                                               Message &responseMessage,
                                               Database &db, const Context &context,  Partitioner *partitioner,
                                               Transaction *txn,
std::deque<simpleTransaction>* router_txn_queue
) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::READ_VALIDATION_RESPONSE));
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
    // LOG(INFO) << "read_validation_response_handler : " << *(int*)key << " " << txn->pendingResponses;

    if (!success) {
      txn->abort_read_validation = true;
    }
  }

  static void abort_request_handler(MessagePiece inputPiece,
                                    Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                    Transaction *txn,
std::deque<simpleTransaction>* router_txn_queue
) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::ABORT_REQUEST));
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

    // unlock the key
    SiloHelper::unlock_if_locked(tid);

    if(partitioner->is_dynamic()){
      // unlock dynamic replica
      auto router_table = db.find_router_table(table_id, responseMessage.get_source_node_id());
      std::atomic<uint64_t> &tid = router_table->search_metadata(key);
      SiloHelper::unlock_if_locked(tid);
    }
  }

  static void write_request_handler(MessagePiece inputPiece,
                                    Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                    Transaction *txn,
std::deque<simpleTransaction>* router_txn_queue
) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::WRITE_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
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
        static_cast<uint32_t>(LionMessage::WRITE_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    responseMessage.flush();
  }

  static void write_response_handler(MessagePiece inputPiece,
                                     Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                     Transaction *txn,
std::deque<simpleTransaction>* router_txn_queue
) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::WRITE_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);

    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());

    /*
     * The structure of a write response: ()
     */

    // txn->pendingResponses--;
    // txn->network_size += inputPiece.get_message_length();
  }

  static void replication_request_handler(MessagePiece inputPiece,
                                          Message &responseMessage,
                                          Database &db, const Context &context,  Partitioner *partitioner, 
                                          Transaction *txn,
std::deque<simpleTransaction>* router_txn_queue
) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::REPLICATION_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);

    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    /*
     * The structure of a replication request: (primary key, field value,
     * commit_tid).
     * The structure of a replication response: null
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

    bool success;
    SiloHelper::lock(tid, success);
    while(success != true){
      std::this_thread::yield();
      // 说明local data 本来就已经被locked，会被 Thomas write rule 覆盖... 
      SiloHelper::lock(tid, success);
    }

    //! TODO logic needs to be checked
    // DCHECK(last_tid < commit_tid);
    table.deserialize_value(key, valueStringPiece);
    SiloHelper::unlock(tid, commit_tid);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionMessage::REPLICATION_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    responseMessage.flush();
  }

  static void replication_response_handler(MessagePiece inputPiece,
                                           Message &responseMessage,
                                           Database &db, const Context &context,  Partitioner *partitioner,
                                           Transaction *txn,
std::deque<simpleTransaction>* router_txn_queue
) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::REPLICATION_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);

    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of a replication response: ()
     */

    // txn->pendingResponses--;
    // txn->network_size += inputPiece.get_message_length();
  }

  static void release_lock_request_handler(MessagePiece inputPiece,
                                           Message &responseMessage, 
                                           Database &db, const Context &context,  Partitioner *partitioner, 
                                           Transaction *txn,
std::deque<simpleTransaction>* router_txn_queue
) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::RELEASE_LOCK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);

    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of a release lock request: (primary key, commit tid)
     * The structure of a write response: null
     */

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(uint64_t));

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);

    uint64_t commit_tid;
    Decoder dec(stringPiece);
    dec >> commit_tid;
    DCHECK(dec.size() == 0);

    std::atomic<uint64_t> &tid = table.search_metadata(key);
    SiloHelper::unlock(tid, commit_tid);

    if(partitioner->is_dynamic()){
      // unlock dynamic replica
      auto router_table = db.find_router_table(table_id, responseMessage.get_source_node_id());
      std::atomic<uint64_t> &tid = router_table->search_metadata(key);
      // bool success;
      SiloHelper::unlock_if_locked(tid);
      LOG(INFO) << *(int*) key << "release lock";
      // if(!success){
      //   LOG(WARNING) << *(int*)key << " not locked before";
      // }
    }
  }



  static void search_request_router_only_handler(MessagePiece inputPiece,
                                     Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                     Transaction *txn,
std::deque<simpleTransaction>* router_txn_queue
) {
    /**
     * @brief directly move the data to the request node!
     * 修改其他机器上的路由情况， 当前机器不涉及该事务的处理，可以认为事务涉及的数据主节点都不在此，直接处理就可以
     * 
     */
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::SEARCH_REQUEST_ROUTER_ONLY));
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
        static_cast<uint32_t>(LionMessage::SEARCH_RESPONSE_ROUTER_ONLY), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;

    // reserve size for read
    responseMessage.data.append(value_size, 0);
    uint64_t tid = 0; // auto tid = SiloHelper::read(row, dest, value_size);

    encoder << tid << key_offset;

    // lock the router_table 
    if(partitioner->is_dynamic()){
      auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key);
      auto router_table_old = db.find_router_table(table_id, coordinator_id_old);
      
      // preemption
      // bool success;
      // std::atomic<uint64_t> &tid_r = router_table_old->search_metadata(key);
      // uint64_t latest_tid = SiloHelper::lock(tid_r, success);

      // if(success == false){
      //   txn->abort_lock = true;
      // }

      // create the new tuple in global router of source request node
      auto coordinator_id_new = responseMessage.get_dest_node_id(); 
      if(coordinator_id_new != coordinator_id_old){
        auto router_table_new = db.find_router_table(table_id, coordinator_id_new);

        // LOG(INFO) << *(int*) key << " delete " << coordinator_id_old << " --> " << coordinator_id_new;

        router_table_new->insert(key, &coordinator_id_new);
        // std::atomic<uint64_t> &tid_r_new = router_table_new->search_metadata(key);
        // SiloHelper::lock(tid_r_new); // locked, not available so far

        // delete old value in router and real-replica
        router_table_old->delete_(key);
        
        // SiloHelper::unlock(tid_r_new); 
      }

    } else {

      // LOG(INFO) << *(int*) key << "s-delete "; // coordinator_id_old << " --> " << coordinator_id_new;

    }

    responseMessage.flush();
  }

  static void search_response_router_only_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                      Transaction *txn,
std::deque<simpleTransaction>* router_txn_queue
) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::SEARCH_RESPONSE_ROUTER_ONLY));

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();

  }


  static void search_request_original_handler(MessagePiece inputPiece,
                                     Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                     Transaction *txn,
std::deque<simpleTransaction>* router_txn_queue
) {
    /**
     * @brief directly move the data to the request node!
     *  最原始的只读
     * 
     */
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::SEARCH_REQUEST_READ_ONLY));
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
    auto row = table.search(key);

    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset; // index offset in the readSet from source request node

    DCHECK(dec.size() == 0);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + value_size +
                        sizeof(uint64_t) + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionMessage::SEARCH_RESPONSE_READ_ONLY), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;

    // reserve size for read
    responseMessage.data.append(value_size, 0);
    void *dest =
        &responseMessage.data[0] + responseMessage.data.size() - value_size;
    // read to message buffer
    auto tid = SiloHelper::read(row, dest, value_size);

    encoder << tid << key_offset;

    responseMessage.flush();
  }

  static void search_response_original_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                      Transaction *txn,
                                      std::deque<simpleTransaction>* router_txn_queue
) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::SEARCH_RESPONSE_READ_ONLY));
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

    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                  value_size + sizeof(tid) +
                                                  sizeof(key_offset));

    StringPiece stringPiece = inputPiece.toStringPiece();
    stringPiece.remove_prefix(value_size);
    Decoder dec(stringPiece);
    dec >> tid >> key_offset;

    SiloRWKey &readKey = txn->readSet[key_offset];
    dec = Decoder(inputPiece.toStringPiece());
    dec.read_n_bytes(readKey.get_value(), value_size);
    readKey.set_tid(tid);
    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();

  }


  static void router_transaction_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                      Transaction *txn,
                                      std::deque<simpleTransaction>* router_txn_queue
) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::ROUTER_TRANSACTION));

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
    router_txn_queue->push_back(new_router_txn);
  }


  static void ignore_handler(MessagePiece inputPiece,
                                           Message &responseMessage,
                                           Database &db, const Context &context,  Partitioner *partitioner,
                                           Transaction *txn,
std::deque<simpleTransaction>* router_txn_queue
) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::IGNORE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    ITable &table = *db.find_table(table_id, partition_id);

    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of a replication response: ()
     */

    // txn->pendingResponses--;
    // txn->network_size += inputPiece.get_message_length();
  }

  static std::vector<
      std::function<void(MessagePiece, Message &,  Database &, const Context &, Partitioner *, 
                         Transaction *, 
                         std::deque<simpleTransaction>* )>>
  get_message_handlers() {

    std::vector<
        std::function<void(MessagePiece, Message &,  Database &, const Context &, Partitioner *, 
                           Transaction *,
                           std::deque<simpleTransaction>* )>>
        v;
    v.resize(static_cast<int>(ControlMessage::NFIELDS));
    v.push_back(search_request_handler);
    v.push_back(search_response_handler);
    v.push_back(lock_request_handler);
    v.push_back(lock_response_handler);
    v.push_back(read_validation_request_handler);
    v.push_back(read_validation_response_handler);
    v.push_back(abort_request_handler);
    v.push_back(write_request_handler);
    v.push_back(write_response_handler);
    v.push_back(replication_request_handler);
    v.push_back(replication_response_handler);
    v.push_back(release_lock_request_handler);
    v.push_back(search_request_router_only_handler);
    v.push_back(search_response_router_only_handler);
    v.push_back(search_request_original_handler);
    v.push_back(search_response_original_handler);
    v.push_back(router_transaction_handler);
    v.push_back(ignore_handler);
    return v;
  }
};

} // namespace star