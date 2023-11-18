//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"
#include "protocol/Hermes/HermesRWKey.h"
#include "protocol/Hermes/HermesTransaction.h"

namespace star {

enum class HermesMessage {
  READ_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
  ASYNC_SEARCH_REQUEST,
  ASYNC_SEARCH_RESPONSE,
  TRANSFER_REQUEST,
  TRANSFER_RESPONSE,
  TRANSFER_REQUEST_ROUTER_ONLY,
  TRANSFER_RESPONSE_ROUTER_ONLY,
  NFIELDS
};

template <class Database> class HermesMessageFactory {
  using Transaction = HermesTransaction;
  using Context = typename Database::ContextType;
public:
  static std::size_t new_read_message(Message &message, ITable &table,
                                      uint32_t tid, uint32_t key_offset,
                                      const void* key, const void *value) {

    /*
     * The structure of a read request: (tid, key offset, value)
     */
    DCHECK(false);
    auto value_size = table.value_size();

    auto message_size = MessagePiece::get_header_size() + sizeof(tid) +
                        sizeof(key_offset) + value_size;

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HermesMessage::READ_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << tid << key_offset;
    encoder.write_n_bytes(value, value_size);
    message.flush();
  
    VLOG(DEBUG_V12) << "     new_read_message: " << tid << 
                 " Send " << *(int*) key << " " << 
                 message.get_source_node_id() << "->" << message.get_dest_node_id();

    return message_size;
  }

  static std::size_t new_async_search_message(Message &message, ITable &table,
                                        const void *key, 
                                        uint32_t tid,
                                        uint32_t key_offset, 
                                        int replica_id) {

    /*
     * The structure of a search request: (primary key, read key offset)
     */

    auto key_size = table.key_size();

    HermesMessage message_type = HermesMessage::ASYNC_SEARCH_REQUEST;

    VLOG(DEBUG_V8) << "HermesMessage::ASYNC_SEARCH_REQUEST: " << *(int*)key << " " << message.get_source_node_id() << " " << message.get_dest_node_id();

    auto message_size =
        MessagePiece::get_header_size() 
        + sizeof(tid) 
        + sizeof(key_offset) 
        + sizeof(replica_id)
        + key_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(message_type), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << tid << key_offset << replica_id;
    encoder.write_n_bytes(key, key_size);
    message.flush();
    return message_size;
  }


  static std::size_t transfer_request_message(Message &message, 
                                          Database &db, const Context &context, Partitioner *partitioner,
                                          ITable &table, uint32_t key_offset,
                                          Transaction& txn, bool& success){
    // 
    auto &readKey = txn.readSet[key_offset];
    auto key = readKey.get_key();
    auto partition_id = readKey.get_partition_id();
    uint32_t txn_id = txn.id;
    int replica_id = txn.on_replica_id;

    // 该数据原来的位置
    auto table_id = table.tableID();
    auto value_size = table.value_size();

    auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key, replica_id);
    auto router_table = db.find_router_table(table_id, replica_id);
    auto router_val = (RouterValue*)router_table->search_value(key);
    
    // create the new tuple in global router of source request node
    auto coordinator_id_new = message.get_dest_node_id();
    uint64_t tid_int, latest_tid;

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + 
                        sizeof(latest_tid) + sizeof(key_offset) + sizeof(txn_id) + 
                        value_size;
    
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HermesMessage::TRANSFER_REQUEST), message_size,
        table_id, partition_id);

    star::Encoder encoder(message.data);
    encoder << message_piece_header;

    success = table.contains(key);
    if(!success){
      VLOG(DEBUG_V12) << "fail!  dont Exist " << *(int*)key ; // << " " << tid_int;
      // encoder << latest_tid << key_offset << txn_id << success << remaster;
      // message.data.append(value_size, 0);
      // message.flush();
      return message_size;
    }

    std::atomic<uint64_t> &tid = table.search_metadata(key);
    // try to lock tuple. Ensure not locked by current node
    VLOG(DEBUG_V14) << "  LOCK " << *(int*)key;
    latest_tid = HermesHelper::write_lock(tid); // be locked 
    if(!success){
      VLOG(DEBUG_V12) << "fail!  can't Lock " << *(int*)key; // << " " << tid_int;
      // encoder << latest_tid << key_offset << txn_id << success << remaster;
      // message.data.append(value_size, 0);
      // message.flush();
      return message_size;
    } else {
      VLOG(DEBUG_V12) << " Lock" << *(int*)key << " " << tid << " " << latest_tid;
    }

    bool is_delete = false;

    if(coordinator_id_new != coordinator_id_old){
      DCHECK(coordinator_id_new != coordinator_id_old);
      auto router_table = db.find_router_table(table_id, replica_id);

      char* value_transfer = new char[value_size + 1];
      auto row = table.search(key);
      HermesHelper::read(row, value_transfer, value_size);

      VLOG(DEBUG_V12) << "  DELETE " << *(int*)key << " " << coordinator_id_old << " --> " << coordinator_id_new;
      // table.delete_(key);
      is_delete = true;

      encoder << latest_tid << key_offset << txn_id;
      // reserve size for read
      message.data.append(value_size, 0);

      // transfer: read from db and load data into message buffer
      void *dest =
            &message.data[0] + message.data.size() - value_size;
      memcpy(dest, value_transfer, value_size);

      message.flush();

      // delete[] value_transfer;

      readKey.set_master_coordinator_id(coordinator_id_new);
      
      VLOG(DEBUG_V8) << table_id << " " << *(int*) key << " request switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << 
                        "new : " << coordinator_id_new;

      // 修改路由表 
      router_val->set_dynamic_coordinator_id(coordinator_id_new);
      router_val->set_secondary_coordinator_id(coordinator_id_new);

    } else if(coordinator_id_new == coordinator_id_old) {
      success = false;
      VLOG(DEBUG_V12) << "fail! Same coordi : " << coordinator_id_new << " " <<coordinator_id_old << " " << *(int*)key << " " << tid;
      // encoder << latest_tid << key_offset << txn_id << success << remaster;
      // message.data.append(value_size, 0);
      // message.flush();   
    } else {
      DCHECK(false);
    }

    // wait for the commit / abort to unlock
    if(is_delete == false){
      HermesHelper::write_lock_release(tid);
      VLOG(DEBUG_V14) << "  unLOCK " << *(int*)key;
    }

    return message_size;
  }

  static std::size_t transfer_request_router_only_message(Message &message, ITable &table,
                                        Transaction& txn, uint32_t key_offset) {

    /*
     * The structure of a search request: (primary key, read key offset)
     */
    uint32_t txn_id = txn.id;
    int replica_id = txn.on_replica_id;
    DCHECK(replica_id != -1);
    uint32_t new_coordinator_id = txn.router_coordinator_id;
    auto key = txn.readSet[key_offset].get_key();

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + sizeof(txn_id)
                                        + sizeof(key_offset)
                                        + sizeof(replica_id) 
                                        + sizeof(new_coordinator_id)
                                        + key_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HermesMessage::TRANSFER_REQUEST_ROUTER_ONLY), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << txn_id 
            << key_offset
            << replica_id << new_coordinator_id;
    encoder.write_n_bytes(key, key_size);
    message.flush();

    VLOG(DEBUG_V11) << "R   " << txn_id << " " << *(int*)key << "router send " << message.get_source_node_id() << "->" << message.get_dest_node_id() << 
                       " new_coordinator_id: " << new_coordinator_id  << " pending: " << txn.pendingResponses;

    return message_size;
  }


};

template <class Database> class HermesMessageHandler {
  using Transaction = HermesTransaction;
  using Context = typename Database::ContextType;

public:
  static void
  read_request_message_handler(MessagePiece inputPiece, Message &responseMessage,
                       Database &db, const Context &context, Partitioner *partitioner,
                       std::vector<std::unique_ptr<Transaction>> &txns) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HermesMessage::READ_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();

    /*
     * The structure of a read request: (tid, key offset, value)
     * The structure of a read response: null
     */


    uint32_t tid;
    uint32_t key_offset;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> tid >> key_offset;
    
    DCHECK(tid < txns.size());
    DCHECK(key_offset < txns[tid]->readSet.size());

    ITable &table = *db.find_table(table_id, partition_id, txns[tid]->on_replica_id);

    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());

    auto key_size = table.key_size();
    auto value_size = table.value_size();

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + sizeof(tid) + sizeof(key_offset) +
               value_size);

    HermesRWKey &readKey = txns[tid]->readSet[key_offset];
    dec.read_n_bytes(readKey.get_value(), value_size);
    txns[tid]->remote_read.fetch_add(-1);
    VLOG(DEBUG_V12) << "    read_request_message_handler : " <<  tid << " " << txns[tid]->remote_read.load() << 
                 " " << *(int*) readKey.get_key() << 
                 " " << responseMessage.get_dest_node_id() << "->" << responseMessage.get_source_node_id();
  }



  static void async_search_request_handler(MessagePiece inputPiece, Message &responseMessage,
                       Database &db, const Context &context, Partitioner *partitioner,
                       std::vector<std::unique_ptr<Transaction>> &txns
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
           static_cast<uint32_t>(HermesMessage::ASYNC_SEARCH_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    /*
     * The structure of a read request: (primary key, read key offset)
     * The structure of a read response: (value, tid, read key offset)
     */

    auto stringPiece = inputPiece.toStringPiece();
    uint32_t key_offset;
    uint32_t txn_id;
    int replica_id;

    star::Decoder dec(stringPiece);
    dec >> txn_id >> key_offset >> replica_id; // index offset in the readSet from source request node

    DCHECK(dec.size() > 0);

    // DCHECK(txn_id < txns.size());
    // DCHECK(key_offset < txns[txn_id]->readSet.size());

    // auto &txn = txns[txn_id];
    // auto &readKey = txn->readSet[key_offset];
    // key && value
    // auto key = readKey.get_key();
    // auto value = readKey.get_value();

    ITable &table = *db.find_table(table_id, partition_id, replica_id);
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + 
           sizeof(txn_id)     + 
           sizeof(key_offset) + 
           sizeof(replica_id) + 
           key_size);

    auto stringPieceKey = inputPiece.toStringPiece();
    stringPieceKey.remove_prefix(sizeof(txn_id)     + 
                                 sizeof(key_offset) + 
                                 sizeof(replica_id));
    const void *key = stringPieceKey.data();

    bool success = table.contains(key);
    uint64_t latest_tid;
    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + 
                        sizeof(txn_id) + 
                        sizeof(latest_tid) + 
                        sizeof(key_offset) + 
                        sizeof(replica_id) +
                        sizeof(success) + 
                        + key_size + value_size;

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HermesMessage::ASYNC_SEARCH_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;

    if(!success){
      LOG(INFO) << "  dont Exist " << *(int*)key ; // << " " << tid_int;
      encoder << txn_id << latest_tid << key_offset << replica_id << success;
      encoder.write_n_bytes(key, key_size);
      responseMessage.data.append(value_size, 0);
      responseMessage.flush();
      return;
    }

    std::atomic<uint64_t> &tid = table.search_metadata(key);
    // try to lock tuple. Ensure not locked by current node
    latest_tid = TwoPLHelper::write_lock(tid, success); // be locked 

    if(!success){
      LOG(INFO) << "  can't Lock " << *(int*)key; // << " " << tid_int;
      encoder << txn_id << latest_tid << key_offset << replica_id << success;
      encoder.write_n_bytes(key, key_size);
      responseMessage.data.append(value_size, 0);
      responseMessage.flush();
      return;
    } else {
      VLOG(DEBUG_V12) << " Lock " << *(int*)key << " " << tid << " " << latest_tid << " on " << replica_id;
    }
    // lock the router_table 
    // 数据所在节点的路由表
    auto router_table = db.find_router_table(table_id, replica_id); // , coordinator_id_old);
    auto router_val = (RouterValue*)router_table->search_value(key);
    
    // 数据所在节点
    auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key, replica_id);

    // create the new tuple in global router of source request node
    auto coordinator_id_new = responseMessage.get_dest_node_id(); 

    if(coordinator_id_new != coordinator_id_old){
      // 数据更新到 发req的对面
      VLOG(DEBUG_V12) << table_id <<" " << *(int*) key << " request switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << tid.load() << " " << latest_tid << " on " << replica_id; // << " static: " << static_coordinator_id
      
      router_val->set_dynamic_coordinator_id(coordinator_id_new);
      router_val->set_secondary_coordinator_id(coordinator_id_new);

      encoder << txn_id << latest_tid << key_offset << replica_id << success;
      encoder.write_n_bytes(key, key_size);
      // reserve size for read
      responseMessage.data.append(value_size, 0);
        
      // auto value = table.search_value(key);
      // LOG(INFO) << *(int*)key << " " << (char*)value << " success: " << success;
      // transfer: read from db and load data into message buffer
      void *dest =
            &responseMessage.data[0] + responseMessage.data.size() - value_size;
      auto row = table.search(key);
      HermesHelper::read(row, dest, value_size);
      responseMessage.flush();

      // table.delete_(key);

    } else if(coordinator_id_new == coordinator_id_old) {
      success = false;
      VLOG(DEBUG_V12) << " Same coordi : " << coordinator_id_new << " " <<coordinator_id_old << " " << *(int*)key << " " << tid;
      encoder << txn_id << latest_tid << key_offset << replica_id << success;
      encoder.write_n_bytes(key, key_size);
      responseMessage.data.append(value_size, 0);
      responseMessage.flush();
    
    } else {
      DCHECK(false);
    }
    // wait for the commit / abort to unlock
    HermesHelper::write_lock_release(tid);
  }

  static void async_search_response_handler(MessagePiece inputPiece, Message &responseMessage,
                       Database &db, const Context &context, Partitioner *partitioner,
                       std::vector<std::unique_ptr<Transaction>> &txns) {
    /**
     * @brief 
     * 
     * 
     */
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HermesMessage::ASYNC_SEARCH_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();

    /*
     * The structure of a read response: (value, tid, read key offset)
     */
    uint32_t txn_id;
    uint64_t tid;
    uint32_t key_offset;
    int replica_id;
    bool success;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> txn_id >> tid >> key_offset >> replica_id >> success;

    DCHECK(txn_id < txns.size());
    DCHECK(key_offset < txns[txn_id]->readSet.size());

    auto &txn = txns[txn_id];
    auto &readKey = txn->readSet[key_offset];
    // key && value
    auto value = readKey.get_value();

    ITable &table = *db.find_table(table_id, partition_id, replica_id);
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    StringPiece stringPieceKey = inputPiece.toStringPiece();
    stringPieceKey.remove_prefix(sizeof(txn_id) + 
                                 sizeof(tid)    + 
                                 sizeof(key_offset) + 
                                 sizeof(replica_id) +
                                 sizeof(success));
    const void *key = stringPieceKey.data();

    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                  sizeof(txn_id) +
                                                  sizeof(tid) +
                                                  sizeof(key_offset) + 
                                                  sizeof(replica_id) +
                                                  sizeof(success) + 
                                                  key_size + value_size);
    
    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();

    uint64_t last_tid = 0;

    VLOG(DEBUG_V8) << *(int*) key << " async_search_response_handler " << " " << txn_id << " " << replica_id;

    if(success == true){
      // read value message piece
      stringPieceKey = inputPiece.toStringPiece();
      stringPieceKey.remove_prefix(sizeof(txn_id) + 
                                sizeof(tid) + 
                                sizeof(key_offset) + 
                                sizeof(replica_id) +
                                sizeof(success) + 
                                key_size);

      const void *value = stringPieceKey.data();
      DCHECK(strlen((char*)value) > 0);
      table.insert(key, value, (void*)& tid);
      
      VLOG(DEBUG_V12) << table_id <<" " << *(int*) key << " " << (char*)value << " reponse switch " << " " << " " << tid << "  " << " | " << success << " " << " replica: " << replica_id;

      // lock the respond tid and key
      bool success = false;
      std::atomic<uint64_t> &tid_ = table.search_metadata(key, success);
      if(!success){
        VLOG(DEBUG_V14) << "AFTER REMASETER, FAILED TO GET LOCK : " << *(int*)key << " " << tid; // 
        return;
      } 
      
      auto router_table = db.find_router_table(table_id, replica_id); // , coordinator_id_old);
      auto router_val = (RouterValue*)router_table->search_value(key);

      // create the new tuple in global router of source request node
      auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key, replica_id);
      // uint64_t static_coordinator_id = partition_id % context.coordinator_num; // static replica never moves only remastered
      auto coordinator_id_new = responseMessage.get_source_node_id(); 
      // DCHECK(coordinator_id_new != coordinator_id_old);
      if(coordinator_id_new != coordinator_id_old){
        VLOG(DEBUG_V12) << table_id <<" " << *(int*) key << " reponse switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << tid;
        // update router
        router_val->set_dynamic_coordinator_id(coordinator_id_new);
        router_val->set_secondary_coordinator_id(coordinator_id_new);

        readKey.set_master_coordinator_id(coordinator_id_new);
      } else {
        VLOG(DEBUG_V12) << "[FAIL]Same?" << table_id <<" " << *(int*) key << " reponse switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << tid << " on " << replica_id;
      }
    } else {
      LOG(INFO) << "FAILED TO GET LOCK : " << *(int*)key << " " << tid; // 
      txn->abort_lock = true;
    }
  }


  static void
  transfer_request_message_handler(MessagePiece inputPiece, Message &responseMessage,
                       Database &db, const Context &context, Partitioner *partitioner,
                    //    ITable &table,
                       std::vector<std::unique_ptr<Transaction>> &txns) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HermesMessage::TRANSFER_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    
    /*
     * The structure of a read request: (tid, key offset, value)
     * The structure of a read response: null
     */
    uint32_t txn_id;
    uint32_t key_offset;
    uint64_t latest_tid;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> latest_tid >> key_offset >> txn_id;

    DCHECK(txn_id < txns.size());
    DCHECK(key_offset < txns[txn_id]->readSet.size());

    auto &txn = txns[txn_id];
    auto &readKey = txn->readSet[key_offset];
    // key && value
    auto key = readKey.get_key();
    auto value = readKey.get_value();
    int replica_id = txn->on_replica_id;

    ITable &table = *db.find_table(table_id, partition_id, txn->on_replica_id);
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto value_size = table.value_size();

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + 
           sizeof(latest_tid) + sizeof(key_offset) + sizeof(txn_id) + 
           value_size);

    uint64_t tidd = 0;
    uint64_t last_tid = 0;

    stringPiece = inputPiece.toStringPiece();
    stringPiece.remove_prefix(sizeof(latest_tid) + sizeof(key_offset) + sizeof(txn_id));

    // transfer read from message piece
    dec = Decoder(stringPiece);
    dec.read_n_bytes(readKey.get_value(), value_size);
      
    auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key, replica_id);
    auto router_table = db.find_router_table(table_id, replica_id);
    auto router_val = (RouterValue*)router_table->search_value(key);

    // create the new tuple in global router of source request node
    auto coordinator_id_new = responseMessage.get_source_node_id(); 
    DCHECK(coordinator_id_new != coordinator_id_old);

    if(!table.contains(key)){
      table.insert(key, value, (void*)& tidd); 
    }
    // 修改路由表
    router_val->set_dynamic_coordinator_id(coordinator_id_new);
    router_val->set_secondary_coordinator_id(coordinator_id_new);

    // VLOG(DEBUG_V8) << table_id <<" " << *(int*) key << " reponse switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << tidd ;
    // for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
    //   ITable* tab = db.find_router_table(table_id, i);
    //   LOG(INFO) << i << ": " << tab->table_record_num();
    // }
    readKey.set_master_coordinator_id(coordinator_id_new);

    VLOG(DEBUG_V12) << "    transfer_request_message_handler : " <<  txn_id << " " << txns[txn_id]->pendingResponses << 
                 " " << *(int*) readKey.get_key() << 
                 " " << responseMessage.get_dest_node_id() << "->" << responseMessage.get_source_node_id() << 
                 " new: " << coordinator_id_new;
      if(true) {
        // simulate cost of transmit data
        for (auto i = 0u; i < context.n_nop * 2; i++) {
          asm("nop");
        }
      } else {
          for (auto i = 0u; i < context.rn_nop; i++) {
            asm("nop");
          }
        }
    // need check
    // txns[txn_id]->remote_read.fetch_add(-1);
    // txns[txn_id]->local_read.fetch_add(1);

    txns[txn_id]->pendingResponses -- ;

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + sizeof(txn_id) + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HermesMessage::TRANSFER_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header << txn_id << key_offset;
    responseMessage.flush();
  }
  
  
    static void
  transfer_response_message_handler(MessagePiece inputPiece, Message &responseMessage,
                       Database &db, const Context &context, Partitioner *partitioner,
                    //    ITable &table,
                       std::vector<std::unique_ptr<Transaction>> &txns) {
    uint32_t txn_id;
    uint32_t key_offset;

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HermesMessage::TRANSFER_RESPONSE));

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + sizeof(txn_id) + sizeof(key_offset));    

    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> txn_id >> key_offset;

    /*
     * The structure of a replication response: ()
     */
    auto& txn = txns[txn_id];
    auto& readKey = txn->readSet[key_offset];

    ITable &table = *db.find_table(table_id, partition_id, txn->on_replica_id);

    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();  

    VLOG(DEBUG_V12) << "    transfer_response : " <<  txn_id << " " << txns[txn_id]->pendingResponses << 
                 " " << *(int*) readKey.get_key() << 
                 " " << responseMessage.get_dest_node_id() << "->" << responseMessage.get_source_node_id();
                   
  }
  


  static void transfer_request_message_router_only_handler(MessagePiece inputPiece, Message &responseMessage,
                       Database &db, const Context &context, Partitioner *partitioner,
                       std::vector<std::unique_ptr<Transaction>> &txns) {
    /**
     * @brief directly move the data to the request node!
     * 修改其他机器上的路由情况， 当前机器不涉及该事务的处理，可以认为事务涉及的数据主节点都不在此，直接处理就可以
     * 
     */
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HermesMessage::TRANSFER_REQUEST_ROUTER_ONLY));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();

    /*
     * The structure of a read request: (primary key, read key offset)
     * The structure of a read response: (value, tid, read key offset)
     */
    auto stringPiece = inputPiece.toStringPiece();
    
    uint32_t txn_id;
    uint32_t key_offset;
    int replica_id;
    uint32_t new_coordinator_id;
    // auto row = table.search(key);

    star::Decoder dec(stringPiece);
    dec >> txn_id 
        >> key_offset 
        >> replica_id 
        >> new_coordinator_id; // index offset in the readSet from source request node

    ITable &table = *db.find_table(table_id, partition_id, replica_id);
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + sizeof(txn_id) 
                                           + sizeof(key_offset)
                                           + sizeof(replica_id) + sizeof(new_coordinator_id)
                                           + key_size);

    stringPiece = inputPiece.toStringPiece();
    stringPiece.remove_prefix(sizeof(txn_id) + 
                              sizeof(key_offset) +
                              sizeof(replica_id) + 
                              sizeof(new_coordinator_id));
    // get row and offset
    const void *key = stringPiece.data();

    VLOG(DEBUG_V11) << " Req   " << txn_id << " " << *(int*)key << "response send " << responseMessage.get_source_node_id() << "->" << responseMessage.get_dest_node_id() << 
                       " new_coordinator_id: " << new_coordinator_id;

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + 
                        sizeof(key_offset) + sizeof(txn_id);

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HermesMessage::TRANSFER_RESPONSE_ROUTER_ONLY), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    encoder << key_offset << txn_id;

    // lock the router_table 
    auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key, replica_id);
    auto router_table = db.find_router_table(table_id, replica_id);
    auto router_val = (RouterValue*)router_table->search_value(key);
    // create the new tuple in global router of source request node
    auto coordinator_id_new = responseMessage.get_dest_node_id(); // new_coordinator_id; // 
    if(coordinator_id_new != coordinator_id_old){
      // delete old value in router and real-replica
        router_val->set_dynamic_coordinator_id(coordinator_id_new);
        router_val->set_secondary_coordinator_id(coordinator_id_new);
    }
    // txn->pendingResponses--;
    // VLOG(DEBUG_V11) << " Req   " << txn_id << " " << *(int*)key << "response send " << responseMessage.get_source_node_id() << "->" << responseMessage.get_dest_node_id() << " pending: " << txn->pendingResponses;

    responseMessage.flush();
  }

  static void transfer_response_message_router_only_handler(MessagePiece inputPiece, Message &responseMessage,
                       Database &db, const Context &context, Partitioner *partitioner,
                    //    ITable &table,
                       std::vector<std::unique_ptr<Transaction>> &txns) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HermesMessage::TRANSFER_RESPONSE_ROUTER_ONLY));

    auto stringPiece = inputPiece.toStringPiece();
    uint32_t key_offset;
    uint32_t txn_id; 

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + sizeof(key_offset) + sizeof(txn_id));

    star::Decoder dec(stringPiece);
    dec >> key_offset >> txn_id; // index offset in the readSet from source request node

    auto& txn = txns[txn_id];
    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
    
    auto& readKey = txn->readSet[key_offset];
    auto key = readKey.get_key();

    // VLOG(DEBUG_V8) << " TRANSFER_RESPONSE_ROUTER_ONLY   " << txn_id << " " << *(int*)key << "response get " << responseMessage.get_source_node_id() << "->" << responseMessage.get_dest_node_id() << " pending: " << txn->pendingResponses;

  }


  static std::vector<
      std::function<void(MessagePiece, Message &, 
                         Database &, const Context &, Partitioner *,
                        //  ITable &,
                         std::vector<std::unique_ptr<Transaction>> &)>>
  get_message_handlers() {
    std::vector<
        std::function<void(MessagePiece, Message &, 
                           Database &, const Context &, Partitioner *,
                        //    ITable &,
                           std::vector<std::unique_ptr<Transaction>> &)>>
        v;
    v.resize(static_cast<int>(ControlMessage::NFIELDS));
    v.push_back(read_request_message_handler); // transfer_request_message_handler
    v.push_back(async_search_request_handler); 
    v.push_back(async_search_response_handler); 
    v.push_back(transfer_request_message_handler); // 
    v.push_back(transfer_response_message_handler);
    v.push_back(transfer_request_message_router_only_handler);
    v.push_back(transfer_response_message_router_only_handler);
    return v;
  }
};

} // namespace star