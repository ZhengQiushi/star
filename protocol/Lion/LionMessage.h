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

#include "protocol/TwoPL/TwoPLHelper.h"
#include "protocol/Lion/LionTransaction.h"
#include "core/RouterValue.h"

#include "common/ShareQueue.h"

// #include "protocol/TwoPL/TwoPLHelper.h"
// #include "protocol/TwoPL/TwoPLTransaction.h"

namespace star {


enum class LionMessage {
  SEARCH_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
  SEARCH_RESPONSE,
  REPLICATION_REQUEST,
  REPLICATION_RESPONSE,
  SEARCH_REQUEST_ROUTER_ONLY,
  SEARCH_RESPONSE_ROUTER_ONLY,
  //
  ASYNC_SEARCH_REQUEST,
  ASYNC_SEARCH_RESPONSE,
  ASYNC_SEARCH_REQUEST_ROUTER_ONLY,
  ASYNC_SEARCH_RESPONSE_ROUTER_ONLY,

  NFIELDS
};

class LionMessageFactory {
using Transaction = LionTransaction;
public:
  static std::size_t new_search_message(Message &message, ITable &table,
                                        const void *key, 
                                        uint32_t key_offset,
                                        uint32_t txn_id, 
                                        bool remaster, bool is_metis) {

    /*
     * The structure of a search request: (primary key, read key offset)
     */

    auto key_size = table.key_size();

    LionMessage message_type = LionMessage::SEARCH_REQUEST;

    auto message_size =
        MessagePiece::get_header_size() + key_size + 
        sizeof(key_offset) + 
        sizeof(txn_id) + 
        sizeof(remaster) + 
        sizeof(is_metis);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(message_type), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset 
            << txn_id
            << remaster 
            << is_metis;
    message.flush();
    return message_size;
  }


  static std::size_t new_async_search_message(Message &message, ITable &table,
                                        const void *key, uint32_t key_offset,
                                        uint32_t txn_id,
                                        bool remaster, bool is_metis) {

    /*
     * The structure of a search request: (primary key, read key offset)
     */

    auto key_size = table.key_size();

    LionMessage message_type = LionMessage::ASYNC_SEARCH_REQUEST;
    // if(is_metis){
    //   message_type = LionMessage::METIS_SEARCH_REQUEST;
    // VLOG(DEBUG_V8) << "LionMessage::ASYNC_SEARCH_REQUEST: " << *(int*)key << " " << message.get_source_node_id() << " " << message.get_dest_node_id();
    // }

    auto message_size =
        MessagePiece::get_header_size() + key_size + 
        sizeof(key_offset) + 
        sizeof(txn_id) + 
        sizeof(remaster) + sizeof(is_metis);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(message_type), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset << txn_id << remaster << is_metis;
    message.flush();
    return message_size;
  }

  static std::size_t new_replication_message(Message &message, ITable &table,
                                             const void *key, const void *value,
                                             uint64_t commit_tid,
                                             uint32_t txn_id) {

    /*
     * The structure of a replication request: (primary key, field value,
     * commit_tid)
     */

    auto key_size = table.key_size();
    auto field_size = table.field_size();

    auto message_size = MessagePiece::get_header_size() + key_size +
                        field_size + 
                        sizeof(commit_tid) + 
                        sizeof(txn_id);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionMessage::REPLICATION_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
    encoder << commit_tid << txn_id;
    message.flush();
    return message_size;
  }

  static std::size_t new_search_router_only_message(Message &message, ITable &table,
                                        const void *key, 
                                        uint32_t key_offset, 
                                        uint32_t txn_id, 
                                        bool is_metis) {

    /*
     * The structure of a search request: (primary key, read key offset)
     */
    LionMessage message_type = LionMessage::SEARCH_REQUEST_ROUTER_ONLY;

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + 
        key_size + 
        sizeof(key_offset) + 
        sizeof(txn_id) + 
        sizeof(is_metis);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(message_type), message_size,
        table.tableID(), table.partitionID());
    
    VLOG(DEBUG_V14) << "SEND UPDATE ROUTER MESSAGE " << *(int*)key << " " << message.get_source_node_id() << " " << message.get_dest_node_id();

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset 
            << txn_id
            << is_metis;
    message.flush();
    return message_size;
  }

    static std::size_t new_async_search_router_only_message(Message &message, ITable &table,
                                        const void *key, 
                                        uint32_t key_offset, 
                                        uint32_t txn_id, 
                                        bool is_metis) {

    /*
     * The structure of a search request: (primary key, read key offset)
     */
    LionMessage message_type = LionMessage::ASYNC_SEARCH_REQUEST_ROUTER_ONLY;
    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + 
        sizeof(key_offset) + 
        sizeof(txn_id) + 
        sizeof(is_metis);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(message_type), message_size,
        table.tableID(), table.partitionID());
    
    VLOG(DEBUG_V8) << "SEND ASYNC UPDATE ROUTER MESSAGE " << *(int*)key << " " << message.get_source_node_id() << " " << message.get_dest_node_id();

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset 
            << txn_id
            << is_metis;
    message.flush();
    return message_size;
  }

};

template <class Database> class LionMessageHandler {
  using Transaction = LionTransaction;
  using MessageFactoryType = LionMessageFactory;
  using Context = typename Database::ContextType;

public:

  static void search_request_handler(MessagePiece inputPiece,
                                     Message &responseMessage, std::vector<std::unique_ptr<Message>> &messages, Database &db, const Context &context,  Partitioner *partitioner,
                                     std::vector<std::unique_ptr<Transaction>>& txns
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
    uint32_t key_offset, txn_id;
    bool remaster, is_metis; // add by truth 22-04-22
    bool success;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + 
           key_size + 
           sizeof(key_offset) + 
           sizeof(txn_id) + 
           sizeof(remaster) + 
           sizeof(is_metis));

    // get row and offset
    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset 
        >> txn_id 
        >> remaster 
        >> is_metis; // index offset in the readSet from source request node

    DCHECK(dec.size() == 0);

    if(remaster == true){
      // remaster, not transfer
      value_size = 0;
    }

    // WorkloadType::which_workload == myTestSet::TPCC
    if(remaster == false || (remaster == true && context.migration_only > 0)) {
      // simulate cost of transmit data
      for (auto i = 0u; i < context.n_nop * 2; i++) {
        asm("nop");
      }
    }
    //TODO: add transmit length with longer transaction

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + 
                        sizeof(uint64_t) + 
                        sizeof(key_offset) + 
                        sizeof(txn_id) + 
                        sizeof(success) + 
                        sizeof(remaster) + 
                        sizeof(is_metis) + 
                        value_size;

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionMessage::SEARCH_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;

    uint64_t latest_tid;

    
    success = table.contains(key);
    if(!success){
      VLOG(DEBUG_V12) << "  dont Exist " << *(int*)key ; // << " " << tid_int;
      encoder << latest_tid 
              << key_offset 
              << txn_id
              << success 
              << remaster 
              << is_metis;
      responseMessage.data.append(value_size, 0);
      responseMessage.flush();
      return;
    }

    std::atomic<uint64_t> &tid = table.search_metadata(key);
    // try to lock tuple. Ensure not locked by current node
    latest_tid = TwoPLHelper::write_lock(tid, success); // be locked 

    if(!success){
      VLOG(DEBUG_V12) << "  can't Lock " << *(int*)key; // << " " << tid_int;
      encoder << latest_tid 
              << key_offset 
              << txn_id
              << success 
              << remaster 
              << is_metis;
      responseMessage.data.append(value_size, 0);
      responseMessage.flush();
      return;
    } else {
      VLOG(DEBUG_V12) << " Lock " << txn_id << " " 
                      << *(int*)key << " " << tid << " " << latest_tid;
    }
    // lock the router_table 
    if(partitioner->is_dynamic()){
          // 数据所在节点的路由表
          auto router_table = db.find_router_table(table_id); // , coordinator_id_old);
          auto router_val = (RouterValue*)router_table->search_value(key);
          
          // 数据所在节点
          auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key);
          uint64_t static_coordinator_id = partition_id % context.coordinator_num;
          // create the new tuple in global router of source request node
          auto coordinator_id_new = responseMessage.get_dest_node_id(); 

          if(coordinator_id_new != coordinator_id_old){
            // 数据更新到 发req的对面
            VLOG(DEBUG_V12) << txn_id << " " 
                            << table_id << " " << *(int*) key << " request switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << tid.load() << " " << latest_tid << " static: " << static_coordinator_id << " remaster: " << remaster;
            
            if(!is_metis){
              router_val->set_dynamic_coordinator_id(coordinator_id_new);
            }
            router_val->set_secondary_coordinator_id(coordinator_id_new);

            encoder << latest_tid 
                    << key_offset 
                    << txn_id
                    << success 
                    << remaster 
                    << is_metis;
            // reserve size for read
            responseMessage.data.append(value_size, 0);
            
//            auto value = table.search_value(key);
//            LOG(INFO) << *(int*)key << " " << (char*)value << " success: " << success << " " << " remaster: " << remaster << " " << new_secondary_coordinator_id;
            
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
            encoder << latest_tid 
                    << key_offset 
                    << txn_id
                    << success 
                    << remaster 
                    << is_metis;
            responseMessage.data.append(value_size, 0);
            responseMessage.flush();
          
          } else {
            DCHECK(false);
          }

    } else {
      VLOG(DEBUG_V12) << "  already in Static Mode " << *(int*)key; //  << " " << tid_int;
      encoder << latest_tid 
              << key_offset 
              << txn_id
              << success 
              << remaster 
              << is_metis;
      responseMessage.data.append(value_size, 0);
      if(success == true && remaster == false){
        auto row = table.search(key);
        // transfer: read from db and load data into message buffer
        void *dest =
            &responseMessage.data[0] + responseMessage.data.size() - value_size;
        TwoPLHelper::read(row, dest, value_size);
      }
      responseMessage.flush();
      // LOG(INFO) << *(int*) key << "s-delete "; // coordinator_id_old << " --> " << coordinator_id_new;
    }

    // wait for the commit / abort to unlock
    TwoPLHelper::write_lock_release(tid);
  }

  static void search_response_handler(MessagePiece inputPiece,
                                      Message &responseMessage, std::vector<std::unique_ptr<Message>> &messages, Database &db, const Context &context,  Partitioner *partitioner,
                                      std::vector<std::unique_ptr<Transaction>>& txns

) {
    /**
     * @brief 
     * 
     * 
     */
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
    uint32_t key_offset, txn_id;
    bool success;
    bool remaster, is_metis;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> tid >> 
           key_offset >> 
           txn_id     >> 
           success    >> remaster >> is_metis;

    if(remaster == true){
      value_size = 0;
    }

    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                  sizeof(tid) +
                                                  sizeof(key_offset) + 
                                                  sizeof(txn_id) + 
                                                  sizeof(success) + 
                                                  sizeof(remaster) + 
                                                  sizeof(is_metis) + 
                                                  value_size);

    DCHECK(txn_id < txns.size());
    auto& txn = txns[txn_id];
    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();

    LionRWKey &readKey = txn->readSet[key_offset];
    auto key = readKey.get_key();

    uint64_t last_tid = 0;

    if(success == true){
      if(partitioner->is_dynamic()){
        // update router 
        auto key = readKey.get_key();
        auto value = readKey.get_value();

        if(!remaster){
          // read value message piece
          stringPiece = inputPiece.toStringPiece();
          stringPiece.remove_prefix(sizeof(tid) + 
                                    sizeof(key_offset) + 
                                    sizeof(txn_id) + 
                                    sizeof(success) + 
                                    sizeof(remaster) + sizeof(is_metis));
          // insert into local node
          dec = Decoder(stringPiece);
          dec.read_n_bytes(readKey.get_value(), value_size);
          DCHECK(strlen((char*)readKey.get_value()) > 0);

          // LOG(INFO) << "NORMAL " <<  *(int*) key << " " << (char*) value << " insert " << txn->readSet.size();
          table.insert(key, value, (void*)& tid);
        }

        // simulate migrations with receiver 
        if(remaster == false || (remaster == true && context.migration_only > 0)) {
          // simulate cost of transmit data
          for (auto i = 0u; i < context.n_nop * 2; i++) {
            asm("nop");
          }
        } 

        // lock the respond tid and key
        bool success = false;
        std::atomic<uint64_t> &tid_ = table.search_metadata(key, success);
        if(!success){
          VLOG(DEBUG_V14) << "AFTER REMASETER, FAILED TO GET LOCK : " << *(int*)key << " " << tid; // 
          txn->abort_lock = true;
          return;
        } 

        if(readKey.get_write_lock_bit()){
          last_tid = TwoPLHelper::write_lock(tid_, success);
          VLOG(DEBUG_V14) << "LOCK-write " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " " << last_tid;
        } else {
          last_tid = TwoPLHelper::read_lock(tid_, success);
          VLOG(DEBUG_V14) << "LOCK-read " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " " << last_tid;
        }

        if(!success){
          VLOG(DEBUG_V14) << "AFTER REMASETER, FAILED TO GET LOCK : " << *(int*)key << " " << tid; // 
          txn->abort_lock = true;
          return;
        } 

        VLOG(DEBUG_V12) << txn_id << " " 
                        << table_id << " " 
                        << *(int*) key << " " << (char*)readKey.get_value() << " reponse switch " << " " << " " << tid << "  " << remaster << " | " << success << " txn->pendingResponses: " << txn->pendingResponses;

        auto router_table = db.find_router_table(table_id); // , coordinator_id_old);
        auto router_val = (RouterValue*)router_table->search_value(key);

        // create the new tuple in global router of source request node
        auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key);
        uint64_t static_coordinator_id = partition_id % context.coordinator_num; // static replica never moves only remastered
        auto coordinator_id_new = responseMessage.get_source_node_id(); 
        // DCHECK(coordinator_id_new != coordinator_id_old);
        if(coordinator_id_new != coordinator_id_old){
          VLOG(DEBUG_V12) << table_id <<" " << *(int*) key << " " << (char*)value << " reponse switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << tid << "  " << remaster;

          if(!is_metis){
            // update router
            router_val->set_dynamic_coordinator_id(coordinator_id_new);
          }
          router_val->set_secondary_coordinator_id(coordinator_id_new);
          readKey.set_dynamic_coordinator_id(coordinator_id_new);
          readKey.set_router_value(router_val->get_dynamic_coordinator_id(), router_val->get_secondary_coordinator_id());
          readKey.set_read_respond_bit();
          readKey.set_tid(tid); // original tid for lock release

          txn->tids[key_offset] = &tid_;
        } else {
          VLOG(DEBUG_V12) <<"Abort. Same Coordinators. " << table_id <<" " << *(int*) key << " " << (char*)value << " reponse switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << tid << "  " << remaster;
          txn->abort_lock = true;
          if(readKey.get_write_lock_bit()){
            TwoPLHelper::write_lock_release(tid_, last_tid);
            VLOG(DEBUG_V14) << "unLOCK-write " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " " << last_tid;
          } else {
            TwoPLHelper::read_lock_release(tid_);
            VLOG(DEBUG_V14) << "unLOCK-read " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " " << last_tid;
          }
        }


      }

    } else {
      VLOG(DEBUG_V14) << "FAILED TO GET LOCK : " << *(int*)key << " " << tid; // 
      txn->abort_lock = true;
    }

  }
 
  static void replication_request_handler(MessagePiece inputPiece,
                                          Message &responseMessage, std::vector<std::unique_ptr<Message>> &messages, 
                                          Database &db, const Context &context,  Partitioner *partitioner, 
                                          std::vector<std::unique_ptr<Transaction>>& txns

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
    uint64_t commit_tid;
    uint32_t txn_id;

    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                  key_size + field_size +
                                                  sizeof(commit_tid) + 
                                                  sizeof(txn_id));

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);
    auto valueStringPiece = stringPiece;
    stringPiece.remove_prefix(field_size);

    Decoder dec(stringPiece);
    dec >> commit_tid >> txn_id;

    DCHECK(dec.size() == 0);
    bool success;

    std::atomic<uint64_t> &tid = table.search_metadata(key, success);
    int debug_key = *(int*)key;

    if(success){
      TwoPLHelper::write_lock(tid, success);
      if(success){
        auto test = table.search_value(key);

        VLOG(DEBUG_V11) << " respond send to : " << responseMessage.get_source_node_id() << " -> " << responseMessage.get_dest_node_id() << "  with " << debug_key << " " << (char*)test;

        //! TODO logic needs to be checked
        // DCHECK(last_tid < commit_tid);
        table.deserialize_value(key, valueStringPiece);
        TwoPLHelper::write_lock_release(tid, commit_tid);
      }
    } else {
      LOG(INFO) << " replication failed: " << debug_key;
    }
    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + 
                        sizeof(txn_id) + 
                        sizeof(debug_key);
                        
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionMessage::REPLICATION_RESPONSE), message_size,
        table_id, partition_id);
    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header 
            << txn_id
            << debug_key;
            
    responseMessage.flush();
  }

  static void replication_response_handler(MessagePiece inputPiece,
                                           Message &responseMessage, std::vector<std::unique_ptr<Message>> &messages, 
                                           Database &db, const Context &context,  Partitioner *partitioner,
                                           std::vector<std::unique_ptr<Transaction>>& txns

) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::REPLICATION_RESPONSE));
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



  static void search_request_router_only_handler(MessagePiece inputPiece,
                                     Message &responseMessage, std::vector<std::unique_ptr<Message>> &messages, Database &db, const Context &context,  Partitioner *partitioner,
                                     std::vector<std::unique_ptr<Transaction>>& txns

) {
    /**
     * @brief directly move the data to the request node!
     * 修改其他机器上的路由情况， 当前机器不涉及该事务的处理，可以认为事务涉及的数据主节点都不在此，直接处理就可以
     * Transaction *txn unused
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
    uint32_t key_offset, txn_id;
    bool is_metis;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + 
           key_size + 
           sizeof(key_offset) + 
           sizeof(txn_id) + 
           sizeof(is_metis));

    // get row and offset
    const void *key = stringPiece.data();
    // auto row = table.search(key);

    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset >> txn_id >> is_metis; // index offset in the readSet from source request node

    DCHECK(dec.size() == 0);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + 
    sizeof(uint64_t) + 
    sizeof(key_offset) + 
    sizeof(txn_id) + 
    value_size;

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionMessage::SEARCH_RESPONSE_ROUTER_ONLY), message_size,
        table_id, partition_id);

    uint64_t tid = 0; // auto tid = TwoPLHelper::read(row, dest, value_size);
    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header << tid << key_offset << txn_id;

    // reserve size for read
    responseMessage.data.append(value_size, 0);
    // if(context.coordinator_id == context.coordinator_num){
    //   VLOG(DEBUG_V8) << "RECV ROUTER UPDATE. " << *(int*)key << "  ";
    // }
    
    if(partitioner->is_dynamic()){
      auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key);

      // create the new tuple in global router of source request node
      auto coordinator_id_new = responseMessage.get_dest_node_id(); 
      if(coordinator_id_new != coordinator_id_old){
        auto router_table = db.find_router_table(table_id); // , coordinator_id_new);
        auto router_val = (RouterValue*)router_table->search_value(key);

        if(!is_metis){
          router_val->set_dynamic_coordinator_id(coordinator_id_new);// (key, &coordinator_id_new);
        }
        router_val->set_secondary_coordinator_id(coordinator_id_new);

        // if(context.coordinator_id == context.coordinator_num){
        VLOG(DEBUG_V9) << "ROUTER UPDATE. " << txn_id << " " 
                       << *(int*)key << " " 
                       << coordinator_id_old << "-->" << coordinator_id_new;
        // }
      }

    } else {

      // LOG(INFO) << *(int*) key << "s-delete "; // coordinator_id_old << " --> " << coordinator_id_new;

    }

    responseMessage.flush();
  }

  static void search_response_router_only_handler(MessagePiece inputPiece,
                                      Message &responseMessage, std::vector<std::unique_ptr<Message>> &messages, Database &db, const Context &context,  Partitioner *partitioner,
                                      std::vector<std::unique_ptr<Transaction>>& txns
) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::SEARCH_RESPONSE_ROUTER_ONLY));

    auto stringPiece = inputPiece.toStringPiece();

    uint64_t tid;
    uint32_t key_offset, txn_id;

    star::Decoder dec(stringPiece);
    dec >> tid >> key_offset >> txn_id; // index offset in the readSet from source request node
    DCHECK(txn_id < txns.size()) << txn_id << " " << txns.size();
    auto& txn = txns[txn_id];

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();

    VLOG(DEBUG_V12) << "RECV DONE SEARCH_RESPONSE_ROUTER_ONLY " 
                    << txn_id << " " 
                    << key_offset << " " 
                    << *(int*)txn->readSet[key_offset].get_key() 
                    << " txn->pendingResponses: " << txn->pendingResponses;
  }



  static void async_search_request_handler(MessagePiece inputPiece,
                                     Message &responseMessage, std::vector<std::unique_ptr<Message>> &messages, Database &db, const Context &context,  Partitioner *partitioner,
                                     std::vector<std::unique_ptr<Transaction>>& txns
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
           static_cast<uint32_t>(LionMessage::ASYNC_SEARCH_REQUEST));
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
    uint32_t key_offset, txn_id;
    bool remaster, is_metis; // add by truth 22-04-22
    bool success;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + 
           sizeof(key_offset) + 
           sizeof(txn_id) + 
           sizeof(remaster) + 
           sizeof(is_metis));

    // get row and offset
    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset 
        >> txn_id     
        >> remaster   
        >> is_metis; // index offset in the readSet from source request node

    DCHECK(dec.size() == 0);

    if(remaster == true){
      // remaster, not transfer
      value_size = 0;
    }

    // WorkloadType::which_workload == myTestSet::TPCC
    // if(remaster == false || (remaster == true && context.migration_only > 0)) {
    //   // simulate cost of transmit data
    //   for (auto i = 0u; i < context.n_nop; i++) {
    //     asm("nop");
    //   }
    // }
    //TODO: add transmit length with longer transaction

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + 
                        sizeof(uint64_t) + 
                        sizeof(key_offset) + 
                        sizeof(txn_id) + 
                        sizeof(success) + 
                        sizeof(remaster) + sizeof(is_metis) + 
                        + key_size + value_size;

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionMessage::ASYNC_SEARCH_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;

    uint64_t latest_tid;

    
    success = table.contains(key);
    if(!success){
      VLOG(DEBUG_V12) << "  dont Exist " << *(int*)key ; // << " " << tid_int;
      encoder << latest_tid << key_offset << txn_id << success << remaster << is_metis;
      encoder.write_n_bytes(key, key_size);
      responseMessage.data.append(value_size, 0);
      responseMessage.flush();
      return;
    }

    std::atomic<uint64_t> &tid = table.search_metadata(key);
    // try to lock tuple. Ensure not locked by current node
    latest_tid = TwoPLHelper::write_lock(tid, success); // be locked 

    if(!success){
      VLOG(DEBUG_V12) << "  can't Lock " << *(int*)key; // << " " << tid_int;
      encoder << latest_tid << key_offset << txn_id << success << remaster << is_metis;
      encoder.write_n_bytes(key, key_size);
      responseMessage.data.append(value_size, 0);
      responseMessage.flush();
      return;
    } else {
      VLOG(DEBUG_V12) << " Lock " << *(int*)key << " " << tid << " " << latest_tid;
    }
    // lock the router_table 
    if(partitioner->is_dynamic()){
          // 数据所在节点的路由表
          auto router_table = db.find_router_table(table_id); // , coordinator_id_old);
          auto router_val = (RouterValue*)router_table->search_value(key);
          
          // 数据所在节点
          auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key);
          // uint64_t static_coordinator_id = partition_id % context.coordinator_num;
          // create the new tuple in global router of source request node
          auto coordinator_id_new = responseMessage.get_dest_node_id(); 

          if(coordinator_id_new != coordinator_id_old){
            // 数据更新到 发req的对面
            VLOG(DEBUG_V12) << table_id <<" " << *(int*) key << " request switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << tid.load() << " " << latest_tid  << " remaster: " << remaster; // << " static: " << static_coordinator_id
            
            if(!is_metis){
              router_val->set_dynamic_coordinator_id(coordinator_id_new);
            }
            router_val->set_secondary_coordinator_id(coordinator_id_new);

            encoder << latest_tid << key_offset << txn_id << success << remaster << is_metis;
            encoder.write_n_bytes(key, key_size);
            // reserve size for read
            responseMessage.data.append(value_size, 0);
            
//            auto value = table.search_value(key);
//            LOG(INFO) << *(int*)key << " " << (char*)value << " success: " << success << " " << " remaster: " << remaster << " " << new_secondary_coordinator_id;
            
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
            encoder << latest_tid << key_offset << txn_id << success << remaster << is_metis;
            encoder.write_n_bytes(key, key_size);
            responseMessage.data.append(value_size, 0);
            responseMessage.flush();
          
          } else {
            DCHECK(false);
          }

    } else {
      VLOG(DEBUG_V12) << "  already in Static Mode " << *(int*)key; //  << " " << tid_int;
      encoder << latest_tid << key_offset << txn_id << success << remaster << is_metis;
      encoder.write_n_bytes(key, key_size);
      responseMessage.data.append(value_size, 0);
      if(success == true && remaster == false){
        auto row = table.search(key);
        // transfer: read from db and load data into message buffer
        void *dest =
            &responseMessage.data[0] + responseMessage.data.size() - value_size;
        TwoPLHelper::read(row, dest, value_size);
      }
      responseMessage.flush();
      // LOG(INFO) << *(int*) key << "s-delete "; // coordinator_id_old << " --> " << coordinator_id_new;
    }

    // wait for the commit / abort to unlock
    TwoPLHelper::write_lock_release(tid);
  }

  static void async_search_response_handler(MessagePiece inputPiece,
                                      Message &responseMessage, std::vector<std::unique_ptr<Message>> &messages, Database &db, const Context &context,  Partitioner *partitioner,
                                      std::vector<std::unique_ptr<Transaction>>& txns

) {
    /**
     * @brief 
     * 
     * 
     */
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::ASYNC_SEARCH_RESPONSE));
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
    uint32_t key_offset, txn_id;
    bool success;
    bool remaster, is_metis;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> tid >> key_offset >> txn_id >> success >> remaster >> is_metis;

    StringPiece stringPieceKey = inputPiece.toStringPiece();
    stringPieceKey.remove_prefix(sizeof(tid) + 
                                 sizeof(key_offset) + 
                                 sizeof(txn_id) + 
                                 sizeof(success) + 
                                 sizeof(remaster) + 
                                 sizeof(is_metis));
    const void *key = stringPieceKey.data();

    if(remaster == true){
      value_size = 0;
    }

    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                  sizeof(tid) +
                                                  sizeof(key_offset) + 
                                                  sizeof(txn_id) + 
                                                  sizeof(success) + 
                                                  sizeof(remaster) + 
                                                  sizeof(is_metis) + 
                                                  key_size + value_size);
    
    // txn->asyncPendingResponses--;
    // txn->network_size += inputPiece.get_message_length();


    uint64_t last_tid = 0;

    VLOG(DEBUG_V8) << *(int*) key << " async_search_response_handler ";

    if(success == true){
      if(partitioner->is_dynamic()){
        // update router 
        // auto key = readKey.get_key();
        // auto value = readKey.get_value();

        if(!remaster){
          // read value message piece
          stringPiece = inputPiece.toStringPiece();
          stringPiece.remove_prefix(sizeof(tid) + 
                                    sizeof(key_offset) + 
                                    sizeof(txn_id) +
                                    sizeof(success) + sizeof(remaster) + sizeof(is_metis) + 
                                    key_size);

          const void *value = stringPieceKey.data();
          // insert into local node
          // dec = Decoder(stringPiece);
          // dec.read_n_bytes(readKey.get_value(), value_size);
          
          DCHECK(strlen((char*)value) > 0);

          table.insert(key, value, (void*)& tid);
          
          VLOG(DEBUG_V12) << table_id <<" " << *(int*) key << " " << (char*)value << " reponse switch " << " " << " " << tid << "  " << remaster << " | " << success << " ";
        }

        // simulate migrations with receiver 
        // if(remaster == false || (remaster == true && context.migration_only > 0)) {
        //   // simulate cost of transmit data
        //   for (auto i = 0u; i < context.n_nop; i++) {
        //     asm("nop");
        //   }
        // } 

        // lock the respond tid and key
        bool success = false;
        std::atomic<uint64_t> &tid_ = table.search_metadata(key, success);
        if(!success){
          VLOG(DEBUG_V14) << "AFTER REMASETER, FAILED TO GET LOCK : " << *(int*)key << " " << tid; // 
          // txn->abort_lock = true;
          return;
        } 
        
        // LionRWKey &readKey = txn->readSet[key_offset];
        // auto key = readKey.get_key();
        // // already be locked by read
        // if(readKey.get_write_lock_bit()){
        //   last_tid = TwoPLHelper::write_lock(tid_, success);
        //   VLOG(DEBUG_V14) << "LOCK-write " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " " << last_tid;
        // } else {
        //   last_tid = TwoPLHelper::read_lock(tid_, success);
        //   VLOG(DEBUG_V14) << "LOCK-read " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " " << last_tid;
        // }

        // if(!success){
        //   VLOG(DEBUG_V14) << "AFTER REMASETER, FAILED TO GET LOCK : " << *(int*)key << " " << tid; // 
        //   txn->abort_lock = true;
        //   return;
        // } 

        auto router_table = db.find_router_table(table_id); // , coordinator_id_old);
        auto router_val = (RouterValue*)router_table->search_value(key);

        // create the new tuple in global router of source request node
        auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key);
        // uint64_t static_coordinator_id = partition_id % context.coordinator_num; // static replica never moves only remastered
        auto coordinator_id_new = responseMessage.get_source_node_id(); 
        // DCHECK(coordinator_id_new != coordinator_id_old);
        if(coordinator_id_new != coordinator_id_old){
          VLOG(DEBUG_V12) << table_id <<" " << *(int*) key << " async reponse switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << tid << "  " << remaster;

          if(!is_metis){
            // update router
            router_val->set_dynamic_coordinator_id(coordinator_id_new);
          }
          router_val->set_secondary_coordinator_id(coordinator_id_new);
          // readKey.set_dynamic_coordinator_id(coordinator_id_new);
          // readKey.set_router_value(router_val->get_dynamic_coordinator_id(), router_val->get_secondary_coordinator_id());
          // readKey.set_read_respond_bit();
          // readKey.set_tid(tid); // original tid for lock release

          // txn->tids[key_offset] = &tid_;
        }
        
        //  else {
        //   VLOG(DEBUG_V12) <<"Abort. Same Coordinators. " << table_id <<" " << *(int*) key << " " << (char*)value << " reponse switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << tid << "  " << remaster;
        //   txn->abort_lock = true;
        //   if(readKey.get_write_lock_bit()){
        //     TwoPLHelper::write_lock_release(tid_, last_tid);
        //     VLOG(DEBUG_V14) << "unLOCK-write " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " " << last_tid;
        //   } else {
        //     TwoPLHelper::read_lock_release(tid_);
        //     VLOG(DEBUG_V14) << "unLOCK-read " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " " << last_tid;
        //   }
        // }


      }

    } else {
      VLOG(DEBUG_V14) << "FAILED TO GET LOCK : " << *(int*)key << " " << tid; // 
      // txn->abort_lock = true;
    }

  }


  static void async_search_request_router_only_handler(MessagePiece inputPiece,
                                     Message &responseMessage, std::vector<std::unique_ptr<Message>> &messages, Database &db, const Context &context,  Partitioner *partitioner,
                                     std::vector<std::unique_ptr<Transaction>>& txns

) {
    /**
     * @brief directly move the data to the request node!
     * 修改其他机器上的路由情况， 当前机器不涉及该事务的处理，可以认为事务涉及的数据主节点都不在此，直接处理就可以
     * Transaction *txn unused
     */
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::ASYNC_SEARCH_REQUEST_ROUTER_ONLY));
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
    uint32_t key_offset, txn_id;
    bool is_metis;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + 
           key_size + 
           sizeof(key_offset) + 
           sizeof(txn_id) + 
           sizeof(is_metis));

    // get row and offset
    const void *key = stringPiece.data();
    // auto row = table.search(key);

    VLOG(DEBUG_V9) << *(int*) key << " async_search_request_router_only_handler ";

    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset 
        >> txn_id
        >> is_metis; // index offset in the readSet from source request node

    DCHECK(dec.size() == 0);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + 
              sizeof(uint64_t) + 
              sizeof(key_offset) + 
              sizeof(txn_id) + 
              value_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionMessage::ASYNC_SEARCH_RESPONSE_ROUTER_ONLY), message_size,
        table_id, partition_id);

    uint64_t tid = 0; // auto tid = TwoPLHelper::read(row, dest, value_size);
    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header 
            << tid 
            << key_offset 
            << txn_id;

    // reserve size for read
    responseMessage.data.append(value_size, 0);
    // if(context.coordinator_id == context.coordinator_num){
    //   VLOG(DEBUG_V8) << "RECV ROUTER UPDATE. " << *(int*)key << "  ";
    // }
    
    if(partitioner->is_dynamic()){
      auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key);

      // create the new tuple in global router of source request node
      auto coordinator_id_new = responseMessage.get_dest_node_id(); 
      if(coordinator_id_new != coordinator_id_old){
        auto router_table = db.find_router_table(table_id); // , coordinator_id_new);
        auto router_val = (RouterValue*)router_table->search_value(key);

        if(!is_metis){
          router_val->set_dynamic_coordinator_id(coordinator_id_new);// (key, &coordinator_id_new);
        }
        router_val->set_secondary_coordinator_id(coordinator_id_new);

        if(context.coordinator_id == context.coordinator_num){
          VLOG(DEBUG_V9) << "ROUTER UPDATE. " << *(int*)key << " " << coordinator_id_old << "-->" << coordinator_id_new;
        }
      }

    } else {

      // LOG(INFO) << *(int*) key << "s-delete "; // coordinator_id_old << " --> " << coordinator_id_new;

    }

    responseMessage.flush();
  }

  static void async_search_response_router_only_handler(MessagePiece inputPiece,
                                      Message &responseMessage, std::vector<std::unique_ptr<Message>> &messages, Database &db, const Context &context,  Partitioner *partitioner,
                                      std::vector<std::unique_ptr<Transaction>>& txns

) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionMessage::ASYNC_SEARCH_RESPONSE_ROUTER_ONLY));

    auto stringPiece = inputPiece.toStringPiece();

    uint64_t tid;
    uint32_t key_offset;

    star::Decoder dec(stringPiece);
    dec >> tid >> key_offset; // index offset in the readSet from source request node

    // txn->asyncPendingResponses--;
    // txn->network_size += inputPiece.get_message_length();

    // VLOG(DEBUG_V12) << "RECV DONE SEARCH_RESPONSE_ROUTER_ONLY " << key_offset << " " << *(int*)txn->readSet[key_offset].get_key() << " txn->pendingResponses: " << txn->pendingResponses;
  }

  static std::vector<
      std::function<void(MessagePiece, Message &, std::vector<std::unique_ptr<Message>>&, 
                         Database &, const Context &, Partitioner *, 
                         std::vector<std::unique_ptr<Transaction>>&)>>
  get_message_handlers() {

    std::vector<
        std::function<void(MessagePiece, Message &, std::vector<std::unique_ptr<Message>>&, 
                           Database &, const Context &, Partitioner *, 
                           std::vector<std::unique_ptr<Transaction>>&)>>
        v;
    v.resize(static_cast<int>(ControlMessage::NFIELDS));
    v.push_back(search_request_handler); // SEARCH_REQUEST
    v.push_back(search_response_handler); // SEARCH_RESPONSE
    v.push_back(replication_request_handler); // REPLICATION_REQUEST
    v.push_back(replication_response_handler); // REPLICATION_RESPONSE
    v.push_back(search_request_router_only_handler); // SEARCH_REQUEST_ROUTER_ONLY
    v.push_back(search_response_router_only_handler); // SEARCH_RESPONSE_ROUTER_ONLY

    v.push_back(async_search_request_handler); // SEARCH_REQUEST
    v.push_back(async_search_response_handler); // SEARCH_RESPONSE
    v.push_back(async_search_request_router_only_handler); // SEARCH_REQUEST_ROUTER_ONLY
    v.push_back(async_search_response_router_only_handler); // SEARCH_RESPONSE_ROUTER_ONLY

    return v;
  }
};

} // namespace star