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
#include "protocol/LionSS/LionSSRWKey.h"
#include "protocol/LionSS/LionSSTransaction.h"

namespace star {

enum class LionSSMessage {
  TRANSMIT_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
  TRANSMIT_RESPONSE,
  TRANSMIT_ROUTER_ONLY_REQUEST,
  TRANSMIT_ROUTER_ONLY_RESPONSE,

  //
  ASYNC_SEARCH_REQUEST,
  ASYNC_SEARCH_RESPONSE,
  ASYNC_SEARCH_REQUEST_ROUTER_ONLY,
  ASYNC_SEARCH_RESPONSE_ROUTER_ONLY,

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
  RELEASE_WRITE_LOCK_REQUEST,

  NFIELDS
};


  // uint64_t my_debug_key(int table_id, int partition_id, const void* key){
  //   uint64_t record_key;
  //   tpcc::warehouse::key k1;
  //   tpcc::district::key k2;
  //   tpcc::customer::key k3;
  //   tpcc::stock::key k4;

  //   uint64_t W_ID, D_W_ID, D_ID, C_W_ID, C_D_ID, C_ID, S_W_ID, S_I_ID;


  //   switch (table_id)
  //   {
  //   case tpcc::warehouse::tableID:
  //     k1 = *(tpcc::warehouse::key*)key;
  //     W_ID = k1.W_ID;
  //     record_key = (static_cast<uint64_t>(tpcc::warehouse::tableID) << RECORD_COUNT_TABLE_ID_OFFSET) + (W_ID << RECORD_COUNT_W_ID_OFFSET); 
  //     break;
  //   case tpcc::district::tableID:
  //     k2 = *(tpcc::district::key*)key;
  //     D_W_ID = k2.D_W_ID;
  //     D_ID   = k2.D_ID;
  //     record_key = (static_cast<uint64_t>(tpcc::district::tableID)  << RECORD_COUNT_TABLE_ID_OFFSET) 
  //     + (D_W_ID << RECORD_COUNT_W_ID_OFFSET) 
  //     + (D_ID << RECORD_COUNT_D_ID_OFFSET);
  //     break;
  //   case tpcc::customer::tableID:
  //     k3 = *(tpcc::customer::key*)key;
  //     C_W_ID = k3.C_W_ID;
  //     C_D_ID = k3.C_D_ID;
  //     C_ID   = k3.C_ID;

  //     record_key = (static_cast<uint64_t>(tpcc::customer::tableID)  << RECORD_COUNT_TABLE_ID_OFFSET) 
  //     + (C_W_ID << RECORD_COUNT_W_ID_OFFSET) 
  //     + (C_D_ID << RECORD_COUNT_D_ID_OFFSET) 
  //     + (C_ID << RECORD_COUNT_C_ID_OFFSET);
  //     break;
  //   case tpcc::stock::tableID:
  //     k4 = *(tpcc::stock::key*)key;
  //     S_W_ID = k4.S_W_ID;
  //     S_I_ID = k4.S_I_ID;
  //     // stock_keys.push_back(stock::key(OL_SUPPLY_W_ID, OL_I_ID));
  //     record_key = 
  //     (static_cast<uint64_t>(tpcc::stock::tableID) << RECORD_COUNT_TABLE_ID_OFFSET) 
  //     + (S_W_ID << RECORD_COUNT_W_ID_OFFSET) 
  //     + (S_I_ID);
  //     break;
  //   default:
  //     break;
  //   }
    

  //   return record_key;
  // }

class LionSSMessageFactory {

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
        static_cast<uint32_t>(LionSSMessage::READ_LOCK_REQUEST), message_size,
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
        static_cast<uint32_t>(LionSSMessage::WRITE_LOCK_REQUEST), message_size,
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
        static_cast<uint32_t>(LionSSMessage::ABORT_REQUEST), message_size,
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
        static_cast<uint32_t>(LionSSMessage::WRITE_REQUEST), message_size,
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
        static_cast<uint32_t>(LionSSMessage::RELEASE_READ_LOCK_REQUEST),
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
        static_cast<uint32_t>(LionSSMessage::RELEASE_WRITE_LOCK_REQUEST),
        message_size, table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << commit_tid;
    message.flush();
    return message_size;
  }

  static std::size_t new_transmit_message(Message &message, ITable &table,
                                        const void *key, uint32_t key_offset, 
                                        bool remaster, RouterTxnOps op
                                        ) {

    /*
     * The structure of a search request: (primary key, read key offset)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(key_offset) + 
        sizeof(remaster) + 
        sizeof(op);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionSSMessage::TRANSMIT_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset 
            << remaster 
            << op;
    message.flush();
    return message_size;
  }

  static std::size_t new_async_search_message(Message &message, ITable &table,
                                        const void *key, uint32_t key_offset, 
                                        bool remaster, RouterTxnOps op
                                        ) {

    /*
     * The structure of a search request: (primary key, read key offset)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(key_offset) + 
        sizeof(remaster) + 
        sizeof(op);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionSSMessage::ASYNC_SEARCH_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset 
            << remaster 
            << op;
    message.flush();
    return message_size;
  }
  // 
  static std::size_t new_transmit_router_only_message(Message &message, ITable &table,
                                        const void *key, uint32_t key_offset,
                                        RouterTxnOps op
                                        ) {

    /*
     * The structure of a search request: (primary key, read key offset)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + 
        sizeof(key_offset) + sizeof(op);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionSSMessage::TRANSMIT_ROUTER_ONLY_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset << op;
    message.flush();
    return message_size;
  }

  static std::size_t new_async_search_router_only_message(Message &message, ITable &table,
                                        const void *key, uint32_t key_offset,
                                        RouterTxnOps op, uint32_t new_destination
                                        ) {

    /*
     * The structure of a search request: (primary key, read key offset)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + 
        sizeof(key_offset) + sizeof(op) + sizeof(new_destination);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionSSMessage::ASYNC_SEARCH_REQUEST_ROUTER_ONLY), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset << op << new_destination;
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
        static_cast<uint32_t>(LionSSMessage::REPLICATION_REQUEST), message_size,
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

template <class Database> class LionSSMessageHandler {
  using Transaction = LionSSTransaction;
  using Context = typename Database::ContextType;

public:
  // 
  static void transmit_request_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                      Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionSSMessage::TRANSMIT_REQUEST));
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
    RouterTxnOps op;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + 
           sizeof(key_offset) + 
           sizeof(remaster) + 
           sizeof(op));

    // get row and offset
    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset >> remaster >> op; // index offset in the readSet from source request node



    DCHECK(dec.size() == 0);

    if(remaster == true){
      // remaster, not transfer
      value_size = 0;
    }

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + 
                        sizeof(uint64_t) + sizeof(key_offset) + sizeof(success) + 
                        sizeof(remaster) + sizeof(op) +
                        value_size;
    
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionSSMessage::TRANSMIT_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;

    uint64_t latest_tid;

    success = table.contains(key);
    if(!success){
      LOG(INFO) << " TRANSMIT_REQUEST!!! dont Exist " << *(int*)key ; // << " " << tid_int;
      encoder << latest_tid << key_offset << success << remaster << op;
      responseMessage.data.append(value_size, 0);
      responseMessage.flush();
      return;
    }
    // try to lock tuple. Ensure not locked by current node
    std::atomic<uint64_t> &tid = table.search_metadata(key);
    latest_tid = TwoPLHelper::write_lock(tid, success); // be locked 

    if(!success){ // VLOG(DEBUG_V12) 
      auto test = my_debug_key(table_id, partition_id, key);
      LOG(INFO) << " TRANSMIT_REQUEST!!! can't Lock " << *(int*)key << " " <<  test; // << " " << tid_int;
      encoder << latest_tid << key_offset << success << remaster << op;
      responseMessage.data.append(value_size, 0);
      responseMessage.flush();
      return;
    } else {
      VLOG(DEBUG_V12) << " Lock " << *(int*)key << " " << tid << " " << latest_tid;
    }
    // simulate migrate latency
    // std::vector<size_t> lock_;
    // size_t p_id = *(size_t*)key / 200000;
    // size_t offset_ = *(size_t*)key % 200000;
    // for(int i = 0 ; i + offset_ < 200000 && i < 50000; i ++ ){
    //   size_t k = p_id * 200000 + i + offset_;
      
    //   // bool succ;
    //   // latest_tid = TwoPLHelper::write_lock(tid, succ); // be locked 
    //   // if(succ){
    //   //   lock_.push_back(k);
    //   // }
    // }
    ycsb::ycsb::key k(*(size_t*)key % 200000 / 50000 + 200000 * partition_id);
    ITable &router_lock_table = *db.find_router_lock_table(table_id, partition_id);
    std::atomic<uint64_t> &lock_tid = router_lock_table.search_metadata((void*) &k);

    if(op == RouterTxnOps::ADD_REPLICA){
      
    } else {
      // LOG(INFO) << "LOCK ROUTER " << *(int*)key << " " << static_cast<int>(op);

      TwoPLHelper::write_lock(lock_tid, success); // be locked 
      if(!success){
        TwoPLHelper::write_lock_release(tid);
        auto test = my_debug_key(table_id, partition_id, key);
        LOG(INFO) << " TRANSMIT_REQUEST!!! can't Lock router table " << *(int*)key << " " <<  test; // << " " << tid_int;
        encoder << latest_tid << key_offset << success << remaster << op;
        responseMessage.data.append(value_size, 0);
        responseMessage.flush();
        return;
      }
    }


    if(remaster == false || context.migration_only > 0) {
      // simulate cost of transmit data
      for (auto i = 0u; i < context.n_nop * 2; i++) {
        asm("nop");
      }
    } else if(remaster == true) {
        for (auto i = 0u; i < context.n_nop * 2 / 5; i++) {
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
      // auto test = my_debug_key(table_id, partition_id, key);
      // LOG(INFO) << table_id <<" " << *(int*) key << " transmit request switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << tid.load() << " " << latest_tid << " static: " << static_coordinator_id << " remaster: " << remaster << " " << test << " " << success;
      
      // update the router 
      if(op == RouterTxnOps::TRANSFER){
        router_val->set_dynamic_coordinator_id(coordinator_id_new);
      }
      router_val->set_secondary_coordinator_id(coordinator_id_new);



    } else if(coordinator_id_new == coordinator_id_old) {
      success = true;
      // auto test = my_debug_key(table_id, partition_id, key);
      // LOG(INFO) << " TRANSMIT_REQUEST!!! Same coordi : " << coordinator_id_new << " " <<coordinator_id_old << " " << *(int*)key << " " << test << " " << tid;
      // encoder << latest_tid << key_offset << success << remaster;
      // responseMessage.data.append(value_size, 0);
      // responseMessage.flush();
    
    } else {
      DCHECK(false);
    }
    encoder << latest_tid << key_offset << success << remaster << op;
    // reserve size for read
    responseMessage.data.append(value_size, 0);
    
    // auto value = table.search_value(key);
    // LOG(INFO) << *(int*)key << " " <<  " success: " << success << "" << " remaster: " << remaster;//  << " " << new_secondary_coordinator_id; (char*)value <<
    
    if(success == true && remaster == false){
      // transfer: read from db and load data into message buffer
      void *dest =
          &responseMessage.data[0] + responseMessage.data.size() - value_size;
      auto row = table.search(key);
      TwoPLHelper::read(row, dest, value_size);
    }
    responseMessage.flush();
    // wait for the commit / abort to unlock

    // for(auto& k: lock_){ 
    //   std::atomic<uint64_t> &tid = table.search_metadata((void*) &k);
    //   TwoPLHelper::write_lock_release(tid); // be locked 
    // }

    TwoPLHelper::write_lock_release(tid);
    if(op == RouterTxnOps::ADD_REPLICA){
      
    } else {
      TwoPLHelper::write_lock_release(lock_tid);
    }
  }


  static void transmit_response_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                      Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionSSMessage::TRANSMIT_RESPONSE));
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
    RouterTxnOps op;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> tid >> key_offset >> success >> remaster >> op;

    if(remaster == true){
      value_size = 0;
    }

    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                  sizeof(tid) +
                                                  sizeof(key_offset) + sizeof(success) + sizeof(remaster) + 
                                                  sizeof(op) +
                                                  value_size);

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
    
    LionSSRWKey &readKey = txn->readSet[key_offset];
    auto key = readKey.get_key();

    auto test = my_debug_key(table_id, partition_id, key);
    // LOG(INFO) << "TRANSMIT_RESPONSE " << table_id << " "
    //                                   << test << " " << *(int*)key << " "
    //                                   << success << " " << responseMessage.get_dest_node_id() << " -> " << responseMessage.get_source_node_id() ;

    uint64_t last_tid = 0;




    if(success == true){
      // update router 
      auto key = readKey.get_key();
      auto value = readKey.get_value();

      if(!remaster){
        // read value message piece
        stringPiece = inputPiece.toStringPiece();
        stringPiece.remove_prefix(sizeof(tid) + sizeof(key_offset) + sizeof(success) +
                                  sizeof(remaster) + 
                                  sizeof(op));
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
        LOG(INFO) << "TRANSMIT_RESPONSE !!! AFTER REMASETER, FAILED TO GET LOCK : " << *(int*)key << " " << tid; // 
        txn->abort_lock = true;
        return;
      } 

      if(remaster == false || context.migration_only > 0) {
        // simulate cost of transmit data
        for (auto i = 0u; i < context.n_nop * 2; i++) {
          asm("nop");
        }
      } else if(remaster == true) {
        for (auto i = 0u; i < context.n_nop * 2 / 5; i++) {
          asm("nop");
        }
      }
      // LOG(INFO) << table_id <<" " << *(int*) key << " " << (char*)readKey.get_value() << " reponse switch " << " " << " " << tid << "  " << remaster << " | " << success << " ";

      auto router_table = db.find_router_table(table_id); // , coordinator_id_old);
      auto router_val = (RouterValue*)router_table->search_value(key);

      // create the new tuple in global router of source request node
      auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key);
      uint64_t static_coordinator_id = partition_id % context.coordinator_num; // static replica never moves only remastered
      auto coordinator_id_new = responseMessage.get_source_node_id(); 

      // LOG(INFO) << table_id <<" " << *(int*) key << " " << (char*)value << " transmit reponse switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << tid << "  " << remaster;
      // update router
      if(op == RouterTxnOps::TRANSFER){
        router_val->set_dynamic_coordinator_id(coordinator_id_new);
      }
      
      router_val->set_secondary_coordinator_id(coordinator_id_new);

      readKey.set_dynamic_coordinator_id(coordinator_id_new);
      readKey.set_router_value(router_val->get_dynamic_coordinator_id(),router_val->get_secondary_coordinator_id());
      // readKey.set_Read();
      readKey.set_tid(tid); // original tid for lock release
      readKey.set_write_lock_bit();
      
      // txn->tids[key_offset] = &tid_;
    } else {
      LOG(INFO) << "TRANSMIT_RESPONSE !!! FAILED TO GET LOCK : " << *(int*)key << " " << tid; // 
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
           static_cast<uint32_t>(LionSSMessage::TRANSMIT_ROUTER_ONLY_REQUEST));
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
    RouterTxnOps op;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + 
           sizeof(key_offset) + 
           sizeof(op));

    // get row and offset
    const void *key = stringPiece.data();
    // auto row = table.search(key);
     // LOG(INFO) << "TRANSMIT_ROUTER_ONLY_REQUEST " << *(int*)key;
    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset >> op; // index offset in the readSet from source request node

    DCHECK(dec.size() == 0);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + value_size +
                        sizeof(uint64_t) + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionSSMessage::TRANSMIT_ROUTER_ONLY_RESPONSE), message_size,
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
      if(op == RouterTxnOps::TRANSFER){
        router_val->set_dynamic_coordinator_id(coordinator_id_new);// (key, &coordinator_id_new);
      }
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
           static_cast<uint32_t>(LionSSMessage::TRANSMIT_ROUTER_ONLY_RESPONSE));
    // LOG(INFO) << "TRANSMIT_ROUTER_ONLY_RESPONSE";
    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
  }

  // 
  static void async_search_request_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                      Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionSSMessage::ASYNC_SEARCH_REQUEST));
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
    RouterTxnOps op;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + 
           sizeof(key_offset) + 
           sizeof(remaster) + 
           sizeof(op));

    // get row and offset
    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset >> remaster >> op; // index offset in the readSet from source request node



    DCHECK(dec.size() == 0);

    if(remaster == true){
      // remaster, not transfer
      value_size = 0;
    }

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + 
                        sizeof(uint64_t) + sizeof(key_offset) + sizeof(success) + 
                        sizeof(remaster) + sizeof(op) +
                        value_size;
    
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionSSMessage::ASYNC_SEARCH_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;

    uint64_t latest_tid;

    success = table.contains(key);
    if(!success){
      LOG(INFO) << "  dont Exist " << *(int*)key ; // << " " << tid_int;
      encoder << latest_tid << key_offset << success << remaster << op;
      responseMessage.data.append(value_size, 0);
      responseMessage.flush();
      return;
    }

    std::atomic<uint64_t> &tid = table.search_metadata(key);
    // try to lock tuple. Ensure not locked by current node
    latest_tid = TwoPLHelper::write_lock(tid, success); // be locked 

    if(!success){ // VLOG(DEBUG_V12) 
      auto test = my_debug_key(table_id, partition_id, key);
      LOG(INFO) << "  can't Lock " << *(int*)key << " " <<  test; // << " " << tid_int;
      encoder << latest_tid << key_offset << success << remaster << op;
      responseMessage.data.append(value_size, 0);
      responseMessage.flush();
      return;
    } else {
      VLOG(DEBUG_V12) << " Lock " << *(int*)key << " " << tid << " " << latest_tid;
    }

    ycsb::ycsb::key k(*(size_t*)key % 200000 / 50000 + 200000 * partition_id);
    ITable &router_lock_table = *db.find_router_lock_table(table_id, partition_id);
    std::atomic<uint64_t> &lock_tid = router_lock_table.search_metadata((void*) &k);

    if(op == RouterTxnOps::ADD_REPLICA){
      
    } else {
      // LOG(INFO) << "LOCK ROUTER " << *(int*)key << " " << static_cast<int>(op);
      TwoPLHelper::write_lock(lock_tid, success); // be locked 
      if(!success){
        TwoPLHelper::write_lock_release(tid);
        auto test = my_debug_key(table_id, partition_id, key);
        LOG(INFO) << " TRANSMIT_REQUEST!!! can't Lock router table " << *(int*)key << " " <<  test; // << " " << tid_int;
        encoder << latest_tid << key_offset << success << remaster << op;
        responseMessage.data.append(value_size, 0);
        responseMessage.flush();
        return;
      }
    }


    if(remaster == false || context.migration_only > 0) {
      // simulate cost of transmit data
      for (auto i = 0u; i < context.n_nop * 2; i++) {
        asm("nop");
      }
    } else if(remaster == true) {
        for (auto i = 0u; i < context.n_nop * 2 / 5; i++) {
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
      auto test = my_debug_key(table_id, partition_id, key);
      // LOG(INFO) << table_id <<" " << *(int*) key << " REMASTER request switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << tid.load() << " " << latest_tid << " static: " << static_coordinator_id << " remaster: " << remaster << " " << test << " " << success;
      
      // update the router 
      router_val->set_dynamic_coordinator_id(coordinator_id_new);
      router_val->set_secondary_coordinator_id(coordinator_id_new);

    } else if(coordinator_id_new == coordinator_id_old) {
      success = true;
      // auto test = my_debug_key(table_id, partition_id, key);
      // LOG(INFO) << " Same coordi : " << coordinator_id_new << " " <<coordinator_id_old << " " << *(int*)key << " " << test << " " << tid;
      // encoder << latest_tid << key_offset << success << remaster;
      // responseMessage.data.append(value_size, 0);
      // responseMessage.flush();
    
    } else {
      DCHECK(false);
    }
    encoder << latest_tid << key_offset << success << remaster << op;
    // reserve size for read
    responseMessage.data.append(value_size, 0);
    
    // auto value = table.search_value(key);
    // LOG(INFO) << *(int*)key << " " <<  " success: " << success << "" << " remaster: " << remaster;//  << " " << new_secondary_coordinator_id; (char*)value <<
    
    if(success == true && remaster == false){
      // transfer: read from db and load data into message buffer
      void *dest =
          &responseMessage.data[0] + responseMessage.data.size() - value_size;
      auto row = table.search(key);
      TwoPLHelper::read(row, dest, value_size);
    }
    responseMessage.flush();
    // wait for the commit / abort to unlock
    TwoPLHelper::write_lock_release(tid);
    
    if(op == RouterTxnOps::ADD_REPLICA){
      
    } else {
      TwoPLHelper::write_lock_release(lock_tid);
    }
  }


  static void async_search_response_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                      Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionSSMessage::ASYNC_SEARCH_RESPONSE));
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
    RouterTxnOps op;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> tid >> key_offset >> success >> remaster >> op;

    if(remaster == true){
      value_size = 0;
    }

    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                  sizeof(tid) +
                                                  sizeof(key_offset) + sizeof(success) + sizeof(remaster) + 
                                                  sizeof(op) +
                                                  value_size);


    LionSSRWKey &readKey = txn->readSet[key_offset];
    auto key = readKey.get_key();

    auto test = my_debug_key(table_id, partition_id, key);
    // LOG(INFO) << "TRANSMIT_RESPONSE " << table_id << " "
    //                                   << test << " " << *(int*)key << " "
    //                                   << success << " " << responseMessage.get_dest_node_id() << " -> " << responseMessage.get_source_node_id() ;

    uint64_t last_tid = 0;


    if(success == true){
      // update router 
      auto key = readKey.get_key();
      auto value = readKey.get_value();

      if(!remaster){
        // read value message piece
        stringPiece = inputPiece.toStringPiece();
        stringPiece.remove_prefix(sizeof(tid) + sizeof(key_offset) + sizeof(success) +
                                  sizeof(remaster) + 
                                  sizeof(op));
        // insert into local node
        dec = Decoder(stringPiece);
        dec.read_n_bytes(readKey.get_value(), value_size);
        DCHECK(strlen((char*)readKey.get_value()) > 0);

        VLOG(DEBUG_V12) << *(int*) key << " " << (char*) value << " insert ";
        table.insert(key, value, (void*)& tid);
      }

      // lock the respond tid and key
      std::atomic<uint64_t> &tid_ = table.search_metadata(key);
      // bool success = false;

      // last_tid = TwoPLHelper::write_lock(tid_, success);
      //   VLOG(DEBUG_V14) << "LOCK-write " << *(int*)key << " " << success << " " << readKey.get_dynamic_coordinator_id() << " " << readKey.get_router_value()->get_secondary_coordinator_id_printed() << " " << last_tid;

      // if(!success){
      //   VLOG(DEBUG_V14) << "AFTER REMASETER, FAILED TO GET LOCK : " << *(int*)key << " " << tid; // 
      //   txn->abort_lock = true;
      //   return;
      // } 

      // LOG(INFO) << "REMASTER: " << table_id <<" " << *(int*) key << " " << (char*)readKey.get_value() << " reponse switch " << " " << " " << tid << "  " << remaster << " | " << success << " ";

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
      // LOG(INFO) << "FAILED TO GET LOCK : " << *(int*)key << " " << tid; // 
      txn->abort_lock = true;
    }
    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();

  }
  static void async_search_request_router_only_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                      Transaction *txn) {
    /**
     * @brief directly move the data to the request node!
     * 修改其他机器上的路由情况， 当前机器不涉及该事务的处理，可以认为事务涉及的数据主节点都不在此，直接处理就可以
     * Transaction *txn unused
     */
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionSSMessage::ASYNC_SEARCH_REQUEST_ROUTER_ONLY));
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
    RouterTxnOps op;
    uint32_t new_destination;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + 
           sizeof(key_offset) + 
           sizeof(op) + 
           sizeof(new_destination));

    // get row and offset
    const void *key = stringPiece.data();
    // auto row = table.search(key);
     // LOG(INFO) << "TRANSMIT_ROUTER_ONLY_REQUEST " << *(int*)key;
    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset >> op >> new_destination; // index offset in the readSet from source request node

    DCHECK(dec.size() == 0);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + value_size +
                        sizeof(uint64_t) + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionSSMessage::ASYNC_SEARCH_RESPONSE_ROUTER_ONLY), message_size,
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
    auto coordinator_id_new = responseMessage.get_dest_node_id(); // new_destination;// 
    if(coordinator_id_new != coordinator_id_old){
      auto router_table = db.find_router_table(table_id); // , coordinator_id_new);
      auto router_val = (RouterValue*)router_table->search_value(key);
      // // LOG(INFO) << *(int*) key << " delete " << coordinator_id_old << " --> " << coordinator_id_new;
      router_val->set_dynamic_coordinator_id(coordinator_id_new);
      router_val->set_secondary_coordinator_id(coordinator_id_new);
    }

    // if(context.coordinator_id == context.coordinator_num){
    //   LOG(INFO) << "transmit_router_only_request_handler : " << *(int*)key << " " << coordinator_id_old << " -> " << coordinator_id_new;
    // }
    responseMessage.flush();
  }

  static void async_search_response_router_only_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                      Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionSSMessage::ASYNC_SEARCH_RESPONSE_ROUTER_ONLY));
    // LOG(INFO) << "TRANSMIT_ROUTER_ONLY_RESPONSE";
    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
  }




  static void read_lock_request_handler(MessagePiece inputPiece,
                                        Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                        Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionSSMessage::READ_LOCK_REQUEST));
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

      if(success){
        ycsb::ycsb::key k(*(size_t*)key % 200000 / 50000 + 200000 * partition_id);
        ITable &router_lock_table = *db.find_router_lock_table(table_id, partition_id);
        std::atomic<uint64_t> &lock_tid = router_lock_table.search_metadata((void*) &k);
        TwoPLHelper::write_lock(lock_tid, success); // be locked 

        if(!success){
          // 
          // LOG(INFO) << " Failed to add write lock, since current is being migrated" << *(int*)key;
          TwoPLHelper::read_lock_release(tid);
        } else {
          TwoPLHelper::write_lock_release(lock_tid);
        }
      }
    }
    

    // prepare response message header
    auto message_size =
        MessagePiece::get_header_size() + sizeof(bool) + sizeof(key_offset);

    if (success) {
      message_size += value_size + sizeof(uint64_t);
    }

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionSSMessage::READ_LOCK_RESPONSE), message_size,
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
           static_cast<uint32_t>(LionSSMessage::READ_LOCK_RESPONSE));
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
    LionSSRWKey &readKey = txn->readSet[key_offset];

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
      // LOG(INFO) << "failed READ_LOCK_RESPONSE " << *(int*)readKey.get_key();
      txn->abort_lock = true;
    }

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
  }

  static void write_lock_request_handler(MessagePiece inputPiece,
                                         Message &responseMessage,
                                         Database &db, const Context &context,  Partitioner *partitioner, Transaction *txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionSSMessage::WRITE_LOCK_REQUEST));
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
      // 
      if(success){
        ycsb::ycsb::key k(*(size_t*)key % 200000 / 50000 + 200000 * partition_id);
        ITable &router_lock_table = *db.find_router_lock_table(table_id, partition_id);
        std::atomic<uint64_t> &lock_tid = router_lock_table.search_metadata((void*) &k);
        TwoPLHelper::write_lock(lock_tid, success); // be locked 

        if(!success){
          // 
          // LOG(INFO) << " Failed to add write lock, since current is being migrated" << *(int*)key;
          TwoPLHelper::write_lock_release(tid);
        } else {
          TwoPLHelper::write_lock_release(lock_tid);
        }
      }
    }
    // prepare response message header
    auto message_size =
        MessagePiece::get_header_size() + sizeof(bool) + sizeof(key_offset);

    if (success) {
      message_size += value_size + sizeof(uint64_t);
    }

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(LionSSMessage::WRITE_LOCK_RESPONSE), message_size,
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
           static_cast<uint32_t>(LionSSMessage::WRITE_LOCK_RESPONSE));
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

    LionSSRWKey &readKey = txn->readSet[key_offset];

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
      // LOG(INFO) << "failed READ_LOCK_RESPONSE";
      txn->abort_lock = true;
    }

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
  }

  static void abort_request_handler(MessagePiece inputPiece,
                                    Message &responseMessage, Database &db, const Context &context,  Partitioner *partitioner,
                                    Transaction *txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(LionSSMessage::ABORT_REQUEST));
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
           static_cast<uint32_t>(LionSSMessage::WRITE_REQUEST));
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
        static_cast<uint32_t>(LionSSMessage::WRITE_RESPONSE), message_size,
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
           static_cast<uint32_t>(LionSSMessage::WRITE_RESPONSE));
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
           static_cast<uint32_t>(LionSSMessage::REPLICATION_REQUEST));
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
        static_cast<uint32_t>(LionSSMessage::REPLICATION_RESPONSE), message_size,
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
           static_cast<uint32_t>(LionSSMessage::REPLICATION_RESPONSE));
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
           static_cast<uint32_t>(LionSSMessage::RELEASE_READ_LOCK_REQUEST));
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
    //     static_cast<uint32_t>(LionSSMessage::RELEASE_READ_LOCK_RESPONSE),
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
  //          static_cast<uint32_t>(LionSSMessage::RELEASE_READ_LOCK_RESPONSE));
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
           static_cast<uint32_t>(LionSSMessage::RELEASE_WRITE_LOCK_REQUEST));
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
    //     static_cast<uint32_t>(LionSSMessage::RELEASE_WRITE_LOCK_RESPONSE),
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
  //          static_cast<uint32_t>(LionSSMessage::RELEASE_WRITE_LOCK_RESPONSE));
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
    v.push_back(LionSSMessageHandler::transmit_request_handler);
    v.push_back(LionSSMessageHandler::transmit_response_handler);
    v.push_back(LionSSMessageHandler::transmit_router_only_request_handler);
    v.push_back(LionSSMessageHandler::transmit_router_only_response_handler);
    // 

    v.push_back(LionSSMessageHandler::async_search_request_handler); // SEARCH_REQUEST
    v.push_back(LionSSMessageHandler::async_search_response_handler); // SEARCH_RESPONSE
    v.push_back(LionSSMessageHandler::async_search_request_router_only_handler); // SEARCH_REQUEST_ROUTER_ONLY
    v.push_back(LionSSMessageHandler::async_search_response_router_only_handler); // SEARCH_RESPONSE_ROUTER_ONLY

    v.push_back(LionSSMessageHandler::read_lock_request_handler);
    v.push_back(LionSSMessageHandler::read_lock_response_handler);
    v.push_back(LionSSMessageHandler::write_lock_request_handler);
    v.push_back(LionSSMessageHandler::write_lock_response_handler);
    v.push_back(LionSSMessageHandler::abort_request_handler);
    v.push_back(LionSSMessageHandler::write_request_handler);
    v.push_back(LionSSMessageHandler::write_response_handler);
    // 
    v.push_back(LionSSMessageHandler::replication_request_handler);
    v.push_back(LionSSMessageHandler::replication_response_handler);
    //
    v.push_back(LionSSMessageHandler::release_read_lock_request_handler);
    // v.push_back(LionSSMessageHandler::release_read_lock_response_handler);
    v.push_back(LionSSMessageHandler::release_write_lock_request_handler);
    // v.push_back(LionSSMessageHandler::release_write_lock_response_handler);

    return v;
  }
};

} // namespace star
