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

  static std::size_t transfer_request_message(Message &message, 
                                          Database &db, const Context &context, Partitioner *partitioner,
                                          ITable &table, uint32_t key_offset,
                                          Transaction& txn, bool& success){
    auto &readKey = txn.readSet[key_offset];
    auto key = readKey.get_key();
    auto partition_id = readKey.get_partition_id();
    uint32_t txn_id = txn.id;

    // 该数据原来的位置
    auto table_id = table.tableID();
    auto value_size = table.value_size();

    auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key);
    auto router_table_old = db.find_router_table(table_id, coordinator_id_old);
    
    // secondary_id
    uint64_t old_secondary_coordinator_id = *(uint64_t*)router_table_old->search_value(key);
    uint64_t static_coordinator_id = partitioner->secondary_coordinator(partition_id); //; partitioner->secondary_coordinator(partition_id);

    // create the new tuple in global router of source request node
    auto coordinator_id_new = message.get_dest_node_id();
    // target_id == secondary_id then it is remaster
    bool remaster = (old_secondary_coordinator_id == coordinator_id_new);
    // bool success = true;
    uint64_t tid_int, latest_tid;

    if(remaster == true){
      // remaster, not transfer
      value_size = 0;
    }


    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + 
                        sizeof(latest_tid) + sizeof(key_offset) + sizeof(txn_id) + 
                        sizeof(remaster) + sizeof(success) + 
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
      auto router_table_new = db.find_router_table(table_id, coordinator_id_new);

      char* value_transfer = new char[value_size + 1];
      if(remaster == false){
        auto row = table.search(key);
        HermesHelper::read(row, value_transfer, value_size);
      }


      size_t new_secondary_coordinator_id;
      // be related with static replica 涉及 静态副本 
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
          DCHECK(false);// success = false;
          VLOG(DEBUG_V12) << "  Not Transfer " << *(int*)key; // << " " << tid_int;
        } else {
          // 非静态 迁到 新的非静态 1. delete old
          VLOG(DEBUG_V12) << "  DELETE " << *(int*)key << " " << coordinator_id_old << " --> " << coordinator_id_new;
          table.delete_(key);
          is_delete = true;
          // LOG(INFO) << *(int*) key << " delete " << coordinator_id_old << " --> " << coordinator_id_new;
          // 2. 
          new_secondary_coordinator_id = static_coordinator_id;
        }

      }

      encoder << latest_tid << key_offset << txn_id << success << remaster;
      // reserve size for read
      message.data.append(value_size, 0);

      if(success == true && remaster == false){
        // transfer: read from db and load data into message buffer
        void *dest =
            &message.data[0] + message.data.size() - value_size;
        memcpy(dest, value_transfer, value_size);
      }
      message.flush();

      delete[] value_transfer;

      readKey.set_master_coordinator_id(coordinator_id_new);
      readKey.set_secondary_coordinator_id(new_secondary_coordinator_id);
      
      VLOG(DEBUG_V8) << table_id << " " << *(int*) key << " request switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << " static: " << static_coordinator_id << " success " << success << 
                        "new : " << coordinator_id_new << "  new2:" << new_secondary_coordinator_id;

      // 修改路由表
      router_table_new->insert(key, &new_secondary_coordinator_id); // 
      // delete old value in router and real-replica
      router_table_old->delete_(key);
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
                                        Transaction& txn, uint32_t key_offset, uint32_t target_coordi_id) {

    /*
     * The structure of a search request: (primary key, read key offset)
     */
    uint32_t txn_id = txn.id;
    auto key = txn.readSet[key_offset].get_key();

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(key_offset) + sizeof(txn_id) + sizeof(target_coordi_id);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HermesMessage::TRANSFER_REQUEST_ROUTER_ONLY), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset << txn_id << target_coordi_id;
    message.flush();

    VLOG(DEBUG_V11) << "R   " << txn_id << " " << *(int*)key << "router send " << message.get_source_node_id() << "->" << message.get_dest_node_id() << 
                       " target_coordi_id: " << target_coordi_id  << " pending: " << txn.pendingResponses;

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
                       ITable &table,
                       std::vector<std::unique_ptr<Transaction>> &txns) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HermesMessage::READ_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto value_size = table.value_size();

    /*
     * The structure of a read request: (tid, key offset, value)
     * The structure of a read response: null
     */

    uint32_t tid;
    uint32_t key_offset;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + sizeof(tid) + sizeof(key_offset) +
               value_size);

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> tid >> key_offset;
    DCHECK(tid < txns.size());
    DCHECK(key_offset < txns[tid]->readSet.size());
    HermesRWKey &readKey = txns[tid]->readSet[key_offset];
    dec.read_n_bytes(readKey.get_value(), value_size);
    txns[tid]->remote_read.fetch_add(-1);
    VLOG(DEBUG_V12) << "    read_request_message_handler : " <<  tid << " " << txns[tid]->remote_read.load() << 
                 " " << *(int*) readKey.get_key() << 
                 " " << responseMessage.get_dest_node_id() << "->" << responseMessage.get_source_node_id();
  }

  static void
  transfer_request_message_handler(MessagePiece inputPiece, Message &responseMessage,
                       Database &db, const Context &context, Partitioner *partitioner,
                       ITable &table,
                       std::vector<std::unique_ptr<Transaction>> &txns) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HermesMessage::TRANSFER_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto value_size = table.value_size();

    /*
     * The structure of a read request: (tid, key offset, value)
     * The structure of a read response: null
     */
    uint32_t txn_id;
    uint32_t key_offset;
    uint64_t latest_tid;

    bool remaster = false;
    bool success  = true;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> latest_tid >> key_offset >> txn_id >> success >> remaster;

    auto &txn = txns[txn_id];
    auto &readKey = txn->readSet[key_offset];
    auto key = readKey.get_key();

    DCHECK(success == true) << *(int*)key;

    if(remaster == true){
      value_size = 0;
    }

    stringPiece = inputPiece.toStringPiece();
    stringPiece.remove_prefix(value_size);

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + 
           sizeof(latest_tid) + sizeof(key_offset) + sizeof(txn_id) + 
           sizeof(remaster) + sizeof(success) +  
               value_size);


    bool is_exist = table.contains(key);
    bool can_be_locked = true;

    uint64_t tidd = 0;
    uint64_t last_tid = 0;

    // if(success == true){
    //   if(is_exist){
    //     std::atomic<uint64_t>& tid_c = table.search_metadata(key);// 
    //     if(readKey.get_write_lock_bit()){
    //       last_tid = HermesHelper::write_lock(tid_c);
    //     } else {
    //       last_tid = TwoPLHelper::read_lock(tid_c);
    //     }
    //     if(can_be_locked){
    //       // table.update_metadata(key, (void*)& tid);
    //       VLOG(DEBUG_V14) << "LOCK " << *(int*)key << " " << last_tid << " " << tid_c;
    //     }
    //     tidd = tid_c.load();
    //   } else {
    //     if(readKey.get_write_lock_bit()){
    //       tidd = TwoPLHelper::write_lock(tid);
    //     } else {
    //       tidd = TwoPLHelper::read_lock(tid);
    //     }
    //   }
    // }


    if(success == true){
      // 本地没有 或者 本地有但是没被锁
      if(remaster == true){
        // read from local
        auto value = table.search_value(key);
        readKey.set_value(value);
      } else {
        // transfer read from message piece
        dec = Decoder(inputPiece.toStringPiece());
        dec.read_n_bytes(readKey.get_value(), value_size);
      }
      
        // key && value
        auto key = readKey.get_key();
        auto value = readKey.get_value();

        auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key);
        auto router_table_old = db.find_router_table(table_id, coordinator_id_old);
        
        uint64_t old_secondary_coordinator_id = *(uint64_t*)router_table_old->search_value(key);
        uint64_t static_coordinator_id = partitioner->secondary_coordinator(partition_id); // partition_id % context.coordinator_num; //; 

        // create the new tuple in global router of source request node
        auto coordinator_id_new = responseMessage.get_source_node_id(); 
        DCHECK(coordinator_id_new != coordinator_id_old);
        auto router_table_new = db.find_router_table(table_id, coordinator_id_new);

        // VLOG(DEBUG_V8) << table_id <<" " << *(int*) key << " reponse switch " << coordinator_id_old << " --> " << coordinator_id_new << " " << tidd ;
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
            if(!table.contains(key)){
              table.insert(key, value, (void*)& tidd); 
            }
            // LOG(INFO) << *(int*) key << " insert " << coordinator_id_old << " --> " << coordinator_id_new;

          }

        } else {
          // 非静态 迁到 新的非静态
          // 1. insert new
          if(remaster == true){
            // 不应该进入
            DCHECK(false);
          }
          if(!table.contains(key)){
            table.insert(key, value, (void*)& tidd);
          }
          // LOG(INFO) << *(int*) key << " insert " << coordinator_id_old << " --> " << coordinator_id_new;

          // 2. 
          new_secondary_coordinator_id = static_coordinator_id;
        }

        // 修改路由表
        if(!router_table_new->contains(key)){
          router_table_new->insert(key, &new_secondary_coordinator_id);
          router_table_old->delete_(key);
        }
        DCHECK(coordinator_id_new != new_secondary_coordinator_id);

        readKey.set_master_coordinator_id(coordinator_id_new);
        readKey.set_secondary_coordinator_id(new_secondary_coordinator_id);

    VLOG(DEBUG_V12) << "    transfer_request_message_handler : " <<  txn_id << " " << txns[txn_id]->pendingResponses << 
                 " " << *(int*) readKey.get_key() << 
                 " " << responseMessage.get_dest_node_id() << "->" << responseMessage.get_source_node_id() << 
                 " new: " << coordinator_id_new << " new2: " << new_secondary_coordinator_id;
  


    } else {
      DCHECK(false) << "FAILED TO GET LOCK : " << *(int*)key << " " << tidd;
    }

    DCHECK(txn_id < txns.size());
    DCHECK(key_offset < txns[txn_id]->readSet.size());

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
                       ITable &table,
                       std::vector<std::unique_ptr<Transaction>> &txns) {
    uint32_t txn_id;
    uint32_t key_offset;

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HermesMessage::TRANSFER_RESPONSE));

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + sizeof(txn_id) + sizeof(key_offset));    

    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    // ITable &table = *db.find_table(table_id, partition_id);

    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> txn_id >> key_offset;

    /*
     * The structure of a replication response: ()
     */
    auto& txn = txns[txn_id];
    auto& readKey = txn->readSet[key_offset];

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();  

    VLOG(DEBUG_V12) << "    transfer_response : " <<  txn_id << " " << txns[txn_id]->pendingResponses << 
                 " " << *(int*) readKey.get_key() << 
                 " " << responseMessage.get_dest_node_id() << "->" << responseMessage.get_source_node_id();
                   
  }
  


  static void transfer_request_message_router_only_handler(MessagePiece inputPiece, Message &responseMessage,
                       Database &db, const Context &context, Partitioner *partitioner,
                       ITable &table,
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
    uint32_t txn_id; 
    uint32_t target_coordi_id;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(key_offset) + sizeof(txn_id) + sizeof(target_coordi_id));

    // get row and offset
    const void *key = stringPiece.data();
    // auto row = table.search(key);

    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset >> txn_id >> target_coordi_id; // index offset in the readSet from source request node
    auto& txn = txns[txn_id];

    VLOG(DEBUG_V11) << " Req   " << txn_id << " " << *(int*)key << "response send " << responseMessage.get_source_node_id() << "->" << responseMessage.get_dest_node_id() << 
                       " target_coordi_id: " << target_coordi_id << " pending: " << txn->pendingResponses;

    DCHECK(dec.size() == 0);

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
    auto coordinator_id_old = db.get_dynamic_coordinator_id(context.coordinator_num, table_id, key);
    auto router_table_old = db.find_router_table(table_id, coordinator_id_old);
    
    // create the new tuple in global router of source request node
    auto coordinator_id_new = responseMessage.get_dest_node_id(); // target_coordi_id; // 
    if(coordinator_id_new != coordinator_id_old){
      auto router_table_new = db.find_router_table(table_id, coordinator_id_new);
      // delete old value in router and real-replica
      uint64_t secondary_coordinator_id_new = *(uint64_t*)router_table_old->search_value(key);
      if(secondary_coordinator_id_new == coordinator_id_new){
        // remaster
        secondary_coordinator_id_new = coordinator_id_old;
      } else {
        // transfer, secondary not change
      }
      router_table_new->insert(key, &coordinator_id_new);
      router_table_old->delete_(key);
    }
    // txn->pendingResponses--;

    // VLOG(DEBUG_V11) << " Req   " << txn_id << " " << *(int*)key << "response send " << responseMessage.get_source_node_id() << "->" << responseMessage.get_dest_node_id() << " pending: " << txn->pendingResponses;

    responseMessage.flush();
  }

  static void transfer_response_message_router_only_handler(MessagePiece inputPiece, Message &responseMessage,
                       Database &db, const Context &context, Partitioner *partitioner,
                       ITable &table,
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
    // txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
    
    auto& readKey = txn->readSet[key_offset];
    auto key = readKey.get_key();

    VLOG(DEBUG_V11) << " R   " << txn_id << " " << *(int*)key << "response get " << responseMessage.get_source_node_id() << "->" << responseMessage.get_dest_node_id() << " pending: " << txn->pendingResponses;

  }


  static std::vector<
      std::function<void(MessagePiece, Message &, 
                         Database &, const Context &, Partitioner *,
                         ITable &,
                         std::vector<std::unique_ptr<Transaction>> &)>>
  get_message_handlers() {
    std::vector<
        std::function<void(MessagePiece, Message &, 
                           Database &, const Context &, Partitioner *,
                           ITable &,
                           std::vector<std::unique_ptr<Transaction>> &)>>
        v;
    v.resize(static_cast<int>(ControlMessage::NFIELDS));
    v.push_back(read_request_message_handler); // transfer_request_message_handler
    v.push_back(transfer_request_message_handler); // 
    v.push_back(transfer_response_message_handler);
    v.push_back(transfer_request_message_router_only_handler);
    v.push_back(transfer_response_message_router_only_handler);
    return v;
  }
};

} // namespace star