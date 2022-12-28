//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "glog/logging.h"

#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Query.h"
#include "benchmark/ycsb/Schema.h"
#include "benchmark/ycsb/Storage.h"
#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"

namespace star {
namespace ycsb {

template <class Transaction> class ReadModifyWrite : public Transaction {

public:
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using StorageType = Storage;

  static constexpr std::size_t keys_num = 10;

  // static constexpr std::size_t transmit_keys_num = 120;
  

  ReadModifyWrite(std::size_t coordinator_id, std::size_t partition_id,
                  DatabaseType &db, const ContextType &context,
                  RandomType &random, Partitioner &partitioner,
                  Storage &storage, double cur_timestamp)
      : Transaction(coordinator_id, partition_id, partitioner), db(db),
        context(context), random(random), storage(storage),
        partition_id(partition_id),
        query(makeYCSBQuery()(context, partition_id, random, db, cur_timestamp, keys_num)) {
          /**
           * @brief 
           * 
           */
        }


  ReadModifyWrite(std::size_t coordinator_id, std::size_t partition_id,
                  DatabaseType &db, const ContextType &context,
                  RandomType &random, Partitioner &partitioner,
                  Storage &storage, simpleTransaction& simple_txn)
      : Transaction(coordinator_id, partition_id, partitioner), db(db),
        context(context), random(random), storage(storage),
        partition_id(partition_id),
        query(makeYCSBQuery()(simple_txn.keys, simple_txn.update)) {
          /**
           * @brief convert from the generated txns
           * 
           */
          is_transmit_request = simple_txn.is_transmit_request;          
        }

  virtual ~ReadModifyWrite() override = default;
  bool is_transmit_requests() override {
    return is_transmit_request;
  }
  TransactionResult execute(std::size_t worker_id) override {

    int ycsbTableID = ycsb::tableID;
    size_t keys_num_ = query.Y_KEY.size();

    for (auto i = 0u; i < keys_num_; i++) {
      auto key = query.Y_KEY[i];
      storage.ycsb_keys[i].Y_KEY = key;

      // LOG(INFO) << sizeof(storage.ycsb_keys[i]) << "  " << sizeof(storage.ycsb_values[i]);

      auto key_partition_id = key / context.keysPerPartition; // db.getPartitionID(context, ycsbTableID, key);
      if(key_partition_id == context.partition_num){
        // 
        return TransactionResult::ABORT;
      }
      
      if (query.UPDATE[i]) {
        this->search_for_update(ycsbTableID, key_partition_id,
                                storage.ycsb_keys[i], storage.ycsb_values[i]);
      } else {
        this->search_for_read(ycsbTableID, key_partition_id,
                              storage.ycsb_keys[i], storage.ycsb_values[i]);
      }
    }

    if (this->process_requests(worker_id)) {
      return TransactionResult::ABORT;
    } 
    
    if(is_transmit_request == true){
      // std::string str = "";
      // for(size_t i = 0 ; i < keys_num_ ; i ++ ){
      //   str += std::to_string(int(storage.ycsb_keys[i].Y_KEY)) + " ";
      // }
      // LOG(INFO) << " @@ TRANSMIT_REQUEST @@ " << str;
      return TransactionResult::TRANSMIT_REQUEST;
    }

    for (auto i = 0u; i < keys_num_; i++) {
      auto key = query.Y_KEY[i];
      if (query.UPDATE[i]) {

        if (this->execution_phase) {
          RandomType local_random;
          storage.ycsb_values[i].Y_F01.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F02.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F03.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F04.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F05.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F06.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F07.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F08.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F09.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F10.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        }
        auto key_partition_id = key / context.keysPerPartition; // db.getPartitionID(context, ycsbTableID, key);
        if(key_partition_id == context.partition_num){
          // 
          return TransactionResult::ABORT;
        }
        this->update(ycsbTableID, key_partition_id, 
                     storage.ycsb_keys[i], storage.ycsb_values[i]);
      }
    }

    if (this->execution_phase && context.nop_prob > 0) {
      auto x = random.uniform_dist(1, 10000);
      if (x <= context.nop_prob) {
        for (auto i = 0u; i < context.n_nop; i++) {
          asm("nop");
        }
      }
    }

    return TransactionResult::READY_TO_COMMIT;
  }
  TransactionResult prepare_read_execute(std::size_t worker_id) override {
    
    size_t keys_num_ = query.Y_KEY.size();

    int ycsbTableID = ycsb::tableID;

    for (auto i = 0u; i < keys_num_; i++) {
      auto key = query.Y_KEY[i];
      storage.ycsb_keys[i].Y_KEY = key;

      // LOG(INFO) << sizeof(storage.ycsb_keys[i]) << "  " << sizeof(storage.ycsb_values[i]);

      auto key_partition_id = key / context.keysPerPartition;
      // if(key_partition_id == context.partition_num){
      //   // 
      //   return TransactionResult::ABORT;
      // }
      
      if (query.UPDATE[i]) {
        this->search_for_update(ycsbTableID, key_partition_id,
                                storage.ycsb_keys[i], storage.ycsb_values[i]);
      } else {
        this->search_for_read(ycsbTableID, key_partition_id,
                              storage.ycsb_keys[i], storage.ycsb_values[i]);
      }
    }

    return TransactionResult::READY_TO_COMMIT;
  };
  TransactionResult read_execute(std::size_t worker_id, ReadMethods local_read_only) override {
    TransactionResult ret = TransactionResult::READY_TO_COMMIT; 
    switch (local_read_only)
    {
    case ReadMethods::REMOTE_READ_ONLY:
      if (this->process_read_only_requests(worker_id)) {
        ret = TransactionResult::ABORT;
      }
      break;
    case ReadMethods::LOCAL_READ:
      if (this->process_local_requests(worker_id)) {
        ret = TransactionResult::NOT_LOCAL_NORETRY;
      }
      break;
    case ReadMethods::REMOTE_READ_WITH_TRANSFER:
      if (this->process_requests(worker_id)) {
        ret = TransactionResult::ABORT;
      }
      break;
    default:
      DCHECK(false);
      break;
    }
    return ret;
  };
  TransactionResult prepare_update_execute(std::size_t worker_id) override {
    int ycsbTableID = ycsb::tableID;
    size_t keys_num_ = query.Y_KEY.size();

    for (auto i = 0u; i < keys_num_; i++) {
      auto key = query.Y_KEY[i];
      if (query.UPDATE[i]) {

        if (this->execution_phase) {
          RandomType local_random;
          storage.ycsb_values[i].Y_F01.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F02.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F03.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F04.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F05.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F06.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F07.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F08.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F09.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F10.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        }
        // auto key_partition_id = db.getPartitionID(context, ycsbTableID, key);
        // if(key_partition_id == context.partition_num){
        //   // 
        //   return TransactionResult::ABORT;
        // }
        auto key_partition_id = key / context.keysPerPartition;

        this->update(ycsbTableID, key_partition_id,  // key_partition_id
                     storage.ycsb_keys[i], storage.ycsb_values[i]);
      }
    }

    if (this->execution_phase && context.nop_prob > 0) {
      auto x = random.uniform_dist(1, 10000);
      if (x <= context.nop_prob) {
        for (auto i = 0u; i < context.n_nop; i++) {
          asm("nop");
        }
      }
    }

    return TransactionResult::READY_TO_COMMIT;
  };


  void reset_query() override {
    query = makeYCSBQuery()(context, partition_id, random, db, 0, 0);
  }
  
  const std::vector<u_int64_t> get_query() override{
    using T = u_int64_t;

    std::vector<T> record_keys;
    size_t keys_num_ = query.Y_KEY.size();
    for (auto i = 0u; i < keys_num_; i++) {
      auto key = static_cast<T>(query.Y_KEY[i]);
      record_keys.push_back(key);
    }
    return record_keys;
  }


  const std::string get_query_printed() override {
    std::string print_ = "";
    for(auto i : get_query()){
      print_ += " " + std::to_string(i);
    }
    return print_;
  }
  
   const std::vector<bool> get_query_update(){
     std::vector<bool> ret;
     size_t keys_num_ = query.Y_KEY.size();
     for(auto i = 0u; i < keys_num_; i ++ ){
       auto update = query.UPDATE[i];
       ret.push_back(update);
     }
     return ret;
   };


   std::set<int> txn_nodes_involved(bool is_dynamic) override {
      std::set<int> from_nodes_id;
      size_t ycsbTableID = ycsb::ycsb::tableID;
      auto query_keys = this->get_query();

      for (size_t j = 0 ; j < query_keys.size(); j ++ ){
        // LOG(INFO) << "query_keys[j] : " << query_keys[j];
        // judge if is cross txn
        size_t cur_c_id = -1;
        if(is_dynamic){
          // look-up the dynamic router to find-out where
          cur_c_id = db.get_dynamic_coordinator_id(context.coordinator_num, ycsbTableID, (void*)& query_keys[j]);
        } else {
          // cal the partition to figure out the coordinator-id
          cur_c_id = query_keys[j] / context.keysPerPartition % context.coordinator_num;
        }
        from_nodes_id.insert(cur_c_id);
      }
     return from_nodes_id;
   }
   std::unordered_map<int, int> txn_nodes_involved(int& max_node, bool is_dynamic) override {
      std::unordered_map<int, int> from_nodes_id;
      size_t ycsbTableID = ycsb::ycsb::tableID;
      auto query_keys = this->get_query();
      int max_cnt = 0;

      for (size_t j = 0 ; j < query_keys.size(); j ++ ){
        // LOG(INFO) << "query_keys[j] : " << query_keys[j];
        // judge if is cross txn
        size_t cur_c_id = -1;
        if(is_dynamic){
          // look-up the dynamic router to find-out where
          cur_c_id = db.get_dynamic_coordinator_id(context.coordinator_num, ycsbTableID, (void*)& query_keys[j]);
        } else {
          // cal the partition to figure out the coordinator-id
          cur_c_id = query_keys[j] / context.keysPerPartition % context.coordinator_num;
        }
        if(!from_nodes_id.count(cur_c_id)){
          from_nodes_id[cur_c_id] = 1;
        } else {
          from_nodes_id[cur_c_id] += 1;
        }
        if(from_nodes_id[cur_c_id] > max_cnt){
          max_cnt = from_nodes_id[cur_c_id];
          max_node = cur_c_id;
        }
      }
     return from_nodes_id;
   }

   bool check_cross_node_txn(bool is_dynamic) override{
    /**
     * @brief must be master and local 判断是不是跨节点事务
     * @return true/false
     */
    std::set<int> from_nodes_id = std::move(txn_nodes_involved(is_dynamic));
    // from_nodes_id.insert(context.coordinator_id);
    return from_nodes_id.size() > 1; 
  }
  std::size_t get_partition_id(){
    return partition_id;
  }
private:
  DatabaseType &db;
  const ContextType &context;
  RandomType &random;
  Storage &storage;
  std::size_t partition_id;
  YCSBQuery query;
  bool is_transmit_request;
};
} // namespace ycsb

} // namespace star
