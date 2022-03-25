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

  ReadModifyWrite(std::size_t coordinator_id, std::size_t partition_id,
                  DatabaseType &db, const ContextType &context,
                  RandomType &random, Partitioner &partitioner,
                  Storage &storage)
      : Transaction(coordinator_id, partition_id, partitioner), db(db),
        context(context), random(random), storage(storage),
        partition_id(partition_id),
        query(makeYCSBQuery<keys_num>()(context, partition_id, random, db)) {}

  virtual ~ReadModifyWrite() override = default;

  TransactionResult execute(std::size_t worker_id) override {

    DCHECK(context.keysPerTransaction == keys_num);

    int ycsbTableID = ycsb::tableID;

    for (auto i = 0u; i < keys_num; i++) {
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

    if (this->process_requests(worker_id)) {
      return TransactionResult::ABORT;
    }

    for (auto i = 0u; i < keys_num; i++) {
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
  }

  TransactionResult prepare_read_execute(std::size_t worker_id) override {
    
    DCHECK(context.keysPerTransaction == keys_num);

    int ycsbTableID = ycsb::tableID;

    for (auto i = 0u; i < keys_num; i++) {
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
  TransactionResult read_execute(std::size_t worker_id, bool local_read_only) override {
    TransactionResult ret = TransactionResult::READY_TO_COMMIT; 

    if(local_read_only){
      if (this->process_local_requests(worker_id)) {
        ret = TransactionResult::NOT_LOCAL_NORETRY;
      }
    } else {
      if (this->process_requests(worker_id)) {
        ret = TransactionResult::ABORT;
      }
    }
    return ret;
  };
  TransactionResult prepare_update_execute(std::size_t worker_id) override {
    int ycsbTableID = ycsb::tableID;

    for (auto i = 0u; i < keys_num; i++) {
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

  
  TransactionResult local_execute(std::size_t worker_id) override {

    DCHECK(context.keysPerTransaction == keys_num);

    int ycsbTableID = ycsb::tableID;

    for (auto i = 0u; i < keys_num; i++) {
      auto key = query.Y_KEY[i];
      storage.ycsb_keys[i].Y_KEY = key;

      auto key_partition_id = key / context.keysPerPartition;
      
      if (query.UPDATE[i]) {
        this->search_for_update(ycsbTableID, key_partition_id,
                                storage.ycsb_keys[i], storage.ycsb_values[i]);
      } else {
        this->search_for_read(ycsbTableID, key_partition_id,
                              storage.ycsb_keys[i], storage.ycsb_values[i]);
      }
    }

    if (this->process_local_requests(worker_id)) {
      // 

      return TransactionResult::NOT_LOCAL_NORETRY;
    }

    for (auto i = 0u; i < keys_num; i++) {
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
  }


  void reset_query() override {
    query = makeYCSBQuery<keys_num>()(context, partition_id, random, db);
  }
  
  const std::vector<u_int64_t> get_query() override{
    using T = u_int64_t;

    std::vector<T> record_keys;

    for (auto i = 0u; i < keys_num; i++) {
      auto key = static_cast<T>(query.Y_KEY[i]);
      record_keys.push_back(key);
    }
    return record_keys;
  }
  
   bool check_cross_txn(bool& success) override{
    /**
     * @brief 判断是不是跨分区事务
     * @return true/false
     */
        size_t ycsbTableID = ycsb::ycsb::tableID;
        auto query_keys = this->get_query();

        int32_t first_key;
        size_t first_key_partition_id;
        bool is_cross_txn = false;
        for (size_t j = 0 ; j < query_keys.size(); j ++ ){
          // judge if is cross txn
          if(j == 0){
            first_key = query_keys[j];
            first_key_partition_id = db.getPartitionID(context, ycsbTableID, first_key);
            if(first_key_partition_id == context.partition_num){
              // cant find this key in current partition
              success = false;
              break;
            }
          } else {
            auto cur_key = query_keys[j];
            auto cur_key_partition_id = db.getPartitionID(context, ycsbTableID, cur_key);
            if(cur_key_partition_id == context.partition_num) {
              success = false;
              break;
            }
            if(cur_key_partition_id != first_key_partition_id){
              is_cross_txn = true;
              break;
            }
          }
        }
    return is_cross_txn;
  }

   bool check_cross_node_txn(bool is_dynamic) override{
    /**
     * @brief must be master and local 判断是不是跨节点事务
     * @return true/false
     */
    std::set<int> from_nodes_id;
    from_nodes_id.insert(context.coordinator_id);
    
    size_t ycsbTableID = ycsb::ycsb::tableID;
    auto query_keys = this->get_query();

    bool is_cross_txn = false;
    for (size_t j = 0 ; j < query_keys.size(); j ++ ){
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

      // if(j == 0){
      //   first_key = query_keys[j];
      //   first_key_coordinator_id = db.getPartitionID(context, ycsbTableID, first_key);
      //   if(first_key_coordinator_id == context.partition_num){
      //     // cant find this key in current partition
      //     success = false;
      //     break;
      //   }
      // } else {
      //   auto cur_key = query_keys[j];
      //   auto cur_key_coordinator_id = db.getPartitionID(context, ycsbTableID, cur_key);
      //   if(cur_key_coordinator_id == context.partition_num) {
      //     success = false;
      //     break;
      //   }
      //   if(cur_key_coordinator_id != first_key_coordinator_id){
      //     is_cross_txn = true;
      //     break;
      //   }
      // }
    }
    return from_nodes_id.size() > 1; 
  }
private:
  DatabaseType &db;
  const ContextType &context;
  RandomType &random;
  Storage &storage;
  std::size_t partition_id;
  YCSBQuery<keys_num> query;
};
} // namespace ycsb

} // namespace star
