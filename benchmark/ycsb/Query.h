//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "benchmark/ycsb/Context.h"
#include "benchmark/ycsb/Random.h"
#include "common/Zipf.h"
#include "benchmark/ycsb/Database.h"
namespace star {
namespace ycsb {

template <std::size_t N> struct YCSBQuery {
  int32_t Y_KEY[N];
  bool UPDATE[N];
};

template <std::size_t N> class makeYCSBQuery {
public:
  using DatabaseType = Database;
  YCSBQuery<N> operator()(const Context &context, uint32_t partitionID,
                          Random &random, DatabaseType& db) const {
    // 
    YCSBQuery<N> query;
    int readOnly = random.uniform_dist(1, 100);
    int crossPartition = random.uniform_dist(1, 100);

    for (auto i = 0u; i < N; i++) {
      // read or write

      if (readOnly <= context.readOnlyTransaction) {
        query.UPDATE[i] = false;
      } else {
        int readOrWrite = random.uniform_dist(1, 100);
        if (readOrWrite <= context.readWriteRatio) {
          query.UPDATE[i] = false;
        } else {
          query.UPDATE[i] = true;
        }
      }

      int32_t key;

      // generate a key in a partition
      bool retry;
      do {
        retry = false;

        // if (context.isUniform) {
        key = random.uniform_dist(
              0, static_cast<int>(context.keysPerPartition * context.partition_num) - 1);
        // } else {
        //   key = Zipf::globalZipf().value(random.next_double());
        // }
        auto getKeyPartitionID_ = [&](int key_) { 
            size_t i = 0;
            for( ; i < context.partition_num; i ++ ){
              ITable *table = db.tbl_ycsb_vec[i].get();
              bool is_exist = table->contains((void*)& key_);
              if(is_exist)
                break;
            }
            DCHECK(i != context.partition_num);

            return i;
        }; 

        auto newPartitionID = getKeyPartitionID_(key);
        if (crossPartition <= context.crossPartitionProbability &&
            context.partition_num > 1) {
          // 跨分区
          while (newPartitionID == partitionID) {
            key = random.uniform_dist(
              0, static_cast<int>(context.keysPerPartition * context.partition_num) - 1);
            newPartitionID = getKeyPartitionID_(key);
          }
        } 

        query.Y_KEY[i] = key;

        for (auto k = 0u; k < i; k++) {
          if (query.Y_KEY[k] == query.Y_KEY[i]) {
            retry = true;
            break;
          }
        }
      } while (retry);

      // LOG(INFO) << query.Y_KEY[i] << " ";
    }
    // LOG(INFO) << "\n"
    return query;
  }
  // std::size_t getGlobalKeyID(std::size_t key, std::size_t partitionID, DatabaseType& db){
  //   ITable *table = db.tbl_ycsb_vec[partitionID].get();
  //   return table->get_global_key(key);
  // }

  std::size_t getKeyPartitionID(std::size_t key,const Context &context, DatabaseType& db){
    /**
     * @brief 全局key
    */
    size_t i = 0;
    for( ; i < context.partition_num; i ++ ){
      ITable *table = db.tbl_ycsb_vec[i].get();
      bool is_exist = table->contains((void*)& key);
      if(is_exist)
        break;
    }
    DCHECK(i != context.partition_num);

    return i;
  }

  std::size_t getTableKeys(std::size_t partitionID, DatabaseType& db) {
    ITable *table = db.tbl_ycsb_vec[partitionID].get();
    return table->table_record_num();
  }

};
} // namespace ycsb
} // namespace star
