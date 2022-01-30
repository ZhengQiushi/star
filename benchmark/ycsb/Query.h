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
  const double my_threshold = 0.0002;
  using DatabaseType = Database;
  YCSBQuery<N> operator()(const Context &context, uint32_t partitionID,
                          Random &random) const {
    // 
    YCSBQuery<N> query;
    int readOnly = random.uniform_dist(1, 100);
    int crossPartition = random.uniform_dist(1, 100);

    int32_t key;
    int32_t first_key; // 一开始的key
    // 
    int32_t key_range = partitionID;
    // generate a key in a partition
    if (crossPartition <= context.crossPartitionProbability &&
          context.partition_num > 1) {
        // 跨分区
        first_key = key_range * static_cast<int32_t>(context.keysPerPartition) + 
              random.uniform_dist(0, my_threshold * (static_cast<int>(context.keysPerPartition) - 1));
    } else {
      // 单分区
        first_key = key_range * static_cast<int32_t>(context.keysPerPartition) + 
              random.uniform_dist((1 - my_threshold) * (static_cast<int>(context.keysPerPartition) - 1), static_cast<int>(context.keysPerPartition) - 1);
    }

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

      if(i == 0){
        query.Y_KEY[i] = first_key;
        continue;
      }

      bool retry;
      do {
        retry = false;

        // auto getKeyPartitionID_ = [&](int key_) { 
        //   // 返回这个key所在的partition
        //     size_t i = 0;
        //     for( ; i < context.partition_num; i ++ ){
        //       ITable *table = db.tbl_ycsb_vec[i].get();
        //       bool is_exist = table->contains((void*)& key_);
        //       if(is_exist)
        //         break;
        //     }
        //     DCHECK(i != context.partition_num);

        //     return i;
        // }; 

        // auto newPartitionID = getKeyPartitionID_(key);
        if (crossPartition <= context.crossPartitionProbability &&
            context.partition_num > 1) {
          // 跨分区
          // int32_t key_range_tmp;
          // key_range_tmp = (key_range + 1)% context.partition_num;
                          // random.uniform_dist(
                          //      0, static_cast<int>(context.partition_num) - 1);
          // while(key_range_tmp == key_range){
          //   key_range_tmp = random.uniform_dist(
          //                       0, static_cast<int>(context.partition_num) - 1);
          // }
          if(i < N / 2){
            key = (first_key + static_cast<int32_t>(context.keysPerPartition) + 
              random.uniform_dist(0, my_threshold * (static_cast<int>(context.keysPerPartition) - 1)));
              if(key >= static_cast<int32_t>(context.keysPerPartition) * static_cast<int32_t>(context.partition_num)){
                key -= static_cast<int32_t>(context.keysPerPartition) * (context.partition_num);
              }
          } else {
            key = (first_key - static_cast<int32_t>(context.keysPerPartition) + 
              random.uniform_dist(0, my_threshold * (static_cast<int>(context.keysPerPartition) - 1)));
              //% static_cast<int32_t>(context.keysPerPartition) * (context.partition_num);
            if(key < 0){
              key += static_cast<int32_t>(context.keysPerPartition) * (context.partition_num);
            }
          }
          // newPartitionID = getKeyPartitionID_(key);
        } else {
          // 单分区
          key = first_key + // key_range * static_cast<int32_t>(context.keysPerPartition) + 
              random.uniform_dist(0, my_threshold * (static_cast<int>(context.keysPerPartition) - 1));
          if(key >= static_cast<int32_t>(context.keysPerPartition) * static_cast<int32_t>(context.partition_num)){
            key = static_cast<int32_t>(context.keysPerPartition) * static_cast<int32_t>(context.partition_num) - 1;
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

    } // end for
    return query;
  }
  // std::size_t getGlobalKeyID(std::size_t key, std::size_t partitionID, DatabaseType& db){
  //   ITable *table = db.tbl_ycsb_vec[partitionID].get();
  //   return table->get_global_key(key);
  // }


};
} // namespace ycsb
} // namespace star