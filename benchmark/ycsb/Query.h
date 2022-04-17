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
  const double my_threshold = 0.001; // 20 0000 
                                      // 0.00005 = 10 ...
  using DatabaseType = Database;
  YCSBQuery<N> operator()(const Context &context, uint32_t partitionID,
                          Random &random, DatabaseType &db) const {
    // 
    DCHECK(context.partition_num > partitionID);

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
        // 保障跨分区
        if(key_range % 2 == 0){ // partition even
          first_key = (int32_t(key_range / 2) * 2)  * static_cast<int32_t>(context.keysPerPartition) + 
                  random.uniform_dist(0, 
                                      my_threshold / 2 * (static_cast<int>(context.keysPerPartition) - 1));
        } else { // partition odd
          first_key = (int32_t(key_range / 2) * 2)  * static_cast<int32_t>(context.keysPerPartition) + 
                  random.uniform_dist(my_threshold / 2 * (static_cast<int>(context.keysPerPartition) - 1) + 1, 
                                      my_threshold * (static_cast<int>(context.keysPerPartition) - 1));
        }
          // first_key = (int32_t(key_range / 2) * 2)  * static_cast<int32_t>(context.keysPerPartition) + 
          //       key_range % 2 * my_threshold * (static_cast<int>(context.keysPerPartition) - 1) / 2 + 
          //       random.uniform_dist(0, my_threshold * (static_cast<int>(context.keysPerPartition) - 1) / 2 - 1);
    } else {
      // 单分区
        first_key = key_range * static_cast<int32_t>(context.keysPerPartition) + 
              random.uniform_dist((1 - my_threshold) * (static_cast<int>(context.keysPerPartition) - 1), static_cast<int>(context.keysPerPartition) - 1);
    }

    // forward or backward
    // int forward_or_backward = random.uniform_dist(1, 100);

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
        if (crossPartition <= context.crossPartitionProbability &&
            context.partition_num > 1) {
          // 跨分区
          int32_t key_num =  first_key % static_cast<int32_t>(context.keysPerPartition);
          int32_t key_partition_num = first_key / static_cast<int32_t>(context.keysPerPartition);

          key = (key_partition_num + 1) * static_cast<int32_t>(context.keysPerPartition) + key_num * N + i;
        } else {
          // 单分区
          auto random_int32 = random.uniform_dist(0, my_threshold * (static_cast<int>(context.keysPerPartition) - 1));
          key = first_key + random_int32; // key_range * static_cast<int32_t>(context.keysPerPartition) + 
          
          if(key >= (key_range + 1) * static_cast<int32_t>(context.keysPerPartition) - 1 ){ // static_cast<int32_t>(context.keysPerPartition) * static_cast<int32_t>(context.partition_num)){
            key = first_key - random_int32;// static_cast<int32_t>(context.keysPerPartition) * static_cast<int32_t>(context.partition_num) - 1;
          }
        }

        query.Y_KEY[i] = key;

        for (auto k = 0u; k < i; k++) {
          // if(query.Y_KEY[k] > 7000000){
          //   LOG(INFO) << "query.Y_KEY[k] > 7400000 : " << query.Y_KEY[k];
          // }
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

  YCSBQuery<N> operator()(const int32_t Y_KEY[N], bool UPDATE[N]) const {
    YCSBQuery<N> query;
    std::vector<int> hh;
    for(size_t i = 0 ; i < N; i ++ ){
      query.Y_KEY[i] = Y_KEY[i];
      query.UPDATE[i] = UPDATE[i];
    }
    return query;
  }
};
} // namespace ycsb
} // namespace star