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
// need to reconsider
  const double my_threshold = 0.001; // 20 0000 
                                      //  0.001 = 200  
  const int period_duration = 5;

  using DatabaseType = Database;
  YCSBQuery<N> operator()(const Context &context, uint32_t partitionID,
                          Random &random, DatabaseType &db, double cur_timestamp) const {
    // 
    DCHECK(context.partition_num > partitionID);

    YCSBQuery<N> query;
    int readOnly = random.uniform_dist(1, 100);
    int crossPartition = random.uniform_dist(1, 100);
    

    int32_t key;
    int32_t first_key; // 一开始的key
    // 
    int32_t key_range = partitionID;

    int workload_type = which_workload_(crossPartition, (int)cur_timestamp);

    // generate a key in a partition
    // if (crossPartition <= context.crossPartitionProbability &&
    //       context.partition_num > 1) {
    //     // 保障跨分区
    //     if(key_range % 2 == 0){ // partition even
    //       first_key = (int32_t(key_range / 2) * 2)  * static_cast<int32_t>(context.keysPerPartition) + 
    //               random.uniform_dist(0, 
    //                                   my_threshold / 2 * (static_cast<int>(context.keysPerPartition) - 1));
    //     } else { // partition odd
    //       first_key = (int32_t(key_range / 2) * 2)  * static_cast<int32_t>(context.keysPerPartition) + 
    //               random.uniform_dist(my_threshold / 2 * (static_cast<int>(context.keysPerPartition) - 1) + 1, 
    //                                   my_threshold * (static_cast<int>(context.keysPerPartition) - 1));
    //     }
    // } else {
        // 单分区
        first_key = key_range * static_cast<int32_t>(context.keysPerPartition) + 
                random.uniform_dist(0, my_threshold * (static_cast<int>(context.keysPerPartition) - 1));
    // }

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

      // first key 
      if(i == 0){
        query.Y_KEY[i] = first_key;
        continue;
      }

      // other keys
      bool retry;
      do {
        retry = false;
        if (crossPartition <= context.crossPartitionProbability &&
            context.partition_num > 1) {
          // 跨分区
          int32_t key_num =  first_key % static_cast<int32_t>(context.keysPerPartition); // 分区内偏移
          int32_t key_partition_num = first_key / static_cast<int32_t>(context.keysPerPartition); // 分区偏移

          // 对应的几类偏移
          key = (key_partition_num + workload_type) * static_cast<int32_t>(context.keysPerPartition) + key_num * N + i; 
          key = key % static_cast<int32_t>(context.keysPerPartition * context.partition_num);
        } else {
          // 单分区
          auto random_int32 = random.uniform_dist(0, my_threshold * (static_cast<int>(context.keysPerPartition) - 1));
          key = first_key + random_int32; 
          
          if(key >= (key_range + 1) * static_cast<int32_t>(context.keysPerPartition) - 1 ){ 
            key = first_key - random_int32;
          }
        }

        query.Y_KEY[i] = key;

        // ensure not repeated 
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

  YCSBQuery<N> operator()(const int32_t Y_KEY[N], bool UPDATE[N]) const {
    YCSBQuery<N> query;
    std::vector<int> hh;
    for(size_t i = 0 ; i < N; i ++ ){
      query.Y_KEY[i] = Y_KEY[i];
      query.UPDATE[i] = UPDATE[i];
    }
    return query;
  }
  
  int which_workload_(int which_type_workload, int cur_timestamp) const {

    int x1 = (int)cur_timestamp % (period_duration / 2) - period_duration / 4; // (period_duration / 2) s for a circle
    int x2 = (int)cur_timestamp % period_duration - period_duration / 2;       // (period_duration) s for a circle 

    double cur_val[5] = {0, gd(x1, 0, 0.2),  gd(x1, 2, 1.0), gd(x2, 0, 5.0), gd(x2, -5, 0.5)};
    
    double all_ = cur_val[1] + cur_val[2] + cur_val[3] + cur_val[4];
    // sqrt(cur_val[1]*cur_val[1] + cur_val[2]*cur_val[2] + cur_val[3]*cur_val[3] + cur_val[4]*cur_val[4]);
    cur_val[1] = cur_val[1] / all_;
    cur_val[2] = cur_val[2] / all_;
    cur_val[3] = cur_val[3] / all_;
    cur_val[4] = cur_val[4] / all_;

    double cur_ratio[6] = {0, 
                           cur_val[1] * 100, 
                           (cur_val[1] + cur_val[2]) * 100, 
                           (cur_val[1] + cur_val[2] + cur_val[3]) * 100,
                           (cur_val[1] + cur_val[2] + cur_val[3] + cur_val[4]) * 100, 
                           100};

    
    int workload_type = -1;
    for(int i = 0 ; i < 5; i ++ ){
      if(cur_ratio[i] < which_type_workload && which_type_workload <= cur_ratio[i + 1]){
        workload_type = i + 1;
        break;
      }
    }

    DCHECK(workload_type != -1) <<  cur_ratio[1] << " " <<  cur_ratio[2] << " " <<  cur_ratio[3] <<  " " << cur_ratio[4];

    return workload_type;
  }
private:
  double gd(double x, double mu, double sigma) const {
    /* 根据公式, 由自变量x计算因变量的值
        Argument:
          x: array
            输入数据（自变量）
          mu: float
            均值
          sigma: float
            方差
    */
    double pi = 3.1415926;
    double left = 1 / (sqrt(2 * pi) * sqrt(sigma));
    double right = exp(-(x - mu)*(x - mu) / (2 * sigma));
    return left * right;
  }


};
} // namespace ycsb
} // namespace star