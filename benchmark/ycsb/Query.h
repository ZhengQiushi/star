//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "benchmark/ycsb/Context.h"
#include "benchmark/ycsb/Random.h"
#include "common/Zipf.h"
#include "benchmark/ycsb/Database.h"
#include <vector>

namespace star {
namespace ycsb {

struct YCSBQuery {
  std::vector<size_t> Y_KEY;
  std::vector<bool> UPDATE;
  YCSBQuery(size_t query_size){
    Y_KEY.resize(query_size, 0);
    UPDATE.resize(query_size, false);
  }
  std::vector<size_t> record_keys;// for migration
};

class makeYCSBQuery {
public:
// need to reconsider
  // const double my_threshold = 0.05; // 20 0000 
  // double my_threshold = 0.05;
  
                                      //  0.001 = 200  
  const int period_duration = 5;

  // const int hot_area_size = 6;
  static const double get_thresh(const Context &context){
    if(context.protocol == "MyClay"){
      return 0.02;
    } else {
      return 0.02;
    }
  }
  static int get_workload_type(const Context &context, double cur_timestamp){
    int workload_type_num = 4;
    int workload_type = ((int)cur_timestamp / context.workload_time % workload_type_num);// 
    return workload_type;
  }
  static int round(int a, int b){
    return a / b * b;
  }
  
  using DatabaseType = Database;
  YCSBQuery operator()(const Context &context, 
                       uint32_t partitionID,// uint32_t hot_area_size,
                       Random &random, 
                       DatabaseType &db, 
                       double cur_timestamp, 
                       size_t query_size) const {
    // 
    DCHECK(context.partition_num > partitionID);

    YCSBQuery query(query_size);
    int readOnly = random.uniform_dist(1, 100);
    int crossPartition = random.uniform_dist(1, 100);

    double my_threshold = get_thresh(context);

    int32_t key;
    int32_t first_key; // 一开始的key
    // 
    int32_t key_range = partitionID;
    int factor = 10;

    
    int workload_type = get_workload_type(context, cur_timestamp);
    int last_workload_type = get_workload_type(context, 
                                      std::max(0.0, cur_timestamp - 1.0 * context.workload_time / factor));
    static int tests = 0;
    bool show = false;
    // if(tests ++ % 10000 == 0){
    //   LOG(INFO) << tests << " " << cur_timestamp << " " <<  cur_timestamp - 1.0 * context.workload_time / factor <<  " "  << workload_type << " " << last_workload_type;
    //   show = true;
    // }
    if(workload_type != last_workload_type){
      int last_type_ratio = 30 * 
      (context.workload_time - (int)cur_timestamp % context.workload_time) / 
      context.workload_time;

      int lastWorkload = random.uniform_dist(1, 100);
      if(lastWorkload <= last_type_ratio){
        workload_type = last_workload_type;
      } else {
        workload_type = workload_type + 0;
      }
      if(show){
        LOG(INFO) << last_type_ratio << " " << context.workload_time << "-" << (int)cur_timestamp % context.workload_time;
      }
    }

    int cross_partition_probalility = context.crossPartitionProbability ; 

      switch (workload_type)
      {
      case 0:
        // context.skew_factor = 0;
        cross_partition_probalility = 0;
        break;
      case 1:
        // context.skew_factor = 80;
        cross_partition_probalility = 0;
        break;
      case 2:
        // context.skew_factor = 80;
        cross_partition_probalility = 100;
        break;
      case 3:
        // context.skew_factor = 80;
        cross_partition_probalility = 100;
        break;
      default:
        break;
      }

    if(cur_timestamp < context.init_time){
      cross_partition_probalility = 0;
    }
    
    int range = my_threshold * (static_cast<int>(context.keysPerPartition) - 1);
    // generate a key in a partition
    if (crossPartition <= cross_partition_probalility &&
        context.partition_num > 1) {
        first_key = key_range * static_cast<int32_t>(context.keysPerPartition) + 
                random.uniform_dist(0, range);
    } else {
      // 单分区
      int offset = context.keysPerPartition / 2 + range;

      first_key = key_range * static_cast<int32_t>(context.keysPerPartition) + 
                random.uniform_dist(offset,
                                    offset + range * 10) / 10 * 10;
    }

    int first_key_index = random.uniform_dist(0, query_size - 1);
    for (auto i = 0u; i < query_size; i++) {
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
      if(i == 0){ // first_key_index
        query.Y_KEY[i] = first_key;
        continue;
      }

      // other keys
      bool retry;
      do {
        retry = false;
        if (crossPartition <= cross_partition_probalility &&
            context.partition_num > 1) {
          // 跨分区
          int32_t key_num =  first_key % static_cast<int32_t>(context.keysPerPartition); // 分区内偏移
          int32_t key_partition_num = first_key / static_cast<int32_t>(context.keysPerPartition) + 2; // 分区偏移
          

          if(key_partition_num < 0){
            key_partition_num += context.partition_num;
          }

          // 对应的几类偏移
          key = (key_partition_num) * static_cast<int32_t>(context.keysPerPartition) 
                + key_num * query_size + i
                + 2 * my_threshold * static_cast<int>(context.keysPerPartition); 
          key = key % static_cast<int32_t>(context.keysPerPartition * context.partition_num);
        } else {
          key = first_key + i;
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

    // LOG(INFO) << "cur_timestamp: " << cur_timestamp << " " << workload_type << " " << query.Y_KEY[0] << " " << query.Y_KEY[1];

    return query;
  }

  YCSBQuery operator()(const std::vector<size_t>& Y_KEY, const std::vector<bool>& UPDATE) const {
    YCSBQuery query(Y_KEY.size());
    for(size_t i = 0 ; i < Y_KEY.size(); i ++ ){
      query.Y_KEY[i] = Y_KEY[i];
      query.UPDATE[i] = UPDATE[i];
    }
    return query;
  }
  
  int which_workload_(int which_type_workload, int cur_timestamp) const {
    /**
     * @brief given random `which_type_workload` and 4 kinds of workloads
     *        select the most used one as current workload 
     * @return [1, 5]
     */

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