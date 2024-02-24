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
  int parts[5];
  int granules[5][10];
  int part_granule_count[5];
  int num_parts = 0;
  bool cross_partition;
  int32_t get_part(int i) {
    DCHECK(i < num_parts);
    return parts[i];
  }

  int32_t get_part_granule_count(int i) {
    return part_granule_count[i];
  }

  int32_t get_granule(int i, int j) {
    DCHECK(i < num_parts);
    DCHECK(j < part_granule_count[i]);
    return granules[i][j];
  }

  int number_of_parts() {
    return num_parts;
  }
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
      return 0.05;
    } else {
      return 0.05;
    }
  }
  static int get_workload_type(const Context &context, double cur_timestamp){
    int workload_type_num = 3;
    int workload_type = ((int)cur_timestamp / context.workload_time % workload_type_num) + 1;// 
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
                       size_t query_size,
                       uint32_t granuleID) const {
    // 
    DCHECK(context.partition_num > partitionID);

    YCSBQuery query(query_size);
    query.cross_partition = false;
    query.num_parts = 1;
    query.parts[0] = partitionID;
    query.part_granule_count[0] = 0;

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
    if(cur_timestamp < context.init_time){
      cross_partition_probalility = 0;
    }
    
    // generate a key in a partition
    if (crossPartition <= cross_partition_probalility &&
        context.partition_num > 1) {
        first_key = key_range * static_cast<int32_t>(context.keysPerPartition) + 
                random.uniform_dist(0, my_threshold * (static_cast<int>(context.keysPerPartition) - 1));
    } else {
      // 单分区
      first_key = key_range * static_cast<int32_t>(context.keysPerPartition) + 
                random.uniform_dist(context.keysPerPartition / 2 + my_threshold * (static_cast<int>(context.keysPerPartition) - 1),
                                    (static_cast<int>(context.keysPerPartition) - 1)) / 10 * 10;
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
      int this_partition_idx = 0;
      bool retry = false;
      // first key 
      if(i == 0){ // first_key_index
        query.Y_KEY[i] = first_key;
        
        if (query.num_parts == 1) {
          query.num_parts = 1;
          
          query.parts[query.num_parts] = first_key / static_cast<int32_t>(context.keysPerPartition);
          query.part_granule_count[query.num_parts] = 0;
          query.num_parts++;
        }
        query.cross_partition = true;
        this_partition_idx = 0;
      } else {
        // other keys
        
        do {
          retry = false;
          
          if (crossPartition <= cross_partition_probalility &&
              context.partition_num > 1) {
            // 跨分区
            int32_t key_num =  first_key % static_cast<int32_t>(context.keysPerPartition); // 分区内偏移
            int32_t key_partition_num = first_key / static_cast<int32_t>(context.keysPerPartition) + workload_type; // 分区偏移
            

            if(key_partition_num < 0){
              key_partition_num += context.partition_num;
            }
            key_partition_num %= context.partition_num;

            // 对应的几类偏移
            key = (key_partition_num) * static_cast<int32_t>(context.keysPerPartition) 
                  + key_num * query_size + i
                  + 2 * my_threshold * static_cast<int>(context.keysPerPartition); 
            key = key % static_cast<int32_t>(context.keysPerPartition * context.partition_num);
            
            // 
            query.cross_partition = true;
            this_partition_idx = 1;
            query.parts[this_partition_idx] = key_partition_num;
          } else {
            key = first_key + i;
            this_partition_idx = 0;
            query.parts[this_partition_idx] = first_key / static_cast<int32_t>(context.keysPerPartition);
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
      }
      if (retry == false) {
        auto granuleId = (int)context.getGranule(query.Y_KEY[i]);
        // if(i <= 1)
        //   LOG(INFO) << granuleId << " " << query.Y_KEY[i] << " " << query.parts[this_partition_idx];

        bool good = true;
        for (int32_t k = 0; k < query.part_granule_count[this_partition_idx]; ++k) {
          if (query.granules[this_partition_idx][k] == granuleId) {
            good = false;
            break;
          }
        }
        if (good == true) {
          query.granules[this_partition_idx][query.part_granule_count[this_partition_idx]++] = granuleId;
        }
      }
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
    YCSBQuery operator()(const Context &context, uint32_t partitionID, uint32_t granuleID,
                          Random &random, const Partitioner & partitioner, double current_timestamp) const {
    const int N = 10;
    YCSBQuery query(N);
    
    query.cross_partition = false;
    query.num_parts = 1;
    query.parts[0] = partitionID;
    query.part_granule_count[0] = 0;
    int readOnly = random.uniform_dist(1, 100);
    int crossPartition = random.uniform_dist(1, 100);
    //int crossPartitionPartNum = context.crossPartitionPartNum;
    int crossPartitionPartNum = random.uniform_dist(2, context.crossPartitionPartNum);
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

        if (context.isUniform) {
          // For the first key, we ensure that it will land in the granule specified by granuleID.
          // This granule will be served as the coordinating granule
          key = i == 0 ? random.uniform_dist(
              0, static_cast<int>(context.keysPerGranule) - 1) : random.uniform_dist(
              0, static_cast<int>(context.keysPerPartition) - 1);
        } else {
          key = i == 0 ? random.uniform_dist(
              0, static_cast<int>(context.keysPerGranule) - 1) : Zipf::globalZipf().value(random.next_double());
        }
        int this_partition_idx = 0;
        if (crossPartition <= context.crossPartitionProbability &&
            context.partition_num > 1) {
          if (query.num_parts == 1) {
            query.num_parts = 1;
            for (int j = query.num_parts; j < crossPartitionPartNum; ++j) {
              if (query.num_parts >= (int)context.partition_num)
                break;
              int32_t pid = random.uniform_dist(0, context.partition_num - 1);
              do {
                bool good = true;
                for (int k = 0; k < j; ++k) {
                  if (query.parts[k] == pid) {
                    good = false;
                  }
                }
                if (good == true)
                  break;
                pid =  random.uniform_dist(0, context.partition_num - 1);
              } while(true);
              query.parts[query.num_parts] = pid;
              query.part_granule_count[query.num_parts] = 0;
              query.num_parts++;
            }
          }
          auto newPartitionID = query.parts[i % query.num_parts];
          query.Y_KEY[i] = i == 0 ? context.getGlobalKeyID(key, newPartitionID, granuleID) : context.getGlobalKeyID(key, newPartitionID);
          query.cross_partition = true;
          this_partition_idx = i % query.num_parts;
        } else {
          query.Y_KEY[i] = i == 0 ? context.getGlobalKeyID(key, partitionID, granuleID) : context.getGlobalKeyID(key, partitionID);
        }

        for (auto k = 0u; k < i; k++) {
          if (query.Y_KEY[k] == query.Y_KEY[i]) {
            retry = true;
            break;
          }
        }
        if (retry == false) {
          auto granuleId = (int)context.getGranule(query.Y_KEY[i]);
          bool good = true;
          for (int32_t k = 0; k < query.part_granule_count[this_partition_idx]; ++k) {
            if (query.granules[this_partition_idx][k] == granuleId) {
              good = false;
              break;
            }
          }
          if (good == true) {
            query.granules[this_partition_idx][query.part_granule_count[this_partition_idx]++] = granuleId;
          }
        }
      } while (retry);
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