//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "benchmark/ycsb/Context.h"
#include "benchmark/ycsb/Random.h"
#include "benchmark/ycsb/Schema.h"
#include "common/Operation.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "core/RouterValue.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <glog/logging.h>
#include <thread>
#include <unordered_map>
#include <vector>
#include <set> 

namespace star {
namespace ycsb {
class Database {
public:
  using MetaDataType = std::atomic<uint64_t>;
  using ContextType = Context;
  using RandomType = Random;

  ITable *find_table(std::size_t table_id, std::size_t partition_id) {
    DCHECK(table_id < tbl_vecs.size());
    DCHECK(partition_id < tbl_vecs[table_id].size());
    return tbl_vecs[table_id][partition_id];
  }

  ITable *find_router_table(std::size_t table_id) { // , std::size_t coordinator_id
    // 找某个节点的路由表
    // DCHECK(table_id < tbl_vecs.size());
    // DCHECK(coordinator_id < tbl_vecs[table_id].size());
    return tbl_vecs_router[table_id]; // tbl_vecs_router[table_id][coordinator_id];
  }

  std::size_t get_dynamic_coordinator_id(size_t coordinator_num, std::size_t table_id, const void* key){
    /**
     * @brief from router table to find the coordinator
     * 
     */
    ITable* tab = find_router_table(table_id); // , coordinator_id);
    return ((RouterValue*)(tab->search_value(key)))->get_dynamic_coordinator_id();
  }

  void init_router_table(const Context& context){
    /**
     * @brief for Lion only.
     * 
     */
    auto keysPerPartition = context.keysPerPartition;
    auto partitionNum = context.partition_num;
    std::size_t totalKeys = keysPerPartition * partitionNum;


    for (auto j = 0u; j < partitionNum; j++) {
      auto partitionID = j;
      
      if (context.strategy == PartitionStrategy::RANGE) {
        // use range partitioning
        for (auto i = partitionID * keysPerPartition; i < (partitionID + 1) * keysPerPartition; i++) {
          DCHECK(context.getPartitionID(i) == partitionID);
          ycsb::key key(i);

          // if(context.protocol == "Hermes"){
          //   int router_coordinator = (partitionID) % context.coordinator_num;
          //   size_t router_secondary_coordinator = (partitionID + 1) % context.coordinator_num;

          //   ITable *table_router = tbl_ycsb_vec_router[router_coordinator].get(); // 两个不能相同
          //   table_router->insert(&key, &router_secondary_coordinator); // 
          // } else {
          int router_coordinator = (partitionID + 1) % context.coordinator_num;
          size_t router_secondary_coordinator = (partitionID) % context.coordinator_num;

          ITable *table_router = tbl_vecs_router[0]; // tbl_ycsb_vec_router.get(); // 两个不能相同

          RouterValue router;
          router.set_dynamic_coordinator_id(router_coordinator);
          router.set_secondary_coordinator_id(router_coordinator);
          router.set_secondary_coordinator_id(router_secondary_coordinator);
          table_router->insert(&key, &router); // 

          // }
        }
      } else {
        // not available so far
        DCHECK(false);
      }
    }
  }

  void init_star_router_table(const Context& context){
    /**
     * @brief for Star only.
     * 
     */
    auto keysPerPartition = context.keysPerPartition;
    auto partitionNum = context.partition_num;
    std::size_t totalKeys = keysPerPartition * partitionNum;
    

    for (auto j = 0u; j < partitionNum; j++) {
      auto partitionID = j;
      
      if (context.strategy == PartitionStrategy::RANGE) {
        // use range partitioning
        for (auto i = partitionID * keysPerPartition; i < (partitionID + 1) * keysPerPartition; i++) {
          DCHECK(context.getPartitionID(i) == partitionID);
          ycsb::key key(i);

          int router_coordinator = partitionID % context.coordinator_num;
          ITable *table_router = tbl_vecs_router[0];

          RouterValue router;
          router.set_dynamic_coordinator_id(0);
          router.set_secondary_coordinator_id(0);
          router.set_secondary_coordinator_id(router_coordinator);
          table_router->insert(&key, &router); // 

          // if(*(int*)(&key) == 7200034){
          //   LOG(INFO) << "*(int*)(key) == 7200034 " << router_coordinator ;
          // }
        }
      } else {
        // use round-robin hash partitioning
        DCHECK(false);
      }
    }
  }

  template <class InitFunc>
  void initTables(const std::string &name, InitFunc initFunc,
                  std::size_t partitionNum, std::size_t threadsNum,
                  Partitioner *partitioner) {

    std::vector<int> all_parts;

    for (auto i = 0u; i < partitionNum; i++) {
      if (partitioner == nullptr ||
          partitioner->is_partition_replicated_on_me(i)) {
        all_parts.push_back(i);
      }
    }

    std::vector<std::thread> v;
    auto now = std::chrono::steady_clock::now();

    for (auto threadID = 0u; threadID < threadsNum; threadID++) {
      v.emplace_back([=]() {
        for (auto i = threadID; i < all_parts.size(); i += threadsNum) {
          auto partitionID = all_parts[i];
          initFunc(partitionID);
        }
      });
    }
    for (auto &t : v) {
      t.join();
    }
    LOG(INFO) << name << " initialization finished in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - now)
                     .count()
              << " milliseconds.";
  }

  void initialize(const Context &context) {

    coordinator_id = context.coordinator_id;
    partitionNum = context.partition_num;
    threadsNum = context.worker_num;

    auto partitioner = PartitionerFactory::create_partitioner(
        context.partitioner, coordinator_id, context.coordinator_num);

    auto ycsbTableID = ycsb::tableID;
    for (auto partitionID = 0u; partitionID < partitionNum; partitionID++) {
      tbl_ycsb_vec.push_back(
          std::make_unique<Table<9973, ycsb::key, ycsb::value>>(ycsbTableID,
                                                                partitionID));
    }

    // there is 1 table in ycsb
    tbl_vecs.resize(1);

    auto tFunc = [](std::unique_ptr<ITable> &table) { return table.get(); };

    std::transform(tbl_ycsb_vec.begin(), tbl_ycsb_vec.end(),
                   std::back_inserter(tbl_vecs[0]), tFunc);

    using std::placeholders::_1;
    initTables("ycsb",
               [&context, this](std::size_t partitionID) {
                 ycsbInit(context, partitionID);
               },
               partitionNum, threadsNum, partitioner.get());
    
    // initalize_router_table
    if(true){ // context.protocol == "Lion"
      // quick look-up for certain-key on which node, pre-allocate space
      // for(size_t i = 0 ; i <= context.coordinator_num; i ++ ){
      tbl_ycsb_vec_router = (std::make_unique<Table<9973, ycsb::key, RouterValue>>(ycsbTableID, 0));
          // std::make_unique<Table<9973, ycsb::key, size_t>>(ycsbTableID, i)); // 
      // }
      tbl_vecs_router.resize(1);
      tbl_vecs_router[0] = tbl_ycsb_vec_router.get();
      // std::transform(tbl_ycsb_vec_router.begin(), tbl_ycsb_vec_router.end(),
      //               std::back_inserter(tbl_vecs_router[0]), tFunc);
      // init router information
      if(context.protocol == "Lion" || context.protocol == "LionNS" || context.protocol == "Hermes" || 
         context.protocol == "MyClay"){
        init_router_table(context);
      } else if (context.protocol == "Star"){
        init_star_router_table(context);
      }
    }
  }

  void apply_operation(const Operation &operation) {
    CHECK(false); // not supported
  }


  template<typename KeyType>
  std::set<int32_t> getPartitionIDs(const star::Context &context, KeyType key) const{
    // 返回这个key所在的partition
    std::set<int32_t> res;
    size_t i = 0;
    for( ; i < context.partition_num; i ++ ){
      ITable *table = tbl_ycsb_vec[i].get();
      bool is_exist = table->contains((void*)& key);
      if(is_exist){
        res.insert(i);
      }
    }
    return res;
  }
private:
  void ycsbInit(const Context &context, std::size_t partitionID) {

    Random random;
    ITable *table = tbl_ycsb_vec[partitionID].get();

    std::size_t keysPerPartition =
        context.keysPerPartition; // 5M keys per partition
    std::size_t partitionNum = context.partition_num;
    std::size_t totalKeys = keysPerPartition * partitionNum;

    if (context.strategy == PartitionStrategy::RANGE) {

      // use range partitioning

      for (auto i = partitionID * keysPerPartition;
           i < (partitionID + 1) * keysPerPartition; i++) {

        DCHECK(context.getPartitionID(i) == partitionID);

        ycsb::key key(i);
        ycsb::value value;
        value.Y_F01.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F02.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F03.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F04.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F05.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F06.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F07.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F08.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F09.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F10.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));

        table->insert(&key, &value);
      }

    } else {

      // use round-robin hash partitioning

      for (auto i = partitionID; i < totalKeys; i += partitionNum) {

        DCHECK(context.getPartitionID(i) == partitionID);

        ycsb::key key(i);
        ycsb::value value;
        value.Y_F01.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F02.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F03.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F04.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F05.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F06.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F07.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F08.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F09.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F10.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));

        table->insert(&key, &value);
      }
    }
  }

private:
  std::vector<std::vector<ITable *>> tbl_vecs;
  std::vector<ITable *> tbl_vecs_router;
  
  std::vector<std::unique_ptr<ITable>> tbl_ycsb_vec;
  std::unique_ptr<ITable> tbl_ycsb_vec_router; // table_id, coordinator_id
                                                            // key
                                                            // 
  std::size_t coordinator_id; // = context.coordinator_id;
  std::size_t partitionNum; // = context.partition_num;
  std::size_t threadsNum; // = context.worker_num;
};
} // namespace ycsb
} // namespace star
