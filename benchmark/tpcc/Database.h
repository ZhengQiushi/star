//
// Created by Yi Lu on 7/18/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <random>
#include <thread>
#include <unordered_map>
#include <vector>
#include <set>

#include "benchmark/tpcc/Context.h"
#include "benchmark/tpcc/Random.h"
#include "benchmark/tpcc/Schema.h"
#include "common/Operation.h"
#include "common/Time.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include <glog/logging.h>

namespace star {
namespace tpcc {

class Database {
public:
  using MetaDataType = std::atomic<uint64_t>;
  using ContextType = Context;
  using RandomType = Random;

  ITable *find_table(std::size_t table_id, std::size_t partition_id) {
    DCHECK(table_id < tbl_vecs.size());
    if(partition_id >= tbl_vecs[table_id].size()){
      LOG(INFO) << partition_id <<" " << tbl_vecs[table_id].size();
      DCHECK(partition_id < tbl_vecs[table_id].size());
    }
    return tbl_vecs[table_id][partition_id];
  }

  ITable *find_router_table(std::size_t table_id, std::size_t coordinator_id) {
    DCHECK(table_id < tbl_vecs.size());
    DCHECK(coordinator_id < tbl_vecs[table_id].size());
    return tbl_vecs_router[table_id][coordinator_id];
  }

  ITable *tbl_warehouse(std::size_t partition_id) {
    DCHECK(partition_id < tbl_warehouse_vec.size());
    return tbl_warehouse_vec[partition_id].get();
  }

  ITable *tbl_district(std::size_t partition_id) {
    DCHECK(partition_id < tbl_district_vec.size());
    return tbl_district_vec[partition_id].get();
  }

  ITable *tbl_customer(std::size_t partition_id) {
    DCHECK(partition_id < tbl_customer_vec.size());
    return tbl_customer_vec[partition_id].get();
  }

  ITable *tbl_customer_name_idx(std::size_t partition_id) {
    DCHECK(partition_id < tbl_customer_name_idx_vec.size());
    return tbl_customer_name_idx_vec[partition_id].get();
  }

  ITable *tbl_history(std::size_t partition_id) {
    DCHECK(partition_id < tbl_history_vec.size());
    return tbl_history_vec[partition_id].get();
  }

  ITable *tbl_new_order(std::size_t partition_id) {
    DCHECK(partition_id < tbl_new_order_vec.size());
    return tbl_new_order_vec[partition_id].get();
  }

  ITable *tbl_order(std::size_t partition_id) {
    DCHECK(partition_id < tbl_order_vec.size());
    return tbl_order_vec[partition_id].get();
  }

  ITable *tbl_order_line(std::size_t partition_id) {
    DCHECK(partition_id < tbl_order_line_vec.size());
    return tbl_order_line_vec[partition_id].get();
  }

  ITable *tbl_item(std::size_t partition_id) {
    DCHECK(partition_id < tbl_item_vec.size());
    return tbl_item_vec[partition_id].get();
  }

  ITable *tbl_stock(std::size_t partition_id) {
    DCHECK(partition_id < tbl_stock_vec.size());
    return tbl_stock_vec[partition_id].get();
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

  void init_router_table(const Context& context){
    /**
     * @brief for Lion only.
     * 
     */
    auto keysPerPartition = context.keysPerPartition;
    auto partitionNum = context.partition_num;
    auto coordinatorNum = context.coordinator_num;

    std::size_t totalKeys = keysPerPartition * partitionNum;
    
    for (auto p_id = 0u; p_id < partitionNum; p_id++) {
      auto partitionID = p_id;
      int router_coordinator = partitionID % context.coordinator_num;
      // void warehouseInit(std::size_t partitionID) {
      warehouse::key key;
      key.W_ID = partitionID + 1; // partitionID is from 0, W_ID is from 1
      ITable *table_router = tbl_warehouse_vec_router[router_coordinator].get();
      table_router->insert(&key, &router_coordinator);

      // void districtInit(std::size_t partitionID)
      table_router = tbl_district_vec_router[router_coordinator].get();
      for (int i = 1; i <= 10; i++) {
        district::key key;
        key.D_W_ID = partitionID + 1;
        key.D_ID = i;
        table_router->insert(&key, &router_coordinator);
      }

      // customerInit(std::size_t partitionID)
      table_router = tbl_customer_vec_router[router_coordinator].get();
      for (int i = 1; i <= 10; i++) {
        for (int j = 1; j <= 3000; j++) {

          customer::key key;
          key.C_W_ID = partitionID + 1;
          key.C_D_ID = i;
          key.C_ID = j;

          table_router->insert(&key, &router_coordinator);
        }
      }

      // customerNameIdxInit
      table_router = tbl_customer_name_idx_vec_router[router_coordinator].get();
      // todo

      // stockInit
      table_router = tbl_stock_vec_router[router_coordinator].get();
      for (int i = 1; i <= 100000; i++) {
        stock::key key;
        key.S_W_ID = partitionID + 1; // partition_id from 0, W_ID from 1
        key.S_I_ID = i;
        table_router->insert(&key, &router_coordinator);
      }

  }

    // item init 
    for(auto c_id = 0u; c_id < coordinatorNum; c_id ++ ){

      int router_coordinator = int(c_id);
      ITable *table = tbl_item_vec_router[router_coordinator].get();

      // 100,000 rows in the ITEM table
      for (int i = 1; i <= 100000; i++) {
        item::key key;
        key.I_ID = i;

        table->insert(&key, &router_coordinator);
      }

    }

  }

  void allocate_router_table(const Context& context){

    for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
      auto warehouseTableID = warehouse::tableID;
      tbl_warehouse_vec_router.push_back(
          std::make_unique<Table<997, warehouse::key, warehouse::value>>(
              warehouseTableID, i));
      auto districtTableID = district::tableID;
      tbl_district_vec_router.push_back(
          std::make_unique<Table<997, district::key, district::value>>(
              districtTableID, i));
      auto customerTableID = customer::tableID;
      tbl_customer_vec_router.push_back(
          std::make_unique<Table<997, customer::key, customer::value>>(
              customerTableID, i));
      auto customerNameIdxTableID = customer_name_idx::tableID;
      tbl_customer_name_idx_vec_router.push_back(
          std::make_unique<
              Table<997, customer_name_idx::key, customer_name_idx::value>>(
              customerNameIdxTableID, i));
      auto historyTableID = history::tableID;
      tbl_history_vec_router.push_back(
          std::make_unique<Table<997, history::key, history::value>>(
              historyTableID, i));
      auto newOrderTableID = new_order::tableID;
      tbl_new_order_vec_router.push_back(
          std::make_unique<Table<997, new_order::key, new_order::value>>(
              newOrderTableID, i));
      auto orderTableID = order::tableID;
      tbl_order_vec_router.push_back(
          std::make_unique<Table<997, order::key, order::value>>(

              orderTableID, i));
      auto orderLineTableID = order_line::tableID;
      tbl_order_line_vec_router.push_back(
          std::make_unique<Table<997, order_line::key, order_line::value>>(
              orderLineTableID, i));
      auto stockTableID = stock::tableID;
      tbl_stock_vec_router.push_back(

          std::make_unique<Table<997, stock::key, stock::value>>(stockTableID,
                                                                 i));

      auto itemTableID = item::tableID;
      tbl_item_vec_router.push_back(
        std::make_unique<Table<997, item::key, item::value>>(itemTableID, i));


    }    
  }


  void initialize(const Context &context) {
    //
    
    std::size_t coordinator_id = context.coordinator_id;
    std::size_t partitionNum = context.partition_num;
    std::size_t threadsNum = context.worker_num;

    auto partitioner = PartitionerFactory::create_partitioner(
        context.partitioner, coordinator_id, context.coordinator_num);

    auto now = std::chrono::steady_clock::now();

    LOG(INFO) << "creating hash tables for database...";

    for (auto partitionID = 0u; partitionID < partitionNum; partitionID++) {
      auto warehouseTableID = warehouse::tableID;
      tbl_warehouse_vec.push_back(
          std::make_unique<Table<997, warehouse::key, warehouse::value>>(
              warehouseTableID, partitionID));
      auto districtTableID = district::tableID;
      tbl_district_vec.push_back(
          std::make_unique<Table<997, district::key, district::value>>(
              districtTableID, partitionID));
      auto customerTableID = customer::tableID;
      tbl_customer_vec.push_back(
          std::make_unique<Table<997, customer::key, customer::value>>(
              customerTableID, partitionID));
      auto customerNameIdxTableID = customer_name_idx::tableID;
      tbl_customer_name_idx_vec.push_back(
          std::make_unique<
              Table<997, customer_name_idx::key, customer_name_idx::value>>(
              customerNameIdxTableID, partitionID));
      auto historyTableID = history::tableID;
      tbl_history_vec.push_back(
          std::make_unique<Table<997, history::key, history::value>>(
              historyTableID, partitionID));
      auto newOrderTableID = new_order::tableID;
      tbl_new_order_vec.push_back(
          std::make_unique<Table<997, new_order::key, new_order::value>>(
              newOrderTableID, partitionID));
      auto orderTableID = order::tableID;
      tbl_order_vec.push_back(
          std::make_unique<Table<997, order::key, order::value>>(

              orderTableID, partitionID));
      auto orderLineTableID = order_line::tableID;
      tbl_order_line_vec.push_back(
          std::make_unique<Table<997, order_line::key, order_line::value>>(
              orderLineTableID, partitionID));
      auto stockTableID = stock::tableID;
      tbl_stock_vec.push_back(

          std::make_unique<Table<997, stock::key, stock::value>>(stockTableID,
                                                                 partitionID));
    }
    auto itemTableID = item::tableID;
    tbl_item_vec.push_back(
        std::make_unique<Table<997, item::key, item::value>>(itemTableID, 0));


    // quick look-up for certain-key on which node
    allocate_router_table(context);


    // there are 10 tables in tpcc
    tbl_vecs.resize(10);
    tbl_vecs_router.resize(10);


    auto tFunc = [](std::unique_ptr<ITable> &table) { return table.get(); };

    std::transform(tbl_warehouse_vec.begin(), tbl_warehouse_vec.end(),
                   std::back_inserter(tbl_vecs[0]), tFunc);
    std::transform(tbl_district_vec.begin(), tbl_district_vec.end(),
                   std::back_inserter(tbl_vecs[1]), tFunc);
    std::transform(tbl_customer_vec.begin(), tbl_customer_vec.end(),
                   std::back_inserter(tbl_vecs[2]), tFunc);
    std::transform(tbl_customer_name_idx_vec.begin(),
                   tbl_customer_name_idx_vec.end(),
                   std::back_inserter(tbl_vecs[3]), tFunc);
    std::transform(tbl_history_vec.begin(), tbl_history_vec.end(),
                   std::back_inserter(tbl_vecs[4]), tFunc);
    std::transform(tbl_new_order_vec.begin(), tbl_new_order_vec.end(),
                   std::back_inserter(tbl_vecs[5]), tFunc);
    std::transform(tbl_order_vec.begin(), tbl_order_vec.end(),
                   std::back_inserter(tbl_vecs[6]), tFunc);
    std::transform(tbl_order_line_vec.begin(), tbl_order_line_vec.end(),
                   std::back_inserter(tbl_vecs[7]), tFunc);
    std::transform(tbl_item_vec.begin(), tbl_item_vec.end(),
                   std::back_inserter(tbl_vecs[8]), tFunc);
    std::transform(tbl_stock_vec.begin(), tbl_stock_vec.end(),
                   std::back_inserter(tbl_vecs[9]), tFunc);

    // 
    std::transform(tbl_warehouse_vec_router.begin(), tbl_warehouse_vec_router.end(),
                   std::back_inserter(tbl_vecs_router[0]), tFunc);
    std::transform(tbl_district_vec_router.begin(), tbl_district_vec_router.end(),
                   std::back_inserter(tbl_vecs_router[1]), tFunc);
    std::transform(tbl_customer_vec_router.begin(), tbl_customer_vec_router.end(),
                   std::back_inserter(tbl_vecs_router[2]), tFunc);
    std::transform(tbl_customer_name_idx_vec_router.begin(),
                   tbl_customer_name_idx_vec_router.end(),
                   std::back_inserter(tbl_vecs_router[3]), tFunc);
    std::transform(tbl_history_vec_router.begin(), tbl_history_vec_router.end(),
                   std::back_inserter(tbl_vecs_router[4]), tFunc);
    std::transform(tbl_new_order_vec_router.begin(), tbl_new_order_vec_router.end(),
                   std::back_inserter(tbl_vecs_router[5]), tFunc);
    std::transform(tbl_order_vec_router.begin(), tbl_order_vec_router.end(),
                   std::back_inserter(tbl_vecs_router[6]), tFunc);
    std::transform(tbl_order_line_vec_router.begin(), tbl_order_line_vec_router.end(),
                   std::back_inserter(tbl_vecs_router[7]), tFunc);
    std::transform(tbl_item_vec_router.begin(), tbl_item_vec_router.end(),
                   std::back_inserter(tbl_vecs_router[8]), tFunc);
    std::transform(tbl_stock_vec_router.begin(), tbl_stock_vec_router.end(),
                   std::back_inserter(tbl_vecs_router[9]), tFunc);


    DLOG(INFO) << "hash tables created in "
               << std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - now)
                      .count()
               << " milliseconds.";

    using std::placeholders::_1;
    initTables("warehouse",
               [this](std::size_t partitionID) { warehouseInit(partitionID); },
               partitionNum, threadsNum, partitioner.get());
    initTables("district",
               [this](std::size_t partitionID) { districtInit(partitionID); },
               partitionNum, threadsNum, partitioner.get());
    initTables("customer",
               [this](std::size_t partitionID) { customerInit(partitionID); },
               partitionNum, threadsNum, partitioner.get());
    initTables(
        "customer_name_idx",
        [this](std::size_t partitionID) { customerNameIdxInit(partitionID); },
        partitionNum, threadsNum, partitioner.get());
    /*
    initTables("history",
               [this](std::size_t partitionID) { historyInit(partitionID); },
               partitionNum, threadsNum, partitioner.get());
    initTables("new_order",
               [this](std::size_t partitionID) { newOrderInit(partitionID); },
               partitionNum, threadsNum, partitioner.get());
    initTables("order",
               [this](std::size_t partitionID) { orderInit(partitionID); },
               partitionNum, threadsNum, partitioner.get());
    initTables("order_line",
               [this](std::size_t partitionID) { orderLineInit(partitionID); },
               partitionNum, threadsNum, partitioner.get());
    */
    initTables("item",
               [this](std::size_t partitionID) { itemInit(partitionID); }, 1, 1, // 只需要一个分区, 因为没有写操作... 
               nullptr);
    initTables("stock",
               [this](std::size_t partitionID) { stockInit(partitionID); },
               partitionNum, threadsNum, partitioner.get());

    
    init_router_table(context);
  }
  
  template<typename KeyType>
  std::size_t getPartitionID(const star::Context &context, std::size_t table_id, KeyType key) const{
    // 返回这个key所在的partition
    auto which_table = tbl_vecs[table_id];

    size_t i = 0;
    for( ; i < context.partition_num; i ++ ){
      ITable *table = which_table[i];
      bool is_exist = table->contains((void*)& key);
      if(is_exist)
        break;
    }
    // DCHECK(i != context.partition_num);
    return i;
  }

  template<typename KeyType>
  std::set<int32_t> getPartitionIDs(const star::Context &context, KeyType key) const
  {
    std::set<int32_t> res;
    return res;
  }

  std::size_t get_dynamic_coordinator_id(size_t coordinator_num, std::size_t table_id, const void* key){
    std::size_t ret = coordinator_num;
    for(size_t i = 0 ; i < coordinator_num; i ++ ){
      ITable* tab = find_router_table(table_id, i);
      if(tab->contains(key)){
        ret = i;
        break;
      }
    } 
    DCHECK(ret != coordinator_num);
    return ret;
  }

  void apply_operation(const Operation &operation) {

    Decoder dec(operation.data);
    bool is_neworder;
    dec >> is_neworder;

    if (is_neworder) {
      // district
      auto districtTableID = district::tableID;
      district::key district_key;
      dec >> district_key.D_W_ID >> district_key.D_ID;

      auto row =
          tbl_district_vec[district_key.D_W_ID - 1]->search(&district_key);
      MetaDataType &tid = *std::get<0>(row);
      tid.store(operation.tid);
      district::value &district_value =
          *static_cast<district::value *>(std::get<1>(row));
      dec >> district_value.D_NEXT_O_ID;

      // stock
      auto stockTableID = stock::tableID;
      while (dec.size() > 0) {
        stock::key stock_key;
        dec >> stock_key.S_W_ID >> stock_key.S_I_ID;

        auto row = tbl_stock_vec[stock_key.S_W_ID - 1]->search(&stock_key);
        MetaDataType &tid = *std::get<0>(row);
        tid.store(operation.tid);
        stock::value &stock_value =
            *static_cast<stock::value *>(std::get<1>(row));

        dec >> stock_value.S_QUANTITY >> stock_value.S_YTD >>
            stock_value.S_ORDER_CNT >> stock_value.S_REMOTE_CNT;
      }
    } else {
      {
        // warehouse
        auto warehouseTableID = warehouse::tableID;
        warehouse::key warehouse_key;
        dec >> warehouse_key.W_ID;

        auto row =
            tbl_warehouse_vec[warehouse_key.W_ID - 1]->search(&warehouse_key);
        MetaDataType &tid = *std::get<0>(row);
        tid.store(operation.tid);

        warehouse::value &warehouse_value =
            *static_cast<warehouse::value *>(std::get<1>(row));

        dec >> warehouse_value.W_YTD;
      }

      {
        // district
        auto districtTableID = district::tableID;
        district::key district_key;
        dec >> district_key.D_W_ID >> district_key.D_ID;

        auto row =
            tbl_district_vec[district_key.D_W_ID - 1]->search(&district_key);
        MetaDataType &tid = *std::get<0>(row);
        tid.store(operation.tid);

        district::value &district_value =
            *static_cast<district::value *>(std::get<1>(row));

        dec >> district_value.D_YTD;
      }

      {
        // custoemer
        auto customerTableID = customer::tableID;
        customer::key customer_key;
        dec >> customer_key.C_W_ID >> customer_key.C_D_ID >> customer_key.C_ID;

        auto row =
            tbl_customer_vec[customer_key.C_W_ID - 1]->search(&customer_key);
        MetaDataType &tid = *std::get<0>(row);
        tid.store(operation.tid);

        customer::value &customer_value =
            *static_cast<customer::value *>(std::get<1>(row));

        char C_DATA[501];
        const char *old_C_DATA = customer_value.C_DATA.c_str();
        uint32_t total_written;
        dec >> total_written;
        dec.read_n_bytes(C_DATA, total_written);
        std::memcpy(C_DATA + total_written, old_C_DATA, 500 - total_written);
        C_DATA[500] = 0;
        customer_value.C_DATA.assign(C_DATA);

        dec >> customer_value.C_BALANCE >> customer_value.C_YTD_PAYMENT >>
            customer_value.C_PAYMENT_CNT;
      }
    }
  }

private:
  void warehouseInit(std::size_t partitionID) {

    Random random;
    ITable *table = tbl_warehouse_vec[partitionID].get();

    warehouse::key key;
    key.W_ID = partitionID + 1; // partitionID is from 0, W_ID is from 1

    warehouse::value value;
    value.W_NAME.assign(random.a_string(6, 10));
    value.W_STREET_1.assign(random.a_string(10, 20));
    value.W_STREET_2.assign(random.a_string(10, 20));
    value.W_CITY.assign(random.a_string(10, 20));
    value.W_STATE.assign(random.a_string(2, 2));
    value.W_ZIP.assign(random.rand_zip());
    value.W_TAX = static_cast<float>(random.uniform_dist(0, 2000)) / 10000;
    value.W_YTD = 30000;

    table->insert(&key, &value);
  }

  void districtInit(std::size_t partitionID) {

    Random random;
    ITable *table = tbl_district_vec[partitionID].get();

    // For each row in the WAREHOUSE table, 10 rows in the DISTRICT table

    for (int i = 1; i <= 10; i++) {

      district::key key;
      key.D_W_ID = partitionID + 1;
      key.D_ID = i;

      district::value value;
      value.D_NAME.assign(random.a_string(6, 10));
      value.D_STREET_1.assign(random.a_string(10, 20));
      value.D_STREET_2.assign(random.a_string(10, 20));
      value.D_CITY.assign(random.a_string(10, 20));
      value.D_STATE.assign(random.a_string(2, 2));
      value.D_ZIP.assign(random.rand_zip());
      value.D_TAX = static_cast<float>(random.uniform_dist(0, 2000)) / 10000;
      value.D_YTD = 30000;
      value.D_NEXT_O_ID = 3001;

      table->insert(&key, &value);
    }
  }

  void customerInit(std::size_t partitionID) {

    Random random;
    ITable *table = tbl_customer_vec[partitionID].get();

    // For each row in the WAREHOUSE table, 10 rows in the DISTRICT table
    // For each row in the DISTRICT table, 3,000 rows in the CUSTOMER table

    for (int i = 1; i <= 10; i++) {
      for (int j = 1; j <= 3000; j++) {

        customer::key key;
        key.C_W_ID = partitionID + 1;
        key.C_D_ID = i;
        key.C_ID = j;

        customer::value value;
        value.C_MIDDLE.assign("OE");
        value.C_FIRST.assign(random.a_string(8, 16));
        value.C_STREET_1.assign(random.a_string(10, 20));
        value.C_STREET_2.assign(random.a_string(10, 20));
        value.C_CITY.assign(random.a_string(10, 20));
        value.C_STATE.assign(random.a_string(2, 2));
        value.C_ZIP.assign(random.rand_zip());
        value.C_PHONE.assign(random.n_string(16, 16));
        value.C_SINCE = Time::now();
        value.C_CREDIT_LIM = 50000;
        value.C_DISCOUNT =
            static_cast<float>(random.uniform_dist(0, 5000)) / 10000;
        value.C_BALANCE = -10;
        value.C_YTD_PAYMENT = 10;
        value.C_PAYMENT_CNT = 1;
        value.C_DELIVERY_CNT = 1;
        value.C_DATA.assign(random.a_string(300, 500));

        int last_name;

        if (j <= 1000) {
          last_name = j - 1;
        } else {
          last_name = random.non_uniform_distribution(255, 0, 999);
        }

        value.C_LAST.assign(random.rand_last_name(last_name));

        // For 10% of the rows, selected at random , C_CREDIT = "BC"

        int x = random.uniform_dist(1, 10);

        if (x == 1) {
          value.C_CREDIT.assign("BC");
        } else {
          value.C_CREDIT.assign("GC");
        }

        table->insert(&key, &value);
      }
    }
  }

  void customerNameIdxInit(std::size_t partitionID) {

    Random random;
    ITable *table = tbl_customer_name_idx_vec[partitionID].get();

    // For each row in the WAREHOUSE table, 10 rows in the DISTRICT table
    // For each row in the DISTRICT table, 3,000 rows in the CUSTOMER table

    ITable *customer_table = find_table(customer::tableID, partitionID);

    std::unordered_map<FixedString<16>,
                       std::vector<std::pair<FixedString<16>, int32_t>>>
        last_name_to_first_names_and_c_ids;

    for (int i = 1; i <= 10; i++) {
      for (int j = 1; j <= 3000; j++) {
        customer::key customer_key;
        customer_key.C_W_ID = partitionID + 1;
        customer_key.C_D_ID = i;
        customer_key.C_ID = j;

        // no concurrent write, it is ok to read without validation on
        // MetaDataType
        const customer::value &customer_value = *static_cast<customer::value *>(
            customer_table->search_value(&customer_key));
        last_name_to_first_names_and_c_ids[customer_value.C_LAST].push_back(
            std::make_pair(customer_value.C_FIRST, customer_key.C_ID));
      }

      for (auto it = last_name_to_first_names_and_c_ids.begin();
           it != last_name_to_first_names_and_c_ids.end(); it++) {
        auto &v = it->second;
        std::sort(v.begin(), v.end());

        // insert ceiling(n/2) to customer_last_name_idx, n starts from 1
        customer_name_idx::key cni_key(partitionID + 1, i, it->first);
        customer_name_idx::value cni_value(v[(v.size() - 1) / 2].second);
        table->insert(&cni_key, &cni_value);
      }
    }
  }

  void historyInit(std::size_t partitionID) {

    Random random;
    ITable *table = tbl_history_vec[partitionID].get();

    // For each row in the WAREHOUSE table, 10 rows in the DISTRICT table
    // For each row in the DISTRICT table, 3,000 rows in the CUSTOMER table
    // For each row in the CUSTOMER table, 1 row in the HISTORY table

    for (int i = 1; i <= 10; i++) {
      for (int j = 1; j <= 3000; j++) {

        history::key key;

        key.H_W_ID = partitionID + 1;
        key.H_D_ID = i;
        key.H_C_W_ID = partitionID + 1;
        key.H_C_D_ID = i;
        key.H_C_ID = j;
        key.H_DATE = Time::now();

        history::value value;
        value.H_AMOUNT = 10;
        value.H_DATA.assign(random.a_string(12, 24));

        table->insert(&key, &value);
      }
    }
  }

  void newOrderInit(std::size_t partitionID) {

    Random random;
    ITable *table = tbl_new_order_vec[partitionID].get();

    // For each row in the WAREHOUSE table, 10 rows in the DISTRICT table
    // For each row in the DISTRICT table, 3,000 rows in the ORDER table
    // For each row in the ORDER table from 2101 to 3000, 1 row in the NEW_ORDER
    // table

    for (int i = 1; i <= 10; i++) {
      for (int j = 2101; j <= 3000; j++) {

        new_order::key key;
        key.NO_W_ID = partitionID + 1;
        key.NO_D_ID = i;
        key.NO_O_ID = j;

        new_order::value value;

        table->insert(&key, &value);
      }
    }
  }

  void orderInit(std::size_t partitionID) {

    Random random;
    ITable *table = tbl_order_vec[partitionID].get();

    // For each row in the WAREHOUSE table, 10 rows in the DISTRICT table
    // For each row in the DISTRICT table, 3,000 rows in the ORDER table

    std::vector<int> perm;

    for (int i = 1; i <= 3000; i++) {
      perm.push_back(i);
    }

    for (int i = 1; i <= 10; i++) {

      std::shuffle(perm.begin(), perm.end(), std::default_random_engine());

      for (int j = 1; j <= 3000; j++) {

        order::key key;
        key.O_W_ID = partitionID + 1;
        key.O_D_ID = i;
        key.O_ID = j;

        order::value value;
        value.O_C_ID = perm[j - 1];
        value.O_ENTRY_D = Time::now();
        value.O_OL_CNT = random.uniform_dist(5, 15);
        value.O_ALL_LOCAL = true;

        if (key.O_ID < 2101) {
          value.O_CARRIER_ID = random.uniform_dist(1, 10);
        } else {
          value.O_CARRIER_ID = 0;
        }

        table->insert(&key, &value);
      }
    }
  }

  void orderLineInit(std::size_t partitionID) {

    Random random;
    ITable *table = tbl_order_line_vec[partitionID].get();

    // For each row in the WAREHOUSE table, 10 rows in the DISTRICT table
    // For each row in the DISTRICT table, 3,000 rows in the ORDER table
    // For each row in the ORDER table, O_OL_CNT rows in the ORDER_LINE table

    ITable *order_table = find_table(order::tableID, partitionID);

    for (int i = 1; i <= 10; i++) {

      order::key order_key;
      order_key.O_W_ID = partitionID + 1;
      order_key.O_D_ID = i;

      for (int j = 1; j <= 3000; j++) {
        order_key.O_ID = j;

        // no concurrent write, it is ok to read without validation on
        // MetaDataType
        const order::value &order_value =
            *static_cast<order::value *>(order_table->search_value(&order_key));

        for (int k = 1; k <= order_value.O_OL_CNT; k++) {
          order_line::key key;
          key.OL_W_ID = partitionID + 1;
          key.OL_D_ID = i;
          key.OL_O_ID = j;
          key.OL_NUMBER = k;

          order_line::value value;
          value.OL_I_ID = random.uniform_dist(1, 100000);
          value.OL_SUPPLY_W_ID = partitionID + 1;
          value.OL_QUANTITY = 5;
          value.OL_DIST_INFO.assign(random.a_string(24, 24));

          if (key.OL_O_ID < 2101) {
            value.OL_DELIVERY_D = order_value.O_ENTRY_D;
            value.OL_AMOUNT = 0;
          } else {
            value.OL_DELIVERY_D = 0;
            value.OL_AMOUNT =
                static_cast<float>(random.uniform_dist(1, 999999)) / 100;
          }
          table->insert(&key, &value);
        }
      }
    }
  }

  void itemInit(std::size_t partitionID) {

    Random random;
    ITable *table = tbl_item_vec[partitionID].get();

    std::string i_original = "ORIGINAL";

    // 100,000 rows in the ITEM table

    for (int i = 1; i <= 100000; i++) {

      item::key key;
      key.I_ID = i;

      item::value value;
      value.I_IM_ID = random.uniform_dist(1, 10000);
      value.I_NAME.assign(random.a_string(14, 24));
      value.I_PRICE = random.uniform_dist(1, 100);

      std::string i_data = random.a_string(26, 50);

      /*
          For 10% of the rows, selected at random,
          the string "ORIGINAL" must be held by 8 consecutive characters
         starting at a random position within I_DATA
      */

      int x = random.uniform_dist(1, 10);

      if (x == 1) {
        int pos = random.uniform_dist(0, i_data.length() - i_original.length());
        memcpy(&i_data[0] + pos, &i_original[0], i_original.length());
      }

      value.I_DATA.assign(i_data);

      table->insert(&key, &value);
    }
  }

  void stockInit(std::size_t partitionID) {

    Random random;
    ITable *table = tbl_stock_vec[partitionID].get();

    std::string s_original = "ORIGINAL";

    // For each row in the WAREHOUSE table, 100,000 rows in the STOCK table

    for (int i = 1; i <= 100000; i++) {

      stock::key key;
      key.S_W_ID = partitionID + 1; // partition_id from 0, W_ID from 1
      key.S_I_ID = i;

      stock::value value;

      value.S_QUANTITY = random.uniform_dist(10, 100);
      value.S_DIST_01.assign(random.a_string(24, 24));
      value.S_DIST_02.assign(random.a_string(24, 24));
      value.S_DIST_03.assign(random.a_string(24, 24));
      value.S_DIST_04.assign(random.a_string(24, 24));
      value.S_DIST_05.assign(random.a_string(24, 24));
      value.S_DIST_06.assign(random.a_string(24, 24));
      value.S_DIST_07.assign(random.a_string(24, 24));
      value.S_DIST_08.assign(random.a_string(24, 24));
      value.S_DIST_09.assign(random.a_string(24, 24));
      value.S_DIST_10.assign(random.a_string(24, 24));
      value.S_YTD = 0;
      value.S_ORDER_CNT = 0;
      value.S_REMOTE_CNT = 0;

      /*
       For 10% of the rows, selected at random,
       the string "ORIGINAL" must be held by 8 consecutive characters starting
       at a random position within S_DATA
       */

      std::string s_data = random.a_string(26, 40);

      int x = random.uniform_dist(1, 10);

      if (x == 1) {
        int pos = random.uniform_dist(0, s_data.length() - s_original.length());
        memcpy(&s_data[0] + pos, &s_original[0], s_original.length());
      }

      value.S_DATA.assign(s_data);

      table->insert(&key, &value);
    }
  }

private:
  std::vector<std::vector<ITable *>> tbl_vecs;
  std::vector<std::vector<ITable *>> tbl_vecs_router;

  std::vector<std::unique_ptr<ITable>> tbl_warehouse_vec;
  std::vector<std::unique_ptr<ITable>> tbl_district_vec;
  std::vector<std::unique_ptr<ITable>> tbl_customer_vec;
  std::vector<std::unique_ptr<ITable>> tbl_customer_name_idx_vec;
  std::vector<std::unique_ptr<ITable>> tbl_history_vec;
  std::vector<std::unique_ptr<ITable>> tbl_new_order_vec;
  std::vector<std::unique_ptr<ITable>> tbl_order_vec;
  std::vector<std::unique_ptr<ITable>> tbl_order_line_vec;
  std::vector<std::unique_ptr<ITable>> tbl_item_vec;
  std::vector<std::unique_ptr<ITable>> tbl_stock_vec;


  std::vector<std::unique_ptr<ITable>> tbl_warehouse_vec_router;
  std::vector<std::unique_ptr<ITable>> tbl_district_vec_router;
  std::vector<std::unique_ptr<ITable>> tbl_customer_vec_router;
  std::vector<std::unique_ptr<ITable>> tbl_customer_name_idx_vec_router;
  std::vector<std::unique_ptr<ITable>> tbl_history_vec_router;
  std::vector<std::unique_ptr<ITable>> tbl_new_order_vec_router;
  std::vector<std::unique_ptr<ITable>> tbl_order_vec_router;
  std::vector<std::unique_ptr<ITable>> tbl_order_line_vec_router;
  std::vector<std::unique_ptr<ITable>> tbl_item_vec_router;
  std::vector<std::unique_ptr<ITable>> tbl_stock_vec_router;

};
} // namespace tpcc
} // namespace star
