//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "glog/logging.h"
#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Query.h"
#include "benchmark/tpcc/Schema.h"
#include "benchmark/tpcc/Storage.h"
#include "common/Operation.h"
#include "common/Time.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"

namespace star {
namespace tpcc {
static thread_local std::vector<Storage*> storage_cache;

Storage* get_storage() {
  if (storage_cache.empty()) {
    for (size_t i = 0; i < 10; ++i) {
      storage_cache.push_back(new Storage());
    }
  }
  Storage * last = storage_cache.back();
  storage_cache.pop_back();
  return last;
}

void put_storage(Storage * s) {
  storage_cache.push_back(s);
}
template <class Transaction> class NewOrder : public Transaction {
public:
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using StorageType = Storage;

  NewOrder(std::size_t coordinator_id, std::size_t partition_id,
           std::atomic<uint32_t> &worker_status, 
           DatabaseType &db, const ContextType &context, RandomType &random,
           Partitioner &partitioner, Storage &storage, 
           double cur_timestamp)
      : Transaction(coordinator_id, partition_id, partitioner, 0), 
        worker_status_(worker_status), db(db),
        context(context), random(random), storage(&storage),
        partition_id(partition_id),
        query(makeNewOrderQuery()(context, partition_id + 1, random, cur_timestamp)) {
          DCHECK(this->partition_id < (1 << 30));
        }

  NewOrder(std::size_t coordinator_id, std::size_t partition_id,
          std::atomic<uint32_t> &worker_status, 
           DatabaseType &db, const ContextType &context, RandomType &random,
           Partitioner &partitioner, std::size_t ith_replica)
      : Transaction(coordinator_id, partition_id, partitioner, ith_replica), 
        worker_status_(worker_status), db(db),
        context(context), random(random),
        partition_id(partition_id),
        query(makeNewOrderQuery()(context, partition_id + 1, random)) {
          storage = get_storage();
        }
  virtual ~NewOrder() { put_storage(storage); storage = nullptr; }

  NewOrder(std::size_t coordinator_id, std::size_t partition_id,  
           std::atomic<uint32_t> &worker_status, 
           DatabaseType &db, const ContextType &context,
           RandomType &random, Partitioner &partitioner,
           Storage &storage, simpleTransaction& simple_txn, bool is_transmit)
      : Transaction(coordinator_id, partition_id, partitioner, 0), 
        worker_status_(worker_status), db(db),
        context(context), random(random), storage(&storage),
        query(makeNewOrderQuery()(simple_txn.keys, is_transmit)) {
          // size_t size_ = simple_txn.keys.size();
          // DCHECK(simple_txn.keys.size() == 13);
          // 
          this->partition_id = query.W_ID - 1;
          // DCHECK(this->partition_id < (1 << 30));
          query.record_keys = simple_txn.keys;
          is_transmit_request = simple_txn.is_transmit_request;
        }

  NewOrder(std::size_t coordinator_id, std::size_t partition_id,  
           std::atomic<uint32_t> &worker_status, 
           DatabaseType &db, const ContextType &context,
           RandomType &random, Partitioner &partitioner,
           Storage &storage, simpleTransaction& simple_txn)
      : Transaction(coordinator_id, partition_id, partitioner, 0), 
        worker_status_(worker_status), db(db),
        context(context), random(random), storage(&storage),
        query(makeNewOrderQuery()(simple_txn.keys)) {
          this->partition_id = query.W_ID - 1;
          DCHECK(this->partition_id < (1 << 30));
          this->is_transmit_request = simple_txn.is_transmit_request;
        }


  NewOrder(std::size_t coordinator_id, std::size_t partition_id, 
                  std::atomic<uint32_t> &worker_status,
                  DatabaseType &db, const ContextType &context,
                  RandomType &random, Partitioner &partitioner,
                  Storage &storage, 
                  Transaction& txn)
      : Transaction(coordinator_id, partition_id, partitioner, 0), 
        worker_status_(worker_status), db(db),
        context(context), random(random), storage(&storage),
        partition_id(partition_id), 
        query(makeNewOrderQuery()(txn.get_query())) {
          /**
           * @brief convert from the generated txns
           * 
           */
          this->partition_id = query.W_ID - 1;
          // this->is_transmit_request = txn.is_transmit_request;
          DCHECK(this->partition_id < (1 << 30));
          this->on_replica_id = txn.on_replica_id;
          // LOG(INFO) << "reset ! " << txn.on_replica_id;
        }

  bool is_transmit_requests() override {
    return is_transmit_request;
  }

  TransactionResult execute(std::size_t worker_id) override {
    std::size_t granules_per_partition = this->context.granules_per_partition;

    // ScopedTimer t_local_work([&, this](uint64_t us) {
    //   this->record_local_work_time(us);
    // });
    int32_t W_ID = this->partition_id + 1;

    // The input data (see Clause 2.4.3.2) are communicated to the SUT.

    int32_t D_ID = query.D_ID;
    int32_t C_ID = query.C_ID;

    // The row in the WAREHOUSE table with matching W_ID is selected and W_TAX,
    // the warehouse tax rate, is retrieved.

    auto warehouseTableID = warehouse::tableID;
    storage->warehouse_key = warehouse::key(W_ID);
    this->search_for_read(warehouseTableID, W_ID - 1, storage->warehouse_key,
                          storage->warehouse_value, wid_to_granule_id(W_ID, context));

    // The row in the DISTRICT table with matching D_W_ID and D_ ID is selected,
    // D_TAX, the district tax rate, is retrieved, and D_NEXT_O_ID, the next
    // available order number for the district, is retrieved and incremented by
    // one.

    auto districtTableID = district::tableID;
    storage->district_key = district::key(W_ID, D_ID);
    this->search_for_update(districtTableID, W_ID - 1, storage->district_key,
                            storage->district_value, did_to_granule_id(D_ID, context));

    // The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is
    // selected and C_DISCOUNT, the customer's discount rate, C_LAST, the
    // customer's last name, and C_CREDIT, the customer's credit status, are
    // retrieved.

    auto customerTableID = customer::tableID;
    storage->customer_key = customer::key(W_ID, D_ID, C_ID);
    this->search_for_read(customerTableID, W_ID - 1, storage->customer_key,
                          storage->customer_value, did_to_granule_id(D_ID, context));

    auto itemTableID = item::tableID;
    auto stockTableID = stock::tableID;

    for (int i = 0; i < query.O_OL_CNT; i++) {

      // The row in the ITEM table with matching I_ID (equals OL_I_ID) is
      // selected and I_PRICE, the price of the item, I_NAME, the name of the
      // item, and I_DATA are retrieved. If I_ID has an unused value (see
      // Clause 2.4.1.5), a "not-found" condition is signaled, resulting in a
      // rollback of the database transaction (see Clause 2.4.2.3).

      int32_t OL_I_ID = query.INFO[i].OL_I_ID;
      int8_t OL_QUANTITY = query.INFO[i].OL_QUANTITY;
      int32_t OL_SUPPLY_W_ID = query.INFO[i].OL_SUPPLY_W_ID;

      storage->item_keys[i] = item::key(OL_I_ID);

      // If I_ID has an unused value, rollback.
      // In OCC, rollback can return without going through commit protocal

      if (storage->item_keys[i].I_ID == 0) {
        // abort();
        return TransactionResult::ABORT_NORETRY;
      }

      // this->search_local_index(itemTableID, 0, storage->item_keys[i],
      //                          storage->item_values[i], true);

      // The row in the STOCK table with matching S_I_ID (equals OL_I_ID) and
      // S_W_ID (equals OL_SUPPLY_W_ID) is selected.

      storage->stock_keys[i] = stock::key(OL_SUPPLY_W_ID, OL_I_ID);

      this->search_for_update(stockTableID, OL_SUPPLY_W_ID - 1,
                              storage->stock_keys[i], storage->stock_values[i], 
                              id_to_granule_id(OL_I_ID, context));
    }
    
    this->update(districtTableID, W_ID - 1, storage->district_key,
              storage->district_value, did_to_granule_id(D_ID, context));
    for (int i = 0; i < query.O_OL_CNT; i++) {

      int32_t OL_I_ID = query.INFO[i].OL_I_ID;
      int8_t OL_QUANTITY = query.INFO[i].OL_QUANTITY;
      int32_t OL_SUPPLY_W_ID = query.INFO[i].OL_SUPPLY_W_ID;

      this->update(stockTableID, OL_SUPPLY_W_ID - 1, storage->stock_keys[i],
                   storage->stock_values[i], id_to_granule_id(OL_I_ID, context));
    }

    // t_local_work.end();
    if (this->process_requests(worker_id)) {
      return TransactionResult::ABORT;
    }

    // t_local_work.reset();
    float W_TAX = storage->warehouse_value.W_YTD;

    float D_TAX = storage->district_value.D_TAX;
    int32_t D_NEXT_O_ID = storage->district_value.D_NEXT_O_ID;

    storage->district_value.D_NEXT_O_ID += 1;

    // this->update(districtTableID, W_ID - 1, storage->district_key,
    //              storage->district_value, D_ID % granules_per_partition);

    if (context.operation_replication) {
      Encoder encoder(this->operation.data);
      this->operation.partition_id = this->partition_id;
      encoder << true << storage->district_key.D_W_ID
              << storage->district_key.D_ID
              << storage->district_value.D_NEXT_O_ID;
    }

    float C_DISCOUNT = storage->customer_value.C_DISCOUNT;

    // A new row is inserted into both the NEW-ORDER table and the ORDER table
    // to reflect the creation of the new order. O_CARRIER_ID is set to a null
    // value. If the order includes only home order-lines, then O_ALL_LOCAL is
    // set to 1, otherwise O_ALL_LOCAL is set to 0.

    storage->new_order_key = new_order::key(W_ID, D_ID, D_NEXT_O_ID);

    storage->order_key = order::key(W_ID, D_ID, D_NEXT_O_ID);

    storage->order_value.O_ENTRY_D = Time::now();
    storage->order_value.O_CARRIER_ID = 0;
    storage->order_value.O_OL_CNT = query.O_OL_CNT;
    storage->order_value.O_C_ID = query.C_ID;
    storage->order_value.O_ALL_LOCAL = !query.isRemote();

    float total_amount = 0;

    auto orderLineTableID = stock::tableID;

    for (int i = 0; i < query.O_OL_CNT; i++) {

      int32_t OL_I_ID = query.INFO[i].OL_I_ID;
      int8_t OL_QUANTITY = query.INFO[i].OL_QUANTITY;
      int32_t OL_SUPPLY_W_ID = query.INFO[i].OL_SUPPLY_W_ID;

      float I_PRICE = storage->item_values[i].I_PRICE;

      // S_QUANTITY, the quantity in stock, S_DIST_xx, where xx represents the
      // district number, and S_DATA are retrieved. If the retrieved value for
      // S_QUANTITY exceeds OL_QUANTITY by 10 or more, then S_QUANTITY is
      // decreased by OL_QUANTITY; otherwise S_QUANTITY is updated to
      // (S_QUANTITY - OL_QUANTITY)+91. S_YTD is increased by OL_QUANTITY and
      // S_ORDER_CNT is incremented by 1. If the order-line is remote, then
      // S_REMOTE_CNT is incremented by 1.

      if (storage->stock_values[i].S_QUANTITY >= OL_QUANTITY + 10) {
        storage->stock_values[i].S_QUANTITY -= OL_QUANTITY;
      } else {
        storage->stock_values[i].S_QUANTITY =
            storage->stock_values[i].S_QUANTITY - OL_QUANTITY + 91;
      }

      storage->stock_values[i].S_YTD += OL_QUANTITY;
      storage->stock_values[i].S_ORDER_CNT++;

      if (OL_SUPPLY_W_ID != W_ID) {
        storage->stock_values[i].S_REMOTE_CNT++;
      }

      // this->update(stockTableID, OL_SUPPLY_W_ID - 1, storage->stock_keys[i],
      //              storage->stock_values[i], OL_I_ID % granules_per_partition);

      if (context.operation_replication) {
        Encoder encoder(this->operation.data);
        encoder << storage->stock_keys[i].S_W_ID << storage->stock_keys[i].S_I_ID
                << storage->stock_values[i].S_QUANTITY
                << storage->stock_values[i].S_YTD
                << storage->stock_values[i].S_ORDER_CNT
                << storage->stock_values[i].S_REMOTE_CNT;
      }

      if (this->execution_phase) {
        float OL_AMOUNT = I_PRICE * OL_QUANTITY;
        storage->order_line_keys[i] =
            order_line::key(W_ID, D_ID, D_NEXT_O_ID, i + 1);

        storage->order_line_values[i].OL_I_ID = OL_I_ID;
        storage->order_line_values[i].OL_SUPPLY_W_ID = OL_SUPPLY_W_ID;
        storage->order_line_values[i].OL_DELIVERY_D = 0;
        storage->order_line_values[i].OL_QUANTITY = OL_QUANTITY;
        storage->order_line_values[i].OL_AMOUNT = OL_AMOUNT;

        switch (D_ID) {
        case 1:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_01;
          break;
        case 2:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_02;
          break;
        case 3:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_03;
          break;
        case 4:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_04;
          break;
        case 5:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_05;
          break;
        case 6:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_06;
          break;
        case 7:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_07;
          break;
        case 8:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_08;
          break;
        case 9:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_09;
          break;
        case 10:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_10;
          break;
        default:
          DCHECK(false);
          break;
        }
        total_amount += OL_AMOUNT * (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX);
      }
    }

    return TransactionResult::READY_TO_COMMIT;
  }

  std::vector<size_t> debug_record_keys() override {
    return query.record_keys;
  }

  std::vector<size_t> debug_record_keys_master() override {
    std::vector<size_t> record_keys;

    for(int i = 0 ; i < query.record_keys.size(); i ++ ){
      Record rec;
      rec.set_real_key(query.record_keys[i]);;
      int w_c_id, d_c_id, c_c_id, s_c_id;

      switch (rec.table_id)
      {
      case tpcc::warehouse::tableID:
          w_c_id = db.get_dynamic_coordinator_id(context.coordinator_num, 
                                                      tpcc::warehouse::tableID, 
                                                      (void*)& rec.key.w_key);

          record_keys.push_back(w_c_id);
          break;
      case tpcc::district::tableID:
          d_c_id = db.get_dynamic_coordinator_id(context.coordinator_num, 
                                                      tpcc::district::tableID, 
                                                      (void*)& rec.key.d_key);
          record_keys.push_back(d_c_id);
          break;
      case tpcc::customer::tableID:
          c_c_id = db.get_dynamic_coordinator_id(context.coordinator_num, 
                                                      tpcc::customer::tableID, 
                                                      (void*)& rec.key.c_key);
          record_keys.push_back(c_c_id);
          break;
      case tpcc::stock::tableID:
          s_c_id = db.get_dynamic_coordinator_id(context.coordinator_num, 
                                                      tpcc::stock::tableID, 
                                                      (void*)& rec.key.s_key);
          record_keys.push_back(s_c_id);
          break;
      default:
          DCHECK(false);
          break;
      }
    }
    return record_keys;
  }

  TransactionResult transmit_execute(std::size_t worker_id) override {
    storage->key_value.reserve(query.record_keys.size());
    for(int i = 0 ; i < query.record_keys.size(); i ++ ){
      storage->key_value[i].set_real_key(query.record_keys[i]);;

      switch (storage->key_value[i].table_id)
      {
      case tpcc::warehouse::tableID:
          this->search_for_update(storage->key_value[i].table_id, 
                                  storage->key_value[i].key.w_key.W_ID - 1, 
                                  storage->key_value[i].key.w_key, 
                                  storage->key_value[i].value.w_val);
          break;
      case tpcc::district::tableID:
          this->search_for_update(storage->key_value[i].table_id, 
                                  storage->key_value[i].key.d_key.D_W_ID - 1, 
                                  storage->key_value[i].key.d_key, 
                                  storage->key_value[i].value.d_val);
          break;
      case tpcc::customer::tableID:
          this->search_for_update(storage->key_value[i].table_id, 
                                  storage->key_value[i].key.c_key.C_W_ID - 1, 
                                  storage->key_value[i].key.c_key, 
                                  storage->key_value[i].value.c_val);
          break;
      case tpcc::stock::tableID:
          this->search_for_update(storage->key_value[i].table_id, 
                                  storage->key_value[i].key.s_key.S_W_ID - 1, 
                                  storage->key_value[i].key.s_key, 
                                  storage->key_value[i].value.s_val);
          break;
      default:
          DCHECK(false);
          break;
      }
    }
    // LOG(INFO) << "pendingResponses : " << this->pendingResponses;
    if (this->process_migrate_requests(worker_id)) {
      return TransactionResult::ABORT;
    }
    return TransactionResult::TRANSMIT_REQUEST;
  }


  ExecutorStatus get_worker_status() override {
    return static_cast<ExecutorStatus>(worker_status_.load());
  }
  
  TransactionResult prepare_read_execute(std::size_t worker_id) override {
    
    int32_t W_ID = query.W_ID;

    // The input data (see Clause 2.4.3.2) are communicated to the SUT.

    int32_t D_ID = query.D_ID;
    int32_t C_ID = query.C_ID;

    // The row in the WAREHOUSE table with matching W_ID is selected and W_TAX,
    // the warehouse tax rate, is retrieved.

    auto warehouseTableID = warehouse::tableID;
    storage->warehouse_key = warehouse::key(W_ID);

    auto key_partition_id = this->partition_id; // db.getPartitionID(context, warehouseTableID, storage->warehouse_key);
    // if(key_partition_id == context.partition_num){
    //   return TransactionResult::ABORT_NORETRY;
    // }
    DCHECK(key_partition_id < (1 << 30));
    this->search_for_read(warehouseTableID, key_partition_id, storage->warehouse_key,
                          storage->warehouse_value, wid_to_granule_id(W_ID, context));

    // The row in the DISTRICT table with matching D_W_ID and D_ ID is selected,
    // D_TAX, the district tax rate, is retrieved, and D_NEXT_O_ID, the next
    // available order number for the district, is retrieved and incremented by
    // one.

    auto districtTableID = district::tableID;
    storage->district_key = district::key(W_ID, D_ID);
    key_partition_id = this->partition_id; // db.getPartitionID(context, districtTableID, storage->district_key);
    // if(key_partition_id == context.partition_num){
    //   return TransactionResult::ABORT_NORETRY;
    // }
    this->search_for_update(districtTableID, key_partition_id, storage->district_key,
                            storage->district_value, did_to_granule_id(D_ID, context));

    // The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is
    // selected and C_DISCOUNT, the customer's discount rate, C_LAST, the
    // customer's last name, and C_CREDIT, the customer's credit status, are
    // retrieved.

    auto customerTableID = customer::tableID;
    storage->customer_key = customer::key(W_ID, D_ID, C_ID);
    key_partition_id = this->partition_id; // db.getPartitionID(context, customerTableID, storage->customer_key);
    // if(key_partition_id == context.partition_num){
    //   return TransactionResult::ABORT_NORETRY;
    // }
    this->search_for_read(customerTableID, key_partition_id, storage->customer_key,
                          storage->customer_value, did_to_granule_id(D_ID, context));

    auto itemTableID = item::tableID;
    auto stockTableID = stock::tableID;

    for (int i = 0; i < query.O_OL_CNT; i++) {

      // The row in the ITEM table with matching I_ID (equals OL_I_ID) is
      // selected and I_PRICE, the price of the item, I_NAME, the name of the
      // item, and I_DATA are retrieved. If I_ID has an unused value (see
      // Clause 2.4.1.5), a "not-found" condition is signaled, resulting in a
      // rollback of the database transaction (see Clause 2.4.2.3).

      int32_t OL_I_ID = query.INFO[i].OL_I_ID;
      int8_t OL_QUANTITY = query.INFO[i].OL_QUANTITY;
      int32_t OL_SUPPLY_W_ID = query.INFO[i].OL_SUPPLY_W_ID;

      storage->item_keys[i] = item::key(OL_I_ID);

      // If I_ID has an unused value, rollback.
      // In OCC, rollback can return without going through commit protocal

      if (storage->item_keys[i].I_ID == 0) {
        // abort();
        return TransactionResult::ABORT_NORETRY;
      }

      // this->search_local_index(itemTableID, 0, storage->item_keys[i],
      //                          storage->item_values[i]);

      // The row in the STOCK table with matching S_I_ID (equals OL_I_ID) and
      // S_W_ID (equals OL_SUPPLY_W_ID) is selected.

      storage->stock_keys[i] = stock::key(OL_SUPPLY_W_ID, OL_I_ID);

      key_partition_id = OL_SUPPLY_W_ID - 1;//this->partition_id; // db.getPartitionID(context, stockTableID, storage->stock_keys[i]);
      // if(key_partition_id == context.partition_num){
      //   return TransactionResult::ABORT_NORETRY;
      // }
      this->search_for_update(stockTableID, key_partition_id,
                              storage->stock_keys[i], storage->stock_values[i],
                              id_to_granule_id(OL_I_ID, context));
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
      if (this->process_migrate_requests(worker_id)) {
        ret = TransactionResult::NOT_LOCAL_NORETRY;
      }
      break;
    case ReadMethods::REMOTE_READ_WITH_TRANSFER:
      if (this->process_requests(worker_id)) {
        ret = TransactionResult::ABORT;
      }
      break;
    case ReadMethods::REMASTER_ONLY:
      if (this->process_remaster_requests(worker_id)) {
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

    int32_t W_ID = query.W_ID;
    int32_t D_ID = query.D_ID;
    int32_t C_ID = query.C_ID;

    auto warehouseTableID = warehouse::tableID;
    auto districtTableID = district::tableID;
    auto customerTableID = customer::tableID;
    auto itemTableID = item::tableID;
    auto stockTableID = stock::tableID;
    float W_TAX = storage->warehouse_value.W_YTD;

    float D_TAX = storage->district_value.D_TAX;
    int32_t D_NEXT_O_ID = storage->district_value.D_NEXT_O_ID;

    storage->district_value.D_NEXT_O_ID += 1;

    auto key_partition_id = this->partition_id;// db.getPartitionID(context, districtTableID, storage->district_key);
    // if(key_partition_id == context.partition_num){
    //   return TransactionResult::ABORT_NORETRY;
    // }
    this->update(districtTableID, key_partition_id, storage->district_key,
                 storage->district_value, did_to_granule_id(D_ID, context));

    if (context.operation_replication) {
      Encoder encoder(this->operation.data);
      this->operation.partition_id = this->partition_id;
      encoder << true << storage->district_key.D_W_ID
              << storage->district_key.D_ID
              << storage->district_value.D_NEXT_O_ID;
    }

    float C_DISCOUNT = storage->customer_value.C_DISCOUNT;

    // A new row is inserted into both the NEW-ORDER table and the ORDER table
    // to reflect the creation of the new order. O_CARRIER_ID is set to a null
    // value. If the order includes only home order-lines, then O_ALL_LOCAL is
    // set to 1, otherwise O_ALL_LOCAL is set to 0.

    storage->new_order_key = new_order::key(W_ID, D_ID, D_NEXT_O_ID);

    storage->order_key = order::key(W_ID, D_ID, D_NEXT_O_ID);

    storage->order_value.O_ENTRY_D = Time::now();
    storage->order_value.O_CARRIER_ID = 0;
    storage->order_value.O_OL_CNT = query.O_OL_CNT;
    storage->order_value.O_C_ID = query.C_ID;
    storage->order_value.O_ALL_LOCAL = !query.isRemote();

    float total_amount = 0;

    auto orderLineTableID = stock::tableID;

    for (int i = 0; i < query.O_OL_CNT; i++) {

      int32_t OL_I_ID = query.INFO[i].OL_I_ID;
      int8_t OL_QUANTITY = query.INFO[i].OL_QUANTITY;
      int32_t OL_SUPPLY_W_ID = query.INFO[i].OL_SUPPLY_W_ID;

      float I_PRICE = storage->item_values[i].I_PRICE;

      // S_QUANTITY, the quantity in stock, S_DIST_xx, where xx represents the
      // district number, and S_DATA are retrieved. If the retrieved value for
      // S_QUANTITY exceeds OL_QUANTITY by 10 or more, then S_QUANTITY is
      // decreased by OL_QUANTITY; otherwise S_QUANTITY is updated to
      // (S_QUANTITY - OL_QUANTITY)+91. S_YTD is increased by OL_QUANTITY and
      // S_ORDER_CNT is incremented by 1. If the order-line is remote, then
      // S_REMOTE_CNT is incremented by 1.

      if (storage->stock_values[i].S_QUANTITY >= OL_QUANTITY + 10) {
        storage->stock_values[i].S_QUANTITY -= OL_QUANTITY;
      } else {
        storage->stock_values[i].S_QUANTITY =
            storage->stock_values[i].S_QUANTITY - OL_QUANTITY + 91;
      }

      storage->stock_values[i].S_YTD += OL_QUANTITY;
      storage->stock_values[i].S_ORDER_CNT++;

      if (OL_SUPPLY_W_ID != W_ID) {
        storage->stock_values[i].S_REMOTE_CNT++;
      }

      key_partition_id = OL_SUPPLY_W_ID - 1; // db.getPartitionID(context, stockTableID, storage->stock_keys[i]);
      // if(key_partition_id == context.partition_num){
      //   return TransactionResult::ABORT_NORETRY;
      // }
      this->update(stockTableID, key_partition_id, storage->stock_keys[i],
                   storage->stock_values[i], id_to_granule_id(OL_I_ID, context));

      if (context.operation_replication) {
        Encoder encoder(this->operation.data);
        encoder << storage->stock_keys[i].S_W_ID << storage->stock_keys[i].S_I_ID
                << storage->stock_values[i].S_QUANTITY
                << storage->stock_values[i].S_YTD
                << storage->stock_values[i].S_ORDER_CNT
                << storage->stock_values[i].S_REMOTE_CNT;
      }

      if (this->execution_phase) {
        float OL_AMOUNT = I_PRICE * OL_QUANTITY;
        storage->order_line_keys[i] =
            order_line::key(W_ID, D_ID, D_NEXT_O_ID, i + 1);

        storage->order_line_values[i].OL_I_ID = OL_I_ID;
        storage->order_line_values[i].OL_SUPPLY_W_ID = OL_SUPPLY_W_ID;
        storage->order_line_values[i].OL_DELIVERY_D = 0;
        storage->order_line_values[i].OL_QUANTITY = OL_QUANTITY;
        storage->order_line_values[i].OL_AMOUNT = OL_AMOUNT;

        switch (D_ID) {
        case 1:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_01;
          break;
        case 2:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_02;
          break;
        case 3:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_03;
          break;
        case 4:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_04;
          break;
        case 5:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_05;
          break;
        case 6:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_06;
          break;
        case 7:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_07;
          break;
        case 8:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_08;
          break;
        case 9:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_09;
          break;
        case 10:
          storage->order_line_values[i].OL_DIST_INFO =
              storage->stock_values[i].S_DIST_10;
          break;
        default:
          DCHECK(false);
          break;
        }
        total_amount += OL_AMOUNT * (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX);
      }
    }


    return TransactionResult::READY_TO_COMMIT;
  };

  

  void reset_query() override {
    query = makeNewOrderQuery()(context, partition_id, random);
  }
  const std::vector<bool> get_query_update() override {
    std::vector<bool> ret;
    for(int8_t i = 0; i < query.O_OL_CNT; i ++ ){
      ret.push_back(true);
    }
    return ret; 
  };

  std::string print_raw_query_str() override{
    return std::to_string(query.W_ID) + " " + std::to_string(query.D_ID) + " " + std::to_string(query.C_ID);
  }

  const std::vector<u_int64_t> get_query() override{
    /**
     * @brief for generate the COUNT message for Recorder!
     * @add by truth 22-02-24
     */
    using T = u_int64_t;
    std::vector<T> record_keys;
    // 4bit    | 6bit | 10bit | 15bit | 20bit
    // tableID | W_id | D_id  | C_id  | Lo_id

    // record_keys
    // record_keys[0-2]: w_record_key, d_record_key, c_record_key
    // record_keys[3-13]: ol_supply_w_record_keys
    //

    T W_ID = this->partition_id + 1; 
    T D_ID = query.D_ID;
    T C_ID = query.C_ID;
     
    // 
    T w_record_key = (static_cast<T>(warehouse::tableID) << RECORD_COUNT_TABLE_ID_OFFSET) + 
                                                   (W_ID << RECORD_COUNT_W_ID_OFFSET); 

    T d_record_key = (static_cast<T>(district::tableID)  << RECORD_COUNT_TABLE_ID_OFFSET) + 
                                                   (W_ID << RECORD_COUNT_W_ID_OFFSET) + 
                                                   (D_ID << RECORD_COUNT_D_ID_OFFSET);

    T c_record_key = (static_cast<T>(customer::tableID)  << RECORD_COUNT_TABLE_ID_OFFSET) + 
                                                   (W_ID << RECORD_COUNT_W_ID_OFFSET) + 
                                                   (D_ID << RECORD_COUNT_D_ID_OFFSET) + 
                                                   (C_ID << RECORD_COUNT_C_ID_OFFSET);

    int32_t w_id = (c_record_key & RECORD_COUNT_W_ID_VALID) >>RECORD_COUNT_W_ID_OFFSET;
    int32_t d_id = (c_record_key & RECORD_COUNT_D_ID_VALID) >>RECORD_COUNT_D_ID_OFFSET;
    int32_t c_id = (c_record_key & RECORD_COUNT_C_ID_VALID) >>RECORD_COUNT_C_ID_OFFSET;
    DCHECK(D_ID >= 1 && C_ID >= 1);
    record_keys.push_back(w_record_key);
    record_keys.push_back(d_record_key);
    record_keys.push_back(c_record_key);
    
    auto itemTableID = item::tableID;
    auto stockTableID = stock::tableID; 

    std::set<T> ol_supply_w_record_keys; // , ol_i_record_keys;

    for (int i = 0; i < query.O_OL_CNT; i++) {
      T OL_I_ID = query.INFO[i].OL_I_ID;
      T OL_SUPPLY_W_ID = query.INFO[i].OL_SUPPLY_W_ID;
      // stock_keys.push_back(stock::key(OL_SUPPLY_W_ID, OL_I_ID));
      T ol_supply_w_record_key = 
      (static_cast<T>(stock::tableID) << RECORD_COUNT_TABLE_ID_OFFSET) + 
                     (OL_SUPPLY_W_ID << RECORD_COUNT_W_ID_OFFSET) +
                            (OL_I_ID);

      ol_supply_w_record_keys.insert(ol_supply_w_record_key);
    }
    for(auto i : ol_supply_w_record_keys){
      record_keys.push_back(i);
    }
    return record_keys;
  }

  const std::vector<u_int64_t> get_query_master() override{
    /**
     * @brief for generate the COUNT message for Recorder!
     * @add by truth 22-02-24
     */
    using T = u_int64_t;
    std::vector<T> record_keys;
    // 4bit    | 6bit | 10bit | 15bit | 20bit
    // tableID | W_id | D_id  | C_id  | Lo_id

    // record_keys
    // record_keys[0-2]: w_record_key, d_record_key, c_record_key
    // record_keys[3-13]: ol_supply_w_record_keys
    //

    T W_ID = this->partition_id + 1; 
    T D_ID = query.D_ID;
    T C_ID = query.C_ID;

    tpcc::warehouse::key w_id = tpcc::warehouse::key(W_ID);
    tpcc::district::key  d_id = tpcc::district::key(W_ID, D_ID);
    tpcc::customer::key  c_id = tpcc::customer::key(W_ID, D_ID, C_ID);

    auto w_c_id = db.get_dynamic_coordinator_id(context.coordinator_num, 
                                                tpcc::warehouse::tableID, 
                                                (void*)& w_id);
    auto d_c_id = db.get_dynamic_coordinator_id(context.coordinator_num, 
                                                tpcc::district::tableID, 
                                                (void*)& d_id);
    auto c_c_id = db.get_dynamic_coordinator_id(context.coordinator_num, 
                                                tpcc::customer::tableID, 
                                                (void*)& c_id);

    record_keys.push_back(w_c_id);
    record_keys.push_back(d_c_id);
    record_keys.push_back(c_c_id);
    
    for (int i = 0; i < query.O_OL_CNT; i++) {
      T OL_I_ID = query.INFO[i].OL_I_ID;
      T OL_SUPPLY_W_ID = query.INFO[i].OL_SUPPLY_W_ID;
      tpcc::stock::key s_id = tpcc::stock::key(OL_SUPPLY_W_ID, OL_I_ID);

      auto s_c_id = db.get_dynamic_coordinator_id(context.coordinator_num, 
                                                  tpcc::stock::tableID, 
                                                  (void*)& s_id);
      record_keys.push_back(s_c_id);
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

  std::set<int> txn_nodes_involved(bool is_dynamic) override {
    std::set<int> from_nodes_id;

    int32_t W_ID = query.W_ID;
    int32_t D_ID = query.D_ID;
    int32_t C_ID = query.C_ID;
    auto itemTableID = item::tableID;
    auto stockTableID = stock::tableID;

    auto warehouse_key = warehouse::key(W_ID);
    auto district_key = district::key(W_ID, D_ID);
    auto customer_key = customer::key(W_ID, D_ID, C_ID);
    std::vector<item::key> item_keys;
    std::vector<stock::key> stock_keys;

    bool is_cross_txn = false; // ret

    get_item_stock_keys_query(item_keys, stock_keys);
    if(is_dynamic){
      size_t ware_coordinator_id = db.get_dynamic_coordinator_id(context.coordinator_num, warehouse::tableID, (void*)& warehouse_key);
      size_t dist_coordinator_id = db.get_dynamic_coordinator_id(context.coordinator_num, district::tableID, (void*)& district_key);
      size_t cust_coordinator_id = db.get_dynamic_coordinator_id(context.coordinator_num, customer::tableID, (void*)& customer_key);
      
      DCHECK(ware_coordinator_id == dist_coordinator_id &&
             dist_coordinator_id == cust_coordinator_id);

      from_nodes_id.insert(ware_coordinator_id);
      from_nodes_id.insert(dist_coordinator_id);
      from_nodes_id.insert(cust_coordinator_id);

      for(size_t i = 0 ; i < stock_keys.size(); i ++ ){
        size_t stock_coordinator_id = db.get_dynamic_coordinator_id(context.coordinator_num, stock::tableID, (void*)& stock_keys[i]);
        from_nodes_id.insert(stock_coordinator_id);
      }
    } else {
      for(size_t i = 0 ; i < stock_keys.size(); i ++ ){
        size_t stock_coordinator_id;
        stock_coordinator_id = (stock_keys[i].S_W_ID - 1) % context.coordinator_num;
        from_nodes_id.insert(stock_coordinator_id);
      }
    }
    return from_nodes_id;
  }

  std::unordered_map<int, int> txn_nodes_involved(int& max_node, bool is_dynamic) override {
    std::unordered_map<int, int> ret;
    DCHECK(0 == 1);
    return ret;
  }
   bool check_cross_node_txn(bool is_dynamic) override{
    /**
     * @brief must be master and local 判断是不是跨节点事务
     * @return true/false
     */
    std::set<int> from_nodes_id = std::move(txn_nodes_involved(is_dynamic));
    from_nodes_id.insert(context.coordinator_id);
    return from_nodes_id.size() > 1; 
  }
  std::size_t get_partition_id(){
    return partition_id;
  }

  virtual int32_t get_partition_count() override { return query.number_of_parts(); }

  virtual int32_t get_partition(int i) override { return query.get_part(i); }

  virtual int32_t get_partition_granule_count(int i) override { return query.get_part_granule_count(i); }

  virtual int32_t get_granule(int partition_id, int j) override { return query.get_part_granule(partition_id, j); }

  virtual bool is_single_partition() override { return query.number_of_parts() == 1; }


  virtual const std::string serialize(std::size_t ith_replica = 0) override {
    std::string res;
    uint32_t txn_type = 0;
    Encoder encoder(res);
    encoder << this->transaction_id << txn_type << this->straggler_wait_time << ith_replica << this->txn_random_seed_start << partition_id;
    Transaction::serialize_lock_status(encoder);
    return res;
  }
  
private:
  void get_item_stock_keys_query(std::vector<item::key>& item_keys, std::vector<stock::key>& stock_keys){
    for (int i = 0; i < query.O_OL_CNT; i++) {
      int32_t OL_I_ID = query.INFO[i].OL_I_ID;
      int8_t OL_QUANTITY = query.INFO[i].OL_QUANTITY;
      int32_t OL_SUPPLY_W_ID = query.INFO[i].OL_SUPPLY_W_ID;

      item_keys.push_back(item::key(OL_I_ID));
      stock_keys.push_back(stock::key(OL_SUPPLY_W_ID, OL_I_ID));
    }
    return;
  }

private:
  std::atomic<uint32_t> &worker_status_;
  DatabaseType &db;
  const ContextType &context;
  RandomType &random;
  Storage *storage;
  std::size_t partition_id;
  NewOrderQuery query;
  bool is_transmit_request;
  int on_replica_num; // only for hermes
  int is_real_distributed; // only for hermes
};

template <class Transaction> class Payment : public Transaction {
public:
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using StorageType = Storage;

  Payment(std::size_t coordinator_id, std::size_t partition_id,
          DatabaseType &db, const ContextType &context, RandomType &random,
          Partitioner &partitioner, Storage &storage)
      : Transaction(coordinator_id, partition_id, partitioner, 0), db(db),
        context(context), random(random), storage(&storage),
        partition_id(partition_id),
        query(makePaymentQuery()(context, partition_id + 1, random)) {}

  Payment(std::size_t coordinator_id, std::size_t partition_id,
          DatabaseType &db, const ContextType &context, RandomType &random,
          Partitioner &partitioner, std::size_t ith_replica)
      : Transaction(coordinator_id, partition_id, partitioner, ith_replica), db(db),
        context(context), random(random),
        partition_id(partition_id),
        query(makePaymentQuery()(context, partition_id + 1, random)) {
          storage = get_storage();
        }
  virtual ~Payment() {
    put_storage(storage); storage = nullptr; 
  }

  bool is_transmit_requests() override {
    return is_transmit_request;
  }
  TransactionResult execute(std::size_t worker_id) override {

    int32_t W_ID = query.W_ID;

    // The input data (see Clause 2.5.3.2) are communicated to the SUT.

    int32_t D_ID = query.D_ID;
    int32_t C_ID = query.C_ID;
    int32_t C_D_ID = query.C_D_ID;
    int32_t C_W_ID = query.C_W_ID;
    float H_AMOUNT = query.H_AMOUNT;

    // The row in the WAREHOUSE table with matching W_ID is selected.
    // W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, and W_ZIP are retrieved
    // and W_YTD,

    auto warehouseTableID = warehouse::tableID;
    storage->warehouse_key = warehouse::key(W_ID);
    this->search_for_update(warehouseTableID, W_ID - 1, storage->warehouse_key,
                            storage->warehouse_value);

    // The row in the DISTRICT table with matching D_W_ID and D_ID is selected.
    // D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, and D_ZIP are retrieved
    // and D_YTD,

    auto districtTableID = district::tableID;
    storage->district_key = district::key(W_ID, D_ID);
    this->search_for_update(districtTableID, W_ID - 1, storage->district_key,
                            storage->district_value);

    // The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is
    // selected and C_DISCOUNT, the customer's discount rate, C_LAST, the
    // customer's last name, and C_CREDIT, the customer's credit status, are
    // retrieved.

    auto customerNameIdxTableID = customer_name_idx::tableID;

    if (C_ID == 0) {
      storage->customer_name_idx_key =
          customer_name_idx::key(C_W_ID, C_D_ID, query.C_LAST);
      // this->search_local_index(customerNameIdxTableID, C_W_ID - 1,
      //                          storage->customer_name_idx_key,
      //                          storage->customer_name_idx_value);

      this->process_requests(worker_id);
      C_ID = storage->customer_name_idx_value.C_ID;
    }

    auto customerTableID = customer::tableID;
    storage->customer_key = customer::key(C_W_ID, C_D_ID, C_ID);
    this->search_for_update(customerTableID, C_W_ID - 1, storage->customer_key,
                            storage->customer_value);

    if (this->process_requests(worker_id)) {
      return TransactionResult::ABORT;
    }
    if(is_transmit_request == true){
      return TransactionResult::TRANSMIT_REQUEST;
    }

    // the warehouse's year-to-date balance, is increased by H_ AMOUNT.
    storage->warehouse_value.W_YTD += H_AMOUNT;
    this->update(warehouseTableID, W_ID - 1, storage->warehouse_key,
                 storage->warehouse_value);

    if (context.operation_replication) {
      this->operation.partition_id = this->partition_id;
      Encoder encoder(this->operation.data);
      encoder << false << storage->warehouse_key.W_ID
              << storage->warehouse_value.W_YTD;
    }

    // the district's year-to-date balance, is increased by H_AMOUNT.
    storage->district_value.D_YTD += H_AMOUNT;
    this->update(districtTableID, W_ID - 1, storage->district_key,
                 storage->district_value);

    if (context.operation_replication) {
      Encoder encoder(this->operation.data);
      encoder << storage->district_key.D_W_ID << storage->district_key.D_ID
              << storage->district_value.D_YTD;
    }

    char C_DATA[501];
    int total_written = 0;
    if (this->execution_phase) {
      if (storage->customer_value.C_CREDIT == "BC") {
        int written;

        written = std::sprintf(C_DATA + total_written, "%d ", C_ID);
        total_written += written;

        written = std::sprintf(C_DATA + total_written, "%d ", C_D_ID);
        total_written += written;

        written = std::sprintf(C_DATA + total_written, "%d ", C_W_ID);
        total_written += written;

        written = std::sprintf(C_DATA + total_written, "%d ", D_ID);
        total_written += written;

        written = std::sprintf(C_DATA + total_written, "%d ", W_ID);
        total_written += written;

        written = std::sprintf(C_DATA + total_written, "%.2f ", H_AMOUNT);
        total_written += written;

        const char *old_C_DATA = storage->customer_value.C_DATA.c_str();

        std::memcpy(C_DATA + total_written, old_C_DATA, 500 - total_written);
        C_DATA[500] = 0;

        storage->customer_value.C_DATA.assign(C_DATA);
      }

      storage->customer_value.C_BALANCE -= H_AMOUNT;
      storage->customer_value.C_YTD_PAYMENT += H_AMOUNT;
      storage->customer_value.C_PAYMENT_CNT += 1;
    }

    this->update(customerTableID, C_W_ID - 1, storage->customer_key,
                 storage->customer_value);

    if (context.operation_replication) {
      Encoder encoder(this->operation.data);
      encoder << storage->customer_key.C_W_ID << storage->customer_key.C_D_ID
              << storage->customer_key.C_ID;
      encoder << uint32_t(total_written);
      encoder.write_n_bytes(C_DATA, total_written);
      encoder << storage->customer_value.C_BALANCE
              << storage->customer_value.C_YTD_PAYMENT
              << storage->customer_value.C_PAYMENT_CNT;
    }

    char H_DATA[25];
    int written;
    if (this->execution_phase) {
      written = std::sprintf(H_DATA, "%s    %s",
                             storage->warehouse_value.W_NAME.c_str(),
                             storage->district_value.D_NAME.c_str());
      H_DATA[written] = 0;

      storage->h_key =
          history::key(W_ID, D_ID, C_W_ID, C_D_ID, C_ID, Time::now());
      storage->h_value.H_AMOUNT = H_AMOUNT;
      storage->h_value.H_DATA.assign(H_DATA, written);
    }
    return TransactionResult::READY_TO_COMMIT;
  }
  ExecutorStatus get_worker_status() override {
    DCHECK(false);
    return static_cast<ExecutorStatus>(0);
  }
  TransactionResult prepare_read_execute(std::size_t worker_id) override {
    DCHECK(false);
    return TransactionResult::READY_TO_COMMIT;
  };

  std::vector<size_t> debug_record_keys() override {
    return query.record_keys;
  }
  std::vector<size_t> debug_record_keys_master() override {
    std::vector<size_t> query_master;
    DCHECK(false);
    return query_master;
  }
  TransactionResult transmit_execute(std::size_t worker_id) override {
    DCHECK(false);
    return TransactionResult::READY_TO_COMMIT;
  };


  TransactionResult read_execute(std::size_t worker_id, ReadMethods local_read_only) override {
    DCHECK(false);
    return TransactionResult::READY_TO_COMMIT;
  };
  TransactionResult prepare_update_execute(std::size_t worker_id) override {
    DCHECK(false);
    return TransactionResult::READY_TO_COMMIT;
  };

  // TransactionResult local_execute(std::size_t worker_id) override {
  //   // TODO
  //   DCHECK(false);
  //   return TransactionResult::NOT_LOCAL_NORETRY;
  // }
  void reset_query() override {
    query = makePaymentQuery()(context, partition_id, random);
  }
  std::string print_raw_query_str() override{
    DCHECK(false);
  }
  const std::vector<u_int64_t> get_query() override{
    using T = u_int64_t;

    std::vector<T> record_keys;
    /**TODO**/
    DCHECK(false);
    return record_keys;
  }

  const std::vector<u_int64_t> get_query_master() override{
    /**
     * @brief for generate the COUNT message for Recorder!
     * @add by truth 22-02-24
     */
    using T = u_int64_t;
    std::vector<T> record_keys;
    DCHECK(false);
    return record_keys;
  }
  const std::string get_query_printed() override {
    std::string print_ = "";
    for(auto i : get_query()){
      print_ += " " + std::to_string(i);
    }
    return print_;
  }
  
  const std::vector<bool> get_query_update() override {
    std::vector<bool> ret;
    for(size_t i = 0; i < 1; i ++ ){
      ret.push_back(true);
    }
    return ret;
  }

  std::set<int> txn_nodes_involved(bool is_dynamic) override {
    std::set<int> ret;
    return ret;
  }
  std::unordered_map<int, int> txn_nodes_involved(int& max_node, bool is_dynamic) override {
    std::unordered_map<int, int> ret;
    return ret;
  }
   bool check_cross_node_txn(bool is_dynamic) override{
    /**
     * @brief must be master and local 判断是不是跨节点事务
     * @return true/false
     */
    std::set<int> from_nodes_id = std::move(txn_nodes_involved(is_dynamic));
    from_nodes_id.insert(context.coordinator_id);
    return from_nodes_id.size() > 1; 
  }
  std::size_t get_partition_id(){
    return partition_id;
  }

  virtual int32_t get_partition_count() override { return 1; } // uery.number_of_parts()

  virtual int32_t get_partition(int i) override { return 1; } // query.get_part(i)
  
  virtual int32_t get_partition_granule_count(int i) override { return i; } //  query.get_part_granule_count(i);

  virtual int32_t get_granule(int partition_id, int j) override { return j; } // query.get_part_granule(partition_id, j); 
  
  virtual bool is_single_partition() override { return 1; } // query.number_of_parts() == 

  virtual const std::string serialize(std::size_t ith_replica = 0) override {
    std::string res;
    uint32_t txn_type = 1;
    Encoder encoder(res);
    encoder << this->transaction_id <<  txn_type << this->straggler_wait_time << ith_replica << this->txn_random_seed_start << partition_id;
    Transaction::serialize_lock_status(encoder);
    return res;
  }
private:
  DatabaseType &db;
  const ContextType &context;
  RandomType &random;
  Storage * storage;
  std::size_t partition_id;
  PaymentQuery query;
  bool is_transmit_request;
  int on_replica_num; // only for hermes
  int is_real_distributed; // only for hermes
};

} // namespace tpcc
} // namespace star
