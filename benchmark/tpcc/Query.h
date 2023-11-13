//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "benchmark/tpcc/Context.h"
#include "benchmark/tpcc/Random.h"
#include "common/FixedString.h"
#include <string>

namespace star {
namespace tpcc {

struct NewOrderQuery {
  bool isRemote() {
    for (auto i = 0; i < O_OL_CNT; i++) {
      if (INFO[i].OL_SUPPLY_W_ID != W_ID) {
        return true;
      }
    }
    return false;
  }

  void unpack_transaction(const simpleTransaction& t){
    auto record_key = t.keys[2];
    this->W_ID = (record_key & RECORD_COUNT_W_ID_VALID) >>  RECORD_COUNT_W_ID_OFFSET;
    this->D_ID = (record_key & RECORD_COUNT_D_ID_VALID) >>  RECORD_COUNT_D_ID_OFFSET;
    DCHECK(this->D_ID >= 1);
    this->C_ID = (record_key & RECORD_COUNT_C_ID_VALID) >>  RECORD_COUNT_C_ID_OFFSET;
    DCHECK(this->C_ID >= 1);

    for(size_t i = 0 ; i < t.keys.size(); i ++ ){
      auto record_key = t.keys[i];
      if(i >= 3){
        INFO[i - 3].OL_I_ID = (record_key & RECORD_COUNT_OL_ID_VALID);
        INFO[i - 3].OL_SUPPLY_W_ID = 
          (record_key & RECORD_COUNT_W_ID_VALID) >>  RECORD_COUNT_W_ID_OFFSET;
      }
    }
  }
  void print(){
    LOG(INFO) << this->W_ID << " " << this->D_ID << " " << this->C_ID;
  }
  std::string print_str(){
    return std::to_string(this->W_ID) + " " + 
           std::to_string(this->D_ID) + " " + 
           std::to_string(this->C_ID) ;
  }
  int32_t W_ID;
  int32_t D_ID;
  int32_t C_ID;
  int8_t O_OL_CNT;

  struct NewOrderQueryInfo {
    int32_t OL_I_ID;
    int32_t OL_SUPPLY_W_ID;
    int8_t OL_QUANTITY;
  };

  NewOrderQueryInfo INFO[15];
  std::vector<uint64_t> record_keys; // for migration
};

class makeNewOrderQuery {
public:
  NewOrderQuery operator()(const Context &context, 
                           int32_t W_ID,
                           double cur_timestamp,
                           Random &random) const {

    int workload_type_num = 3;
    int workload_type = ((int)cur_timestamp / context.workload_time % workload_type_num) + 1;// which_workload_(crossPartition, (int)cur_timestamp);


    NewOrderQuery query;
    // W_ID is constant over the whole measurement interval
    DCHECK(W_ID != 0);
    query.W_ID = W_ID;
    // The district number (D_ID) is randomly selected within [1 .. 10] from the
    // home warehouse (D_W_ID = W_ID).
    

    // The non-uniform random customer number (C_ID) is selected using the
    // NURand(1023,1,3000) function from the selected district number (C_D_ID =
    // D_ID) and the home warehouse number (C_W_ID = W_ID).
    double my_thresh = 0.1;
    int x = random.uniform_dist(1, 100);
    if (x <= context.newOrderCrossPartitionProbability &&
             context.partition_num > 1) {
        query.D_ID = random.uniform_dist(1, 5);
        query.C_ID = random.uniform_dist(1, 3000 * my_thresh);
    } else {
        // query.C_ID = random.uniform_dist(3000 * my_thresh + 1, 3000);
        query.D_ID = random.uniform_dist(6, 10);
        query.C_ID = 3000 * my_thresh + random.uniform_dist(1, 3000 * my_thresh);
    }
    DCHECK(query.C_ID >= 1);

    // The number of items in the order (ol_cnt) is randomly selected within [5
    // .. 15] (an average of 10).

    query.O_OL_CNT = 10; // random.uniform_dist(5, 10);

    int rbk = random.uniform_dist(1, 100);

    for (auto i = 0; i < query.O_OL_CNT; i++) {

      // A non-uniform random item number (OL_I_ID) is selected using the
      // NURand(8191,1,100000) function. If this is the last item on the order
      // and rbk = 1 (see Clause 2.4.1.4), then the item number is set to an
      // unused value.

      // Comment: An unused value for an item number is a value not found 
      // in the database such that its use will produce a "not-found" condition 
      // within the application program. This condition should result in 
      // rolling back the current database transaction.
      //

      bool retry;
      // do {
      //   retry = false;
      query.INFO[i].OL_I_ID = query.D_ID * 1000 +  
                             (query.C_ID - 1) * query.O_OL_CNT + i + 1;// (query.W_ID - 1) * 3000 + query.C_ID;
            // random.non_uniform_distribution(8191, 1, 100000);
        // figure out the GOODS_ID you need to buy
      //   for (int k = 0; k < i; k++) {
      //     if (query.INFO[k].OL_I_ID == query.INFO[i].OL_I_ID) {
      //       retry = true;
      //       break;
      //     }
      //   }
      // } while (retry);

      // if (i == query.O_OL_CNT - 1 && rbk == 1) {
      //   query.INFO[i].OL_I_ID = 0;
      // }

      // The first supplying warehouse number (OL_SUPPLY_W_ID) is selected as
      // the home warehouse 90% of the time and as a remote warehouse 10% of the
      // time.

      if (i == 0) {
        // figure out buy from which warehouse
        if (x <= context.newOrderCrossPartitionProbability &&
            context.partition_num > 1) {
          // is cross partition 
          query.INFO[i].OL_SUPPLY_W_ID = (W_ID + workload_type) % context.partition_num;
          //  
          if(query.INFO[i].OL_SUPPLY_W_ID == 0){
            query.INFO[i].OL_SUPPLY_W_ID = W_ID;
          }

        } else {
          query.INFO[i].OL_SUPPLY_W_ID = W_ID;
        }
      } else {
        query.INFO[i].OL_SUPPLY_W_ID = W_ID;
      }
      query.INFO[i].OL_QUANTITY = 5; // random.uniform_dist(1, 10);
    }

    return query;
  }

  NewOrderQuery operator()(const std::vector<size_t>& keys) const {
    NewOrderQuery query;

    size_t size_ = keys.size();

    DCHECK(size_ == 13);
    // 
    auto c_record_key = keys[2];

    int32_t w_id = (c_record_key & RECORD_COUNT_W_ID_VALID) >> RECORD_COUNT_W_ID_OFFSET;

    
    int32_t d_id = (c_record_key & RECORD_COUNT_D_ID_VALID) >> RECORD_COUNT_D_ID_OFFSET;
    int32_t c_id = (c_record_key & RECORD_COUNT_C_ID_VALID) >> RECORD_COUNT_C_ID_OFFSET;

    query.W_ID = w_id;
    query.D_ID = d_id;
    DCHECK(query.D_ID >= 1);

    query.C_ID = c_id;
    DCHECK(query.C_ID >= 1);
    query.O_OL_CNT = 10; // random.uniform_dist(5, 10);

    for (auto i = 0; i < query.O_OL_CNT; i++) {
      auto cur_key = keys[3 + i];

      int32_t w_id = (cur_key & RECORD_COUNT_W_ID_VALID) >> RECORD_COUNT_W_ID_OFFSET;
      int32_t s_id = (cur_key & RECORD_COUNT_OL_ID_VALID);

      query.INFO[i].OL_I_ID = s_id;// (query.C_ID - 1) * query.O_OL_CNT + i + 1;// (query.W_ID - 1) * 3000 + query.C_ID;
      query.INFO[i].OL_SUPPLY_W_ID = w_id;
      query.INFO[i].OL_QUANTITY = 5;// random.uniform_dist(1, 10);
    }
    query.record_keys = keys;
    return query;
  }


  NewOrderQuery operator()(const std::vector<size_t>& keys, bool is_transmit) const {
    NewOrderQuery query;
    query.record_keys = keys;
    return query;
  }
  
};

struct PaymentQuery {
  int32_t W_ID;
  int32_t D_ID;
  int32_t C_ID;
  FixedString<16> C_LAST;
  int32_t C_D_ID;
  int32_t C_W_ID;
  float H_AMOUNT;

  std::vector<uint64_t> record_keys; // for migration
};

class makePaymentQuery {
public:
  PaymentQuery operator()(const Context &context, int32_t W_ID,
                          Random &random) const {
    PaymentQuery query;

    // W_ID is constant over the whole measurement interval

    query.W_ID = W_ID;

    // The district number (D_ID) is randomly selected within [1 ..10] from the
    // home warehouse (D_W_ID) = W_ID).

    query.D_ID = random.uniform_dist(1, 10);

    // the customer resident warehouse is the home warehouse 85% of the time
    // and is a randomly selected remote warehouse 15% of the time.

    // If the system is configured for a single warehouse,
    // then all customers are selected from that single home warehouse.

    int x = random.uniform_dist(1, 100);

    if (x <= context.paymentCrossPartitionProbability &&
        context.partition_num > 1) {
      // If x <= 15 a customer is selected from a random district number (C_D_ID
      // is randomly selected within [1 .. 10]), and a random remote warehouse
      // number (C_W_ID is randomly selected within the range of active
      // warehouses (see Clause 4.2.2), and C_W_ID ≠ W_ID).

      int32_t C_W_ID = W_ID;

      while (C_W_ID == W_ID) {
        C_W_ID = random.uniform_dist(1, context.partition_num);
      }

      query.C_W_ID = C_W_ID;
      query.C_D_ID = random.uniform_dist(1, 10);
    } else {
      // If x > 15 a customer is selected from the selected district number
      // (C_D_ID = D_ID) and the home warehouse number (C_W_ID = W_ID).

      query.C_D_ID = query.D_ID;
      query.C_W_ID = W_ID;
    }

    int y = random.uniform_dist(1, 100);

    // The customer is randomly selected 60% of the time by last name (C_W_ID ,
    // C_D_ID, C_LAST) and 40% of the time by number (C_W_ID , C_D_ID , C_ID).

    if (y <= 60) {
      // If y <= 60 a customer last name (C_LAST) is generated according to
      // Clause 4.3.2.3 from a non-uniform random value using the
      // NURand(255,0,999) function.

      std::string last_name =
          random.rand_last_name(random.non_uniform_distribution(255, 0, 999));
      query.C_LAST.assign(last_name);
      query.C_ID = 0;
    } else {
      // If y > 60 a non-uniform random customer number (C_ID) is selected using
      // the NURand(1023,1,3000) function.
      query.C_ID = random.non_uniform_distribution(1023, 1, 3000);
    }

    // The payment amount (H_AMOUNT) is randomly selected within [1.00 ..
    // 5,000.00].

    query.H_AMOUNT = random.uniform_dist(1, 5000);
    return query;
  }
};
} // namespace tpcc
} // namespace star
