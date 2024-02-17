//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "core/Context.h"

namespace star {
namespace tpcc {

enum class TPCCWorkloadType { NEW_ORDER_ONLY, PAYMENT_ONLY, MIXED };

class Context : public star::Context {
public:
  TPCCWorkloadType workloadType = TPCCWorkloadType::NEW_ORDER_ONLY;
  std::size_t getPartitionID(std::size_t key) const{
    // TODO
    return 0;
  }
  Context get_single_partition_context() const {
    Context c = *this;
    c.newOrderCrossPartitionProbability = 0;
    c.paymentCrossPartitionProbability = 0;
    c.operation_replication = this->operation_replication;
    c.star_sync_in_single_master_phase = this->star_sync_in_single_master_phase;
    return c;
  }
  
  std::size_t getGranule(std::size_t key) const { return key;}

  Context get_cross_partition_context() const {
    Context c = *this;
    c.newOrderCrossPartitionProbability = 100;
    c.paymentCrossPartitionProbability = 100;
    c.operation_replication = false;
    c.star_sync_in_single_master_phase = this->star_sync_in_single_master_phase;
    return c;
  }
  std::size_t keysPerTransaction = 10;
  std::size_t keysPerPartition = 200000;

  int newOrderCrossPartitionProbability = 10; // out of 100
  int paymentCrossPartitionProbability = 15;  // out of 100
};
} // namespace tpcc
} // namespace star
