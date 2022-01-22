//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "core/Context.h"

#include <glog/logging.h>

namespace star {
namespace ycsb {


class Context : public star::Context {

public:
  std::size_t getPartitionID(std::size_t key) const override {
    DCHECK(key >= 0 && key < partition_num * keysPerPartition);

    if (strategy == PartitionStrategy::ROUND_ROBIN) {
      return key % partition_num;
    } else {
      return key / keysPerPartition;
    }
  }

  std::size_t getGlobalKeyID(std::size_t key, std::size_t partitionID) const {
    /**
     * @param key 某个分区上的ID
     * @param partitionID 第几个分区
     * @return 全局的key
     * @note 重写，原来默认每个分区只有5000个，现在需要可以数据迁移
     */
    DCHECK(key >= 0 && key < keysPerPartition && partitionID >= 0 &&
           partitionID < partition_num);

    if (strategy == PartitionStrategy::ROUND_ROBIN) {
      return key * partition_num + partitionID;
    } else {
      return partitionID * keysPerPartition + key;
    }
  }

  Context get_single_partition_context() const {
    Context c = *this;
    c.crossPartitionProbability = 0;
    c.operation_replication = this->operation_replication;
    c.star_sync_in_single_master_phase = false;
    return c;
  }

  Context get_cross_partition_context() const {
    Context c = *this;
    c.crossPartitionProbability = 100;
    c.operation_replication = false;
    c.star_sync_in_single_master_phase = this->star_sync_in_single_master_phase;
    return c;
  }

public:
  int readWriteRatio = 0;            // out of 100
  int readOnlyTransaction = 0;       //  out of 100
  int crossPartitionProbability = 0; // out of 100

  std::size_t keysPerTransaction = 10;
  std::size_t keysPerPartition = 200000;

  std::size_t nop_prob = 0; // out of 10000
  std::size_t n_nop = 0;

  bool isUniform = true;

};
} // namespace ycsb
} // namespace star
