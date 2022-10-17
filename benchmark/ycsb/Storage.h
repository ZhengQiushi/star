//
// Created by Yi Lu on 9/12/18.
//

#pragma once

#include "benchmark/ycsb/Schema.h"

namespace star {

namespace ycsb {
struct Storage {
  Storage(){
    memset(ycsb_keys, 0, sizeof(ycsb_keys));
    memset(ycsb_values, 0, sizeof(ycsb_values));
  }
  ycsb::key ycsb_keys[YCSB_TRANSMIT_FIELD_SIZE];
  ycsb::value ycsb_values[YCSB_TRANSMIT_FIELD_SIZE];
};

} // namespace ycsb
} // namespace star