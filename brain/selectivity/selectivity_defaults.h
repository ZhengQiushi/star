//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// selectivity_defaults.cpp
//
// Identification: src/brain/workload/selectivity_defaults.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/selectivity/selectivity_defaults.h"
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// selectivity_defaults.h
//
// Identification: src/include/brain/workload/selectivity_defaults.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

/**
 * This header file contains default attributes
 * associated with the selectivity prediction task
 **/

namespace peloton {
namespace brain {

struct AugmentedNNDefaults {
  static const int COLUMN_NUM;
  static const int ORDER;
  static const int NEURON_NUM;
  static const float LR;
  static const int BATCH_SIZE;
  static const int EPOCHS;
};

const int AugmentedNNDefaults::COLUMN_NUM = 1;
const int AugmentedNNDefaults::ORDER = 1;
const int AugmentedNNDefaults::NEURON_NUM = 16;
const float AugmentedNNDefaults::LR = 0.1f;
const int AugmentedNNDefaults::BATCH_SIZE = 256;
const int AugmentedNNDefaults::EPOCHS = 600;


}  // namespace brain
}  // namespace peloton
