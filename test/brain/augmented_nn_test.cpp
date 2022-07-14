//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// augmented_nn_test.cpp
//
// Identification: test/brain/augmented_nn_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include "brain/selectivity/augmented_nn.h"
#include "brain/selectivity/selectivity_defaults.h"
#include "brain/testing_augmented_nn_util.h"
#include "brain/util/model_util.h"
#include "brain/workload/workload_defaults.h"
#include "common/harness.h"
#include <gtest/gtest.h>

// #include "util/file_util.h"

using  namespace peloton ;// {
using  namespace test ;// {
class AugmentedNNTests : public PelotonTest {};

TEST(AugmentedNNTests, AugmentedNNUniformTest) {
  auto model = std::unique_ptr<brain::AugmentedNN>(new brain::AugmentedNN(
      brain::AugmentedNNDefaults::COLUMN_NUM,
      brain::AugmentedNNDefaults::ORDER,
      brain::AugmentedNNDefaults::NEURON_NUM,
      brain::AugmentedNNDefaults::LR,
      brain::AugmentedNNDefaults::BATCH_SIZE,
      brain::AugmentedNNDefaults::EPOCHS));
  EXPECT_TRUE(model->IsTFModel());
  size_t LOG_INTERVAL = 20;
  size_t NUM_SAMPLES = 10000;
  float VAL_SPLIT = 0.5;
  bool NORMALIZE = false;
  float VAL_THESH = 0.05;

  TestingAugmentedNNUtil::Test(*model, DistributionType::UniformDistribution,
                               LOG_INTERVAL, NUM_SAMPLES,
                               VAL_SPLIT, NORMALIZE, VAL_THESH,
                               brain::CommonWorkloadDefaults::ESTOP_PATIENCE,
                               brain::CommonWorkloadDefaults::ESTOP_DELTA);
}

TEST(AugmentedNNTests, AugmentedNNSkewedTest) {
  auto model = std::unique_ptr<brain::AugmentedNN>(new brain::AugmentedNN(
      brain::AugmentedNNDefaults::COLUMN_NUM,
      brain::AugmentedNNDefaults::ORDER,
      brain::AugmentedNNDefaults::NEURON_NUM,
      brain::AugmentedNNDefaults::LR,
      brain::AugmentedNNDefaults::BATCH_SIZE,
      brain::AugmentedNNDefaults::EPOCHS));
  EXPECT_TRUE(model->IsTFModel());
  size_t LOG_INTERVAL = 20;
  size_t NUM_SAMPLES = 10000;
  float VAL_SPLIT = 0.5;
  bool NORMALIZE = false;
  float VAL_THESH = 0.05;

  TestingAugmentedNNUtil::Test(*model, DistributionType::SkewedDistribution,
                               LOG_INTERVAL, NUM_SAMPLES,
                               VAL_SPLIT, NORMALIZE, VAL_THESH,
                               brain::CommonWorkloadDefaults::ESTOP_PATIENCE,
                               brain::CommonWorkloadDefaults::ESTOP_DELTA);
}

int main(int argc, char **argv)
{

  // 分析gtest程序的命令行参数
  testing::InitGoogleTest(&argc, argv);

  // 调用RUN_ALL_TESTS()运行所有测试用例
  // main函数返回RUN_ALL_TESTS()的运行结果

  int rc = RUN_ALL_TESTS();

  return rc;
}

// }  // namespace test
// }  // namespace peloton
