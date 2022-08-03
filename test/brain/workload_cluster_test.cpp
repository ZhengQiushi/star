//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_clusterer_test.cpp
//
// Identification: test/brain/query_clusterer_test.cpp
//
// Copyright (c) 2017-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/workload_cluster.h"
#include "common/harness.h"

using  namespace peloton;
using  namespace test;


class QueryClustererTests : public PelotonTest {};

TEST(QueryClustererTests, ClusterTest) {

    double cur_timestamp = 0;
    double last_timestamp = 120;

    peloton::brain::workload_data data;
    peloton::brain::get_workload_classified(cur_timestamp, last_timestamp, data);
    
    double period_duration = 40;
    double sample_interval = 0.25;
    const size_t top_cluster_num = 3;

    std::map<std::string, std::vector<double>> raw_features_;
    // 
    int num_features = period_duration / sample_interval; 
    double threshold = 0.8;
    brain::QueryClusterer query_clusterer(num_features, threshold);
    peloton::brain::onlineClustering(raw_features_, cur_timestamp, last_timestamp, 
                                                                               period_duration , sample_interval, 
                                                                               query_clusterer, data);
    
    std::vector<peloton::brain::Cluster*> top_k = peloton::brain::getTopCoverage(top_cluster_num, query_clusterer);

    DCHECK(top_k.size() == top_cluster_num);
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
