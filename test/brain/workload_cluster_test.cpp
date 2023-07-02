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
    double last_timestamp = 90; // 30 30 30 30

    peloton::brain::workload_data data;
    peloton::brain::get_workload_classified(cur_timestamp, last_timestamp, data);
    
    double period_duration = 90;
    double sample_interval = 0.5;
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

    DCHECK(top_k.size() <= top_cluster_num);


    std::ofstream offs("/home/star/data/getWorkLoad.xls", std::ios::trunc);
    int lenn = raw_features_.begin()->second.size();

    offs << "ts" << "\t";
    for(int j = 0 ; j < top_k.size(); j ++ ){
      for(auto& i : top_k[j]->get_templates()){
        offs << i << " " ;
      }
      offs << "\t";
    }
    offs << "\n";

    for(int i = 0 ; i < lenn; i ++ ){
      offs << sample_interval * i << "\t";

      for(int k = 0 ; k < top_k.size(); k ++ ){
        int sum = 0;

        for(auto& j : raw_features_){
          if(top_k[k]->get_templates().count(j.first)){
            sum += j.second[i];
          }
          
        }
        offs << sum << " " ;
        offs << "\t";
      }


      offs << "\n";
    }
    offs.close();



    std::ofstream ofs("/home/star/data/getWorkLoad_clustered.xls", std::ios::trunc);
    offs << "ts" << "\t";
    for(int j = 0 ; j < top_k.size(); j ++ ){
      for(auto& i : top_k[j]->get_templates()){
        ofs << i << " " ;
      }
      ofs << "\t";
    }
    ofs << "\n";
    
    int len = top_k[0]->GetCentroid().size();
    for(int i = 0 ; i < len; i ++ ){
      ofs << sample_interval * i << "\t";
      for(int j = 0 ; j < top_k.size(); j ++ ){
        ofs << top_k[j]->GetCentroid()[i] * top_k[j]->GetFrequency() << "\t";
      }
      ofs << "\n";
    }
    ofs.close();
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
