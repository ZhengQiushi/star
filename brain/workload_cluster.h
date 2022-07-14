#pragma once

#include <iostream>
#include <string>
#include <vector>
#include <set>
#include <fstream>
#include <sstream>
#include <map>
#include <glog/logging.h>

#include "brain/query_clusterer.h"
#include "core/Defs.h"
#include "common/internal_types.h"
#include "brain/util/model_util.h"
#include "brain/util/eigen_util.h"
#include "brain/util/tf_util.h"
#include "brain/workload/base_tf.h"
#include "brain/workload/lstm.h"
#include "brain/workload/workload_defaults.h"


namespace peloton {
namespace brain {
    struct workload_data {
        void insert(std::string workload_type, double timestamp){
            if(contents.find(workload_type) == contents.end()){
                // insert into a new map
                std::map<double, int> new_;
                new_[timestamp] = 1;
                contents[workload_type] = new_;
            } else {
                // 
                auto& workload_type_ = contents[workload_type];
                if(workload_type_.find(timestamp) == workload_type_.end()){
                    workload_type_[timestamp] = 1;
                } else {
                    workload_type_[timestamp] += 1;
                }
            }
        }

        void clear(){
            contents.clear();
        }
        std::map<std::string, std::map<double, int> > contents;
    };

    struct assignment
    {
        std::map<std::string, int> template_cluster_id;
    };
    

    /**
     * @brief Get the workload classified object
     * 
     * @param current_timestamp 
     * @param last_timestamp 
     * @param data 
     */
    void get_workload_classified(double current_timestamp, double last_timestamp, workload_data& data){
        bool verbose = false;

        //读文件
        std::ifstream ifs("/home/zqs/project/workload_predict/data/result_test.xls", std::ios::in);
        std::ofstream ofs("/home/zqs/project/workload_predict/data/result_.xls", std::ios::trunc);

        std::string _line;
        int line = 0;
        int cur_period = 0;

        while (getline(ifs, _line)){
            line ++ ;
            //解析每行的数据
            std::stringstream ss(_line);
            std::string _sub;
            std::vector<std::string> subArray;

            //按照逗号分隔
            while (getline(ss, _sub, '\t'))
                subArray.push_back(_sub);
            
            //输出解析后的每行数据
            std::set<int> non_repeated;
            double timestamp;

            // 
            for (size_t i=0; i<subArray.size(); ++i) {
                // 表头： timestamp	txn-items
                if(line == 1){
                    if(verbose)
                        ofs << subArray[i] << "\t";
                } else {
                    // 正式内容：一个时间戳 + 10个key 
                    //          0.024085	160	601601	601602	601603	601604	601605	601606	601607	601608	601609	
                    if(i == 0){
                        // timestamp
                        timestamp = atof(subArray[i].data());
                        if(timestamp > last_timestamp){
                            return;
                        }
                        if(verbose)
                            ofs << subArray[i] << "\t";
                    } else {
                        // 对应的分区
                        non_repeated.insert(atoi(subArray[i].data()) / 200000);
                    }
                }
                if(verbose)
                    std::cout << subArray[i] << "\t";
            }
            // workload_type
            std::string workload_type = "";
            if(verbose)
                ofs << "@";
            for(auto iter = non_repeated.begin(); iter != non_repeated.end(); iter ++ ){
                if(verbose)
                    ofs << *iter;
                workload_type += std::to_string(*iter);
            }
            if(verbose){
                ofs << "\n";
                std::cout << std::endl;  
            }

            if(workload_type != ""){
                // 
                data.insert(workload_type, timestamp);
                if(int(timestamp) / 20 > cur_period){
                    cur_period ++;
                }
            }
        }
    }

    /**
     * @brief 
     * 
     * @param cur_timestamp 
     * @param last_timestamp 
     * @param period_duration 
     * @param sample_interval 
     * @param data 
     * @return QueryClusterer 
     */
    QueryClusterer onlineClustering(std::map<std::string, std::vector<double>>& raw_features_, 
                                    double cur_timestamp, double last_timestamp, 
                                    double period_duration, double sample_interval, 
                                    workload_data& data){
        int feature_nums = int(ceil(period_duration / sample_interval));
        // same cluster merging threshold
        double threshold = 0.8;
        // divide into several period
        int num_gaps = int(ceil(last_timestamp / period_duration));

        QueryClusterer cluster(feature_nums, threshold);
        
        for(int i = 0 ; i < num_gaps; i ++ ){
            VLOG(DEBUG_V6) << "@@@ gap[" << i << "] @@@";
            // loop for each period
            int current_period_ts = i * period_duration;
            int next_period_ts = (i + 1) * period_duration;

            for(auto& d_: data.contents){
                // loop for each template for each period
                // sample using sub-intervals
                std::vector<double> new_feature(feature_nums, 0);
                int interval_cnt = 0;
                int interval_sum = 0;
                int total_sample_num = 0;

                std::string fingerprint = d_.first;
                auto& fingerprint_data = d_.second;

                // data in current interval range
                std::map<double, int>::iterator low = fingerprint_data.lower_bound(current_period_ts);

                while (low != fingerprint_data.end()){
                    // pass current range    
                    if(low->first > next_period_ts || current_period_ts + interval_cnt * sample_interval > next_period_ts){
                        break;
                    }
                    // in range 
                    if(current_period_ts + interval_cnt * sample_interval <= low->first && 
                       low->first < current_period_ts + (interval_cnt + 1) * sample_interval) {
                        interval_sum += low->second;
                        total_sample_num += 1;
                        low ++ ;
                    } else {
                        // new interval range start
                        new_feature[interval_cnt] = interval_sum;
                        interval_sum = 0;
                        interval_cnt ++ ;
                    }
                }

                // record all the feature
                if(raw_features_.find(fingerprint) == raw_features_.end()){
                    raw_features_[fingerprint] = new_feature;
                } else {
                    raw_features_[fingerprint].insert(raw_features_[fingerprint].end(), new_feature.begin(), new_feature.end());
                }

                if(total_sample_num != 0){
                    // add new template to current cluster
                    cluster.AddFeature(fingerprint, new_feature);
                    cluster.UpdateCluster();
                }
            }
        }
        
        return cluster;
    }

    /**
     * @brief Get the Top Coverage object
     * 
     * @param top_cluster_num 
     * @param clusters 
     * @return std::vector<Cluster*> 
     */
    std::vector<Cluster*> getTopCoverage(size_t top_cluster_num, const QueryClusterer& clusters){

        auto clusters_ = clusters.GetClusters();
        std::vector<Cluster*> top_k;

        for(auto iter: clusters_){
            top_k.push_back(iter);
        }

        sort(top_k.begin(), top_k.end(), [](const Cluster* a, const Cluster* b){
            return a->GetFrequency() > b->GetFrequency();
        });

        while(top_k.size() > top_cluster_num){
            top_k.pop_back();
        }

        return top_k;
    }

}
}