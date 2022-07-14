//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_clusterer.cpp
//
// Identification: src/brain/query_clusterer.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include "brain/query_clusterer.h"
// #include "common/logger.h"
#include "core/Defs.h"
#include <glog/logging.h>
#include <numeric>

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_clusterer.h
//
// Identification: src/include/brain/query_clusterer.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <map>
#include <set>
#include <string>
#include <vector>

#include "brain/cluster.h"
#include "brain/kd_tree.h"
#include "common/bitmap.h"


namespace peloton {
namespace brain {

//===--------------------------------------------------------------------===//
// QueryClusterer
//===--------------------------------------------------------------------===//

class QueryClusterer {
 public:
  /**
   * @brief Constructor
   */
  QueryClusterer(int num_features, double threshold)
      : num_features_(num_features),
        threshold_(threshold),
        kd_tree_(num_features) {
          bitmap.init(500);
        }

  /**
   * @brief Collects the latest data from the server, update the feature
   * vectors of each template and insert the new templates into the clusters
   */
  void UpdateFeatures();

  /**
   * @brief 
   * 
   * @param fingerprint   template name 
   * @param feature 
   * @return Cluster* 
   */
  Cluster* CreateNewCluster(const std::string &fingerprint, const vector<double> &feature);


  /**
   * @brief 
   * 
   * @param cluster 
   */
  void DropCluster(Cluster *cluster);

  /**
   * @brief Update the cluster of the given template
   *
   * @param fingerprint - the fingerprint of the template whose cluster needs
   * to be updated
   * @param is_new - true if the fingerprint is seen for the first time
   */
  void UpdateTemplate(const std::string& fingerprint, bool is_new);

  /**
   * @brief Update the cluster of the existing templates in each cluster
   */
  void UpdateExistingTemplates();

  /**
   * @brief Merge the clusters that are within the threshold distance
   */
  void MergeClusters();

  /**
   * @brief Update the clusters for the current time period
   * This functions needs to be called by the scheduler at the end of every
   * period
   */
  void UpdateCluster();

  /**
   * @brief Add a feature (new/existing) into the cluster
   */
  void AddFeature(const std::string &fingerprint, std::vector<double>& feature);

  /**
   * @brief Return the all the clusters
   */
  const std::set<Cluster *> &GetClusters() const { return clusters_; }

  /**
   * @brief Destroyer
   */
  ~QueryClusterer();
  
 private:
  // Map from the fingerprint of the template query to its feature vector
  std::map<std::string, std::vector<double>> features_;
  // Map from the fingerprint of the template query to its frequency
  std::map<std::string, long long> frequency_;
  // Map from the fingerprint of the template query to its cluster
  std::map<std::string, Cluster *> template_cluster_;
  // Set of all clusters
  std::set<Cluster *> clusters_;
  // number of features or size of each feature vector
  int num_features_;
  // the threshold on similarity of feature vectors
  double threshold_;
  // KDTree for finding the nearest cluster
  KDTree kd_tree_;


  common::Bitmap bitmap;

};


void QueryClusterer::UpdateFeatures() {
  // Read the latest queries from server over RPC or the brainside catalog
  // Update the feature vectors for template queries and l2 - normalize them
  // For new templates - insert into templates_ and
  // call UpdateTemplate(fingerprint, true)
}
Cluster* QueryClusterer::CreateNewCluster(const std::string &fingerprint, const vector<double> &feature) {
  num_features_ = feature.size();
  Cluster *cluster = nullptr;
  cluster = new Cluster(num_features_);
  cluster->AddTemplateAndUpdateCentroid(fingerprint, feature);
  
  // set index
  int cluster_index = bitmap.next_unsetted_bit(0);
  bitmap.set_bit(cluster_index);
  cluster->SetIndex(cluster_index);

  // set freqency
  cluster->SetFrequency(frequency_[fingerprint]);

  return cluster;
}
void QueryClusterer::UpdateTemplate(const std::string& fingerprint, bool is_new) {
  // Find the nearest cluster of the template's feature vector by querying the
  // KDTree of the centroids of the clusters. If the similarity of the feature
  // with the cluster is greater than the threshold, add it to the cluster.
  // Otherwise create a new cluster with this template
  auto feature = features_[fingerprint];
  double similarity = 0.0;
  Cluster *cluster = nullptr;

  kd_tree_.GetNN(feature, cluster, similarity);

  if (cluster == nullptr) {
    // If the kd_tree_ is empty
    cluster = CreateNewCluster(fingerprint, feature);
    kd_tree_.Insert(cluster);
    clusters_.insert(cluster);
    template_cluster_[fingerprint] = cluster;
    VLOG(DEBUG_V6) << "new cluster created for @" << fingerprint << " index = " << cluster->GetIndex();
    return;
  }

  if (similarity > threshold_) {
    // If the nearest neighbor has a similarity higher than the threshold_
    if (is_new) {
      cluster->AddTemplateAndUpdateCentroid(fingerprint, feature);
      kd_tree_.Update(cluster);
    } else {
      // updating an existing template, so need not update the centroid
      cluster->AddTemplate(fingerprint);
    }
    // update frequency
    cluster->SetFrequency(cluster->GetFrequency() + frequency_[fingerprint]);

    VLOG(DEBUG_V6) << "cluster updated for @" << fingerprint << " index = " << cluster->GetIndex();

  } else {
    // create a new cluster as the nearest neighbor is not similar enough
    cluster = CreateNewCluster(fingerprint, feature);
    kd_tree_.Insert(cluster);
    clusters_.insert(cluster);
    VLOG(DEBUG_V6) << "new cluster created for @" << fingerprint << " index = " << cluster->GetIndex();
  }

  template_cluster_[fingerprint] = cluster;
}

void QueryClusterer::DropCluster(Cluster *cluster){

  bitmap.clear_bit(cluster->GetIndex());
  delete cluster;
}
void QueryClusterer::UpdateExistingTemplates() {
  // for each template check the similarity with the cluster
  // if the similarity is less than the threshold, then remove it
  // and insert into the next nearest cluster
  // Update the centroids at the end of the round only
  for (auto &feature : features_) {
    auto fingerprint = feature.first;
    auto *cluster = template_cluster_[fingerprint];
    auto similarity = cluster->CosineSimilarity(feature.second);
    if (similarity < threshold_) {
      VLOG(DEBUG_V6) << "   delete template @" << fingerprint << " from cluster" << " index = " << cluster->GetIndex();
      cluster->SetFrequency(cluster->GetFrequency() - frequency_[fingerprint]);
      cluster->RemoveTemplate(fingerprint);
      UpdateTemplate(fingerprint, false);
    }
  }

  std::vector<Cluster *> to_delete;
  for (auto &cluster : clusters_) {
    if (cluster->GetSize() == 0) {
      to_delete.push_back(cluster);
    } else {
      cluster->UpdateCentroid(features_);
    }
  }

  // Delete the clusters that are empty
  for (auto cluster : to_delete) {
    clusters_.erase(cluster);
    
    VLOG(DEBUG_V6) << "   delete empty cluster" << " index = " << cluster->GetIndex();
    DropCluster(cluster);
  }

  // Rebuild the tree to account for the deleted clusters
  kd_tree_.Build(clusters_);
}

void QueryClusterer::MergeClusters() {
  // Merge two clusters that are within the threshold in similarity
  // Iterate from left to right and merge the left one into right one and mark
  // the left one for deletion
  std::vector<Cluster *> to_delete;
  for (auto i = clusters_.begin(); i != clusters_.end(); i++) {
    for (auto j = i; ++j != clusters_.end();) {
      auto left = *i;
      auto right = *j;
      auto r_centroid = right->GetCentroid();
      auto similarity = left->CosineSimilarity(r_centroid);

      if (similarity > threshold_) {
        VLOG(DEBUG_V6) << "left cluster was merged" << " index = " << left->GetIndex() << " -> " << right->GetIndex();

        auto templates = left->GetTemplates();
        for (auto &fingerprint : templates) {
          right->AddTemplate(fingerprint);
          template_cluster_[fingerprint] = right;
          // update frequency
          right->SetFrequency(right->GetFrequency() + frequency_[fingerprint]);
        }
        right->UpdateCentroid(features_);
        to_delete.push_back(left);
        break;
      }
    }
  }

  // Delete the clusters that are empty
  for (auto cluster : to_delete) {
    clusters_.erase(cluster);
    VLOG(DEBUG_V6) << "delete merged cluster" << " index = " << cluster->GetIndex();
    DropCluster(cluster);
  }

  // Rebuild the KDTree to account for changed clusters
  kd_tree_.Build(clusters_);
}

void QueryClusterer::UpdateCluster() {
  // This function needs to be scheduled periodically for updating the clusters
  // Update the feature vectors of all templates, update new and existing
  // templates and merge the clusters
  UpdateFeatures();
  UpdateExistingTemplates();
  MergeClusters();
}

void QueryClusterer::AddFeature(const std::string &fingerprint,
                                std::vector<double>& feature) {
  // Normalize and add a feature into the cluster.
  // This is currently used only for testing.
  long long frequency = std::accumulate(feature.begin(), feature.end(),0);

  double l2_norm = 0.0;
  for (uint i = 0; i < feature.size(); i++) l2_norm += feature[i] * feature[i];

  if (l2_norm > 0.0)
    for (uint i = 0; i < feature.size(); i++) feature[i] /= l2_norm;

  if (features_.find(fingerprint) == features_.end()) {
    // Update the cluster if it's a new template
    features_[fingerprint] = feature;
    UpdateTemplate(fingerprint, true);
    // VLOG(DEBUG_V6) << "add new feature for @" << fingerprint;

  } else {
    features_[fingerprint] = feature;
    // VLOG(DEBUG_V6) << "update feature for @" << fingerprint;
  }
  frequency_[fingerprint] = frequency;

}

QueryClusterer::~QueryClusterer() {
  for (auto &cluster : clusters_) delete cluster;
}

}  // namespace brain
}  // namespace peloton
