
#pragma once

#include "brain/kd_tree.h"
#include <glog/logging.h>



#include "common/macros.h"
#include "annoy/annoylib.h"
#include "annoy/kissrandom.h"
#include "brain/cluster.h"

namespace peloton {
namespace brain {

//===--------------------------------------------------------------------===//
// KDTree
//===--------------------------------------------------------------------===//

/**
 * KDTree for finding the nearest neigbor of a vector of double of fixed size
 * using angular distance to be used by the Query Clusterer
 */
class KDTree {
 public:
  /**
   * @brief Constructor
   */
  KDTree(int num_features)
      : size_(0), num_features_(num_features), index_(num_features) {}

  /**
   * @brief Insert the cluster into the KDTree
   */
  void Insert(Cluster *cluster);

  /**
   * @brief Update the centroid of the cluster in the index
   */
  void Update(UNUSED_ATTRIBUTE Cluster *cluster);

  /**
   * @brief Get the neares neighbor of the feature in the index
   *
   * @param feature - the vector whose nearest neigbor is being searched for
   * @param cluster - return the cluster of the nearest neighbor in this
   * @param similarity - return the similarity to the nearest neighbor in this
   */
  void GetNN(std::vector<double> &feature, Cluster *&cluster,
             double &similarity);

  /**
   * @brief Reset the clusters and build the index again
   */
  void Build(std::set<Cluster *> &clusters);

 private:
  /**
   * @brief Helper function to build the index from scratch
   */
  void Build();

  // number of (centroid/cluster) entries in the KDTree
  int size_;
  // size of each centroid entry in the KDTree
  int num_features_;
  // similarity search structure for the KDTree
  // - each entry in it is indexed by an int
  // - each entry is a vector of double of fixed size - num_features_
  // - uses Angular Distance metric
  // - Kiss32Random - random number generator
  AnnoyIndex<int, double, Angular, Kiss32Random> index_;
  // clusters whose centroid is in the KDTree
  vector<Cluster *> clusters_;
};


void KDTree::Insert(Cluster *cluster) {
  // TODO[Siva]: Currently we ubuild and the build tree again for every change
  // to the structure. Will need to change AnnoyIndex to optimize this
  index_.unbuild();
  DCHECK(cluster->GetIndex() != -1);
  
  index_.add_item(cluster->GetIndex(), cluster->GetCentroid().data());
  index_.build(2 * num_features_);

  // cluster->SetIndex(new_cluster_index);
  clusters_.push_back(cluster);
  size_++;
}

// TODO[Siva]: cluster is unused as the index is rebuilt using the centroids
// of all the existing clusters already existing in the clusters_
void KDTree::Update(UNUSED_ATTRIBUTE Cluster *cluster) {
  // TODO[Siva]: Currently we ubuild and the build tree again for every change
  // to the structure. Will need to change AnnoyIndex to optimize this
  // The update to the centroid is reflected in the cluster. There is no change
  // to the clusters_, so just rebuild the entire index
  index_.unload();
  Build();
}

void KDTree::GetNN(std::vector<double> &feature, Cluster *&cluster,
                   double &similarity) {
  if (size_ == 0) {
    cluster = nullptr;
    similarity = 0.0;
    return;
  }

  std::vector<int> closest;
  std::vector<double> distances;
  index_.get_nns_by_vector(feature.data(), 1, (size_t)-1, &closest, &distances);

  for(size_t i = 0 ; i< clusters_.size(); i ++ ){
    if(clusters_[i]->GetIndex() == closest[0]){
      cluster = clusters_[i];
      break;
    }
  }
  DCHECK(cluster != nullptr);
  
  // convert the angular distance to corresponsing cosine similarity
  similarity = (2.0 - distances[0]) / 2.0;
}

void KDTree::Build() {
  for (int i = 0; i < size_; i++) {
    index_.add_item(clusters_[i]->GetIndex(), clusters_[i]->GetCentroid().data());
  }
  // number of random forests built by the AnnoyIndex = 2 * num_features
  // the more the faster, but requires more memory
  index_.build(2 * num_features_);
}

void KDTree::Build(std::set<Cluster *> &clusters) {
  index_.unload();
  clusters_.clear();
  for (auto &cluster : clusters) {
    clusters_.push_back(cluster);
  }
  size_ = clusters_.size();
  Build();
}

}  // namespace brain
}  // namespace peloton
