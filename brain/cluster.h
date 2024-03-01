
#pragma once

#include "brain/cluster.h"
#include "common/macros.h"


#include <cmath>
#include <map>
#include <set>
#include <string>
#include <vector>
#include <stdint.h>

namespace LionBrain {
namespace brain {

//===--------------------------------------------------------------------===//
// Cluster
//===--------------------------------------------------------------------===//

class Cluster {
 public:
  Cluster(int num_features) : centroid_(num_features, 0.0) {
    index_ = -1;
    frequency_ = 0;
  }

  /**
   * @brief Add the fingerprint to the set of templates of the cluster
   * and update the centroid of the cluster
   */
  void AddTemplateAndUpdateCentroid(const std::string &fingerprint,
                                    const std::vector<double> &feature);

  /**
   * @brief Add the fingerprint to the set of templates without updating the
   * centroid
   */
  void AddTemplate(const std::string &fingerprint);

  /**
   * @brief Remove the fingerprint from the set of templates without updating
   * the centroid
   */
  void RemoveTemplate(const std::string &fingerprint);

  /**
   * @brief Update the centroid of the cluster
   *
   * @param features : map from the fingerprint to its feature vector
   */
  void UpdateCentroid(std::map<std::string, std::vector<double> > &features);

  /**
   * @brief Compute the cosine similarity between the centroid of the cluster
   * and the given feature vector
   */
  double CosineSimilarity(std::vector<double> &feature);

  /**
   * @brief Return the index of the cluster in the KDTree
   */
  int GetIndex() { return index_; }

  /**
   * @brief Set the index of the cluster, set by the KDTree
   */
  void SetIndex(int index) { index_ = index; }

  /**
   * @brief Return the index of the cluster in the KDTree
   */
  long long GetFrequency() const { return frequency_; }

  /**
   * @brief Set the index of the cluster, set by the KDTree
   */
  void SetFrequency(long long frequecny) { frequency_ = frequecny; }

  /**
   * @brief Return the number of fingerprints in the cluster
   */
  int GetSize() { return templates_.size(); }

  /**
   * @brief Return the centroid of the cluster
   */
  std::vector<double> GetCentroid() { return centroid_; }

  /**
   * @brief Return the fingerprints in the cluster
   */
  const std::set<std::string> &GetTemplates() const { return templates_; }

  const std::set<std::string> &get_templates(){
    return templates_;
  }
 private:
  // index of the cluster in the KDTree
  uint32_t index_;
  // centroid of the cluster
  std::vector<double> centroid_;
  // fingerprints in the cluster
  std::set<std::string> templates_;
  // frequency 
  long long frequency_;
};



void Cluster::AddTemplateAndUpdateCentroid(const std::string &fingerprint,
                                           const std::vector<double> &feature) {
  size_t num_templates = templates_.size();
  for (unsigned int i = 0u; i < feature.size(); i++) {
    centroid_[i] +=
        (centroid_[i] * num_templates + feature[i]) * 1.0 / (num_templates + 1);
  }
  templates_.insert(fingerprint);
}

void Cluster::AddTemplate(const std::string &fingerprint) {
  templates_.insert(fingerprint);
}

void Cluster::RemoveTemplate(const std::string &fingerprint) {
  templates_.erase(fingerprint);
}

void Cluster::UpdateCentroid(
    std::map<std::string, std::vector<double> > &features) {
  int num_features = centroid_.size();
  std::fill(centroid_.begin(), centroid_.end(), 0);
  PELOTON_ASSERT(templates_.size() != 0);

  for(std::set<std::string>::iterator iter = templates_.begin(); iter != templates_.end(); iter ++ ) {
    std::string fingerprint = *iter;
    std::vector<double> feature = features[fingerprint];
    for (int i = 0; i < num_features; i++) {
      centroid_[i] += feature[i];
    }
  }

  for (int i = 0; i < num_features; i++) {
    centroid_[i] /= (templates_.size());
  }
}

double Cluster::CosineSimilarity(std::vector<double> &feature) {
  double dot = 0.0, denom_a = 0.0, denom_b = 0.0;
  // double epsilon = 1e-5;
  for (unsigned int i = 0u; i < feature.size(); i++) {
    dot += centroid_[i] * feature[i];
    denom_a += centroid_[i] * centroid_[i];
    denom_b += feature[i] * feature[i];
  }

  // if (denom_a < epsilon || denom_b < epsilon) return 0.0;

  return dot / (sqrt(denom_a) * sqrt(denom_b));
}

}  // namespace brain
}  // namespace LionBrain
