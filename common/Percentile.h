//
// Created by Yi Lu on 8/29/18.
//

#pragma once

#include <algorithm>
#include <cmath>
#include <fstream>
#include <glog/logging.h>
#include <mutex>

// The nearest-rank method
// https://en.wikipedia.org/wiki/Percentile

namespace star {

template <class T> class Percentile {
public:
  using element_type = T;
  // std::mutex mm;

  void add(const element_type &value) {
    // std::lock_guard<std::mutex> l(mm);
    isSorted_ = false;
    if(busy.load() == true) return;
    data_.push_back(value);
  }


  void add(const std::vector<element_type> &v) {
    // std::lock_guard<std::mutex> l(mm);
    isSorted_ = false;
    if(busy.load() == true) return;
    std::copy(v.begin(), v.end(), std::back_inserter(data_));
  }

  void clear() {
    // std::lock_guard<std::mutex> l(mm);
    isSorted_ = true;
    data_.clear();
  }

  auto size() { return data_.size(); }

  element_type nth(double n) {
    // std::lock_guard<std::mutex> l(mm);
    if (data_.size() == 0) {
      element_type nn;
      return nn;
    }
    checkSort();
    DCHECK(n > 0 && n <= 100);
    auto sz = size();
    auto i = static_cast<decltype(sz)>(ceil(n / 100 * sz)) - 1;
    DCHECK(i >= 0 && i < size());
    return data_[i];
  }

  int avg() {
    return sum / (size() + 0.1);
  }

  void save_cdf(const std::string &path) {
    // std::lock_guard<std::mutex> l(mm);
    if (data_.size() == 0) {
      return;
    }
    checkSort();

    if (path.empty()) {
      return;
    }

    std::ofstream cdf;
    cdf.open(path);

    cdf << "value\tcdf" << std::endl;

    // output ~ 1k rows
    auto step_size = std::max(1, int(data_.size() * 0.99 / 1000));

    std::vector<element_type> cdf_result;

    for (auto i = 0u; i < 0.99 * data_.size(); i += step_size) {
      cdf_result.push_back(data_[i]);
    }

    for (auto i = 0u; i < cdf_result.size(); i++) {
      cdf << cdf_result[i] << "\t" << 1.0 * (i + 1) / cdf_result.size()
          << std::endl;
    }

    cdf.close();
  }

private:
  void checkSort() {
    busy.store(true);
    if (!isSorted_) {
      std::sort(data_.begin(), data_.end());
      isSorted_ = true;
    }
    busy.store(false);
  }

private:
  std::atomic<int> busy;
  bool isSorted_ = true;
  std::vector<element_type> data_;
  int sum = 0;
};
} // namespace star