
#pragma once

#include <unistd.h>
#include <bitset>
#include <climits>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/macros.h"

// Impose Row-major to avoid confusion
#define EIGEN_DEFAULT_TO_ROW_MAJOR
#include "eigen3/Eigen/Dense"

namespace peloton {

typedef std::vector<float> vector_t;
typedef std::vector<std::vector<float>> matrix_t;
typedef Eigen::Matrix<float, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>
    matrix_eig;
typedef Eigen::Matrix<float, Eigen::Dynamic, Eigen::RowMajor> vector_eig;

}  // namespace peloton
