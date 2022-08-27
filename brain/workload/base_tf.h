//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// base_tf.cpp
//
// Identification: src/brain/workload/base_tf.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include "brain/workload/base_tf.h"
#include "brain/util/tf_session_entity/tf_session_entity.h"
#include "brain/util/tf_session_entity/tf_session_entity_input.h"
#include "brain/util/tf_session_entity/tf_session_entity_output.h"
// #include "util/file_util.h"

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// base_tf.h
//
// Identification: src/include/brain/workload/base_tf.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <memory>
#include "brain/util/eigen_util.h"
#include "brain/util/tf_session_entity/tf_session_entity_io.h"

namespace peloton {
namespace brain {

/**
 * Normalizer
 */
class Normalizer {
 public:
  Normalizer(bool do_normalize = true) : do_normalize_(do_normalize){};
  /**
   * @brief calculate `min`, `std` for normalization
   * 
   * @param X 
   */
  void Fit(const matrix_eig &X);
  /**
   * @brief do normalization
   * 
   * @param X 
   * @return matrix_eig 
   */
  matrix_eig Transform(const matrix_eig &X) const;
  /**
   * @brief recover from normalized data
   * 
   * @param X 
   * @return matrix_eig 
   */
  matrix_eig ReverseTransform(const matrix_eig &X) const;

  void GetParameters(float& mean_, float& std_, float& min_) const;
 private:
  float mean_;
  float std_;
  float min_;
  bool do_normalize_;
  bool fit_complete_;
};

/**
 * Base Abstract class to inherit for writing ML models
 * Following a similar base as sklearn
 */
class BaseModel {
 public:
  // virtual ~BaseModel() = 0;
  virtual std::string ToString() const = 0;
  virtual void Fit(const matrix_eig &X, const matrix_eig &y, int bsz) = 0;
  virtual matrix_eig Predict(const matrix_eig &X, int bsz) const = 0;
  virtual bool IsTFModel() const { return false; }
};

/**
 * Base Abstract class to inherit for writing forecasting ML models
 */
class BaseForecastModel : public virtual BaseModel {
 public:
  BaseForecastModel(int bptt, int horizon, int interval, int epochs = 1)
      : // BaseModel(),
        bptt_(bptt),
        horizon_(horizon),
        interval_(interval),
        epochs_(epochs){};
  virtual float TrainEpoch(const matrix_eig &data) = 0;
  virtual float ValidateEpoch(const matrix_eig &data) = 0;
  virtual float ValidateEpoch(const matrix_eig &data, matrix_eig &y, matrix_eig &y_hat) = 0;

  int GetHorizon() const { return horizon_; }
  int GetBPTT() const { return bptt_; }
  int GetInterval() const { return interval_; }
  int GetPaddlingDays() const { return paddling_days_; }
  int GetEpochs() const { return epochs_; }

 protected:
  int bptt_;
  int horizon_;
  int interval_;
  int paddling_days_;
  int epochs_;
};

template <typename InputType, typename OutputType>
class TfSessionEntity;

/**
 * BaseTFModel is an abstract class defining constructor/basic operations
 * that needed to be supported by a Tensorflow model for sequence/workload
 * prediction.
 * Note that the base assumption is that the graph would be generated in
 * tensorflow-python
 * which is a well developed API. This model graph and its Ops can be serialized
 * to
 * `protobuf` format and imported on the C++ end. The constructor will always
 * accept a string
 * path to the serialized graph and import the same.
 */
class BaseTFModel : public virtual BaseModel {
 public:
  // Constructor - sets up session object
  BaseTFModel(const std::string &modelgen_path, const std::string &pymodel_path,
              const std::string &graph_path);
  // Destructor - cleans up any generated model files
  ~BaseTFModel();

  /**
   * Global variable initialization
   * Should be called (1) before training for the first time OR (2) when there
   * is
   * a need for re-training the model.
   */
  void TFInit();

  bool IsTFModel() const override { return true; }

 protected:
  std::unique_ptr<TfSessionEntity<float, float>> tf_session_entity_;
  // Path to the working directory to use to write graph - Must be set in child
  // constructors
  std::string modelgen_path_;
  // Path to the Python TF model to use - Relative path must be passed in
  // constructor
  std::string pymodel_path_;
  // Path to the written graph - Relative path must be passed in constructor
  std::string graph_path_;
  // Function to Generate the tensorflow model graph.
  void GenerateModel(const std::string &args_str);
  // Lambda function for deleting input/output objects
  std::function<void(TfSessionEntityIOBase<float> *)> TFIO_Delete =
      [&](TfSessionEntityIOBase<float> *ptr) { delete ptr; };
};

/**
 * Normalizer methods
 */
void Normalizer::Fit(const matrix_eig &X) {
  if (do_normalize_) {
    min_ = 1 - X.minCoeff();
    matrix_eig Xadj = (X.array() + min_).array().log();
    mean_ = Xadj.mean();
    Xadj.array() -= mean_;
    std_ = EigenUtil::StandardDeviation(Xadj);
  }
  fit_complete_ = true;
}

matrix_eig Normalizer::Transform(const matrix_eig &X) const {
  if (fit_complete_ && do_normalize_) {
    matrix_eig Xadj = (X.array() + min_).array().log();
    Xadj.array() -= mean_;
    return Xadj.array() / std_;
  } else if (do_normalize_) {
    throw("Please call `Fit` before `Transform` or `ReverseTransform`");
  } else {
    return X;
  }
}

matrix_eig Normalizer::ReverseTransform(const matrix_eig &X) const {
  if (fit_complete_ && do_normalize_) {
    matrix_eig Xadj = X.array() * std_ + mean_;
    return Xadj.array().exp() - min_;
  } else if (do_normalize_) {
    throw("Please call `Fit` before `Transform` or `ReverseTransform`");
  } else {
    return X;
  }
}

void Normalizer::GetParameters(float& m_, float& s_, float& mi_) const{
  m_ = mean_;
  s_ = std_;
  mi_ = min_;
}

/**
 * BaseTFModel methods
 */
BaseTFModel::BaseTFModel(const std::string &modelgen_path,
                         const std::string &pymodel_path,
                         const std::string &graph_path)
    : // BaseModel(),
      modelgen_path_(("/root/star/" + modelgen_path)), // peloton::FileUtil::GetRelativeToRootPath
      pymodel_path_(("/root/star/" + pymodel_path)), // peloton::FileUtil::GetRelativeToRootPath(
      graph_path_(("/root/star/" + graph_path)) { // peloton::FileUtil::GetRelativeToRootPath
  tf_session_entity_ = std::unique_ptr<TfSessionEntity<float, float>>(
      new TfSessionEntity<float, float>());
  // PELOTON_ASSERT(FileUtil::Exists(pymodel_path_));
}

BaseTFModel::~BaseTFModel() { remove(graph_path_.c_str()); }

void BaseTFModel::TFInit() {
  tf_session_entity_->Eval("init");
  PELOTON_ASSERT(tf_session_entity_->IsStatusOk());
}

void BaseTFModel::GenerateModel(const std::string &args_str) {
  std::string cmd = "python3 \"" + pymodel_path_ + "\" " + args_str;
  // LOG_DEBUG("Executing command: %s", cmd.c_str());
  UNUSED_ATTRIBUTE bool succ = system(cmd.c_str());
  PELOTON_ASSERT(succ == 0);
  // PELOTON_ASSERT(FileUtil::Exists(graph_path_));
}
}  // namespace brain
}  // namespace peloton