
#pragma once

#include "brain/workload/kernel_model.h"
#include "brain/util/model_util.h"



#include "brain/workload/base_tf.h"

namespace peloton {
namespace brain {
class TimeSeriesKernelReg : public virtual BaseForecastModel {
 public:
  TimeSeriesKernelReg(int bptt, int horizon, int interval);
  void Fit(const matrix_eig &X, const matrix_eig &y, int bsz = 1) override;
  matrix_eig Predict(const matrix_eig &X, int bsz = 1) const override;
  float TrainEpoch(const matrix_eig &data) override;
  float ValidateEpoch(const matrix_eig &data) override;
  /**
   * @return std::string representing model object
   */
  std::string ToString() const override;

 private:
  matrix_eig kernel_x_;
  matrix_eig kernel_y_;
};


/**
 * Kernel Regression model
 **/
TimeSeriesKernelReg::TimeSeriesKernelReg(int bptt, int horizon, int interval)
    : BaseForecastModel(bptt, horizon, interval) {}

std::string TimeSeriesKernelReg::ToString() const {
  std::stringstream model_str_builder;
  model_str_builder << "TimeSeriesKernelReg(";
  model_str_builder << "bptt = " << bptt_;
  model_str_builder << ", horizon = " << horizon_;
  model_str_builder << ", interval = " << interval_;
  model_str_builder << ")";
  return model_str_builder.str();
}

void TimeSeriesKernelReg::Fit(const matrix_eig &X, const matrix_eig &y,
                              UNUSED_ATTRIBUTE int bsz) {
  matrix_eig X_proc;
  ModelUtil::GenerateFeatureMatrix(*this, X, X_proc);
  kernel_x_ = X_proc;
  kernel_y_ = y;
}

matrix_eig TimeSeriesKernelReg::Predict(const matrix_eig &X,
                                        UNUSED_ATTRIBUTE int bsz) const {
  matrix_eig X_proc;
  ModelUtil::GenerateFeatureMatrix(*this, X, X_proc);
  matrix_eig kernel =
      (-EigenUtil::PairwiseEuclideanDist(X_proc, kernel_x_)).array().exp();
  // Eigen doesnt auto-broadcast on division
  matrix_eig norm = kernel.rowwise().sum().replicate(1, kernel_y_.cols());
  matrix_eig y_hat = (kernel * kernel_y_).array() / (norm.array());
  return y_hat;
}

float TimeSeriesKernelReg::TrainEpoch(const matrix_eig &data) {
  matrix_eig X, y;
  ModelUtil::FeatureLabelSplit(*this, data, X, y);
  Fit(X, y);
  matrix_eig y_hat = Predict(X);
  return ModelUtil::MeanSqError(y, y_hat);
}

float TimeSeriesKernelReg::ValidateEpoch(const matrix_eig &data) {
  matrix_eig X, y;
  ModelUtil::FeatureLabelSplit(*this, data, X, y);
  matrix_eig y_hat = Predict(X);
  return ModelUtil::MeanSqError(y, y_hat);
}

}  // namespace brain
}  // namespace peloton