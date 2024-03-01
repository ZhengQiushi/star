
#pragma once

#include "brain/workload/ensemble_model.h"
#include <numeric>
#include "brain/util/model_util.h"



#include "brain/workload/base_tf.h"

namespace LionBrain {
namespace brain {
class TimeSeriesEnsemble {
 public:
  TimeSeriesEnsemble(std::vector<std::shared_ptr<BaseForecastModel>> models,
                     const std::vector<float> &model_weights, int batch_size);
  float Validate(const matrix_eig &data);
  BaseForecastModel &GetModel(size_t idx);
  size_t ModelsSize() const;

 private:
  std::vector<std::shared_ptr<BaseForecastModel>> models_;
  int batch_size_;
  const vector_t model_weights_;
  // TODO(saatviks): Pass TFModel's bsz here(Find better way of handling this)
};

TimeSeriesEnsemble::TimeSeriesEnsemble(
    std::vector<std::shared_ptr<BaseForecastModel>> models,
    const vector_t &model_weights, int batch_size)
    : models_(models), batch_size_(batch_size), model_weights_(model_weights) {}

float TimeSeriesEnsemble::Validate(const matrix_eig &data) {
  std::vector<matrix_eig> preds;
  matrix_eig X, y_true;
  ModelUtil::FeatureLabelSplit(GetModel(0), data, X, y_true);
  matrix_eig y_ensemble_hat = matrix_eig::Zero(y_true.rows(), y_true.cols());
  for (size_t i = 0; i < ModelsSize(); i++) {
    auto &model = GetModel(i);
    matrix_eig y_hat;
    if (model.IsTFModel()) {
      std::vector<std::vector<matrix_eig>> data_batches;
      ModelUtil::GetBatches(model, X, batch_size_, data_batches);
      std::vector<matrix_eig> y_hat_batch;
      for (std::vector<matrix_eig> &batch : data_batches) {
        auto y_hat_i = model.Predict(EigenUtil::VStack(batch), batch.size());
        y_hat_batch.push_back(y_hat_i);
      }
      y_hat = EigenUtil::VStack(y_hat_batch);
    } else {
      y_hat = model.Predict(X, 1);
    }
    // Lstm models will have (bptt - 1) samples extra
    y_ensemble_hat += y_hat.bottomRows(y_true.rows()) * model_weights_[i];
  }
  y_ensemble_hat.array() /=
      std::accumulate(model_weights_.begin(), model_weights_.end(), 0.0f);
  return ModelUtil::MeanSqError(y_true, y_ensemble_hat);
}

BaseForecastModel &TimeSeriesEnsemble::GetModel(size_t idx) {
  return *models_[idx];
}

size_t TimeSeriesEnsemble::ModelsSize() const { return models_.size(); }

}  // namespace brain
}  // namespace LionBrain
