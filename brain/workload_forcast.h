
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
    /**
     * @brief 
     * 
     * @param frequnce[i][j]  0-2 | 0-1600
     */
    peloton::matrix_eig GetWorkload(std::vector<std::vector<int> > &frequnce) {

        peloton::matrix_eig data;
        // 
        // 
        size_t num_feats = frequnce.size();
        size_t num_samples = frequnce[0].size();

        data = peloton::matrix_eig::Zero(num_samples, num_feats);

        for (size_t i = 0; i < num_samples; ++ i) { // 0-1600
            for(size_t j = 0; j < num_feats; ++ j){ // 0-2
                data(i, j) = frequnce[j][i];
                // data 
                // ts1 : feat0 feat1 feat2
                // ts2 : feat0 feat1 feat2
                // ...
            }
        }
        return data;
    }
    
    class my_predictor {
    public:
        my_predictor(int preiod, int bptt, int horizon, float val_split){
            PREIOD = preiod;              // 160; // 40 / 0.25
            EPOCHS = 100;
            CLIP_NORM = 0.25;
            BPTT = bptt;                  // PREIOD * 2;
            HORIZON = horizon;            // PREIOD * 3;
            INTERVAL = 20;
            BATCH_SIZE = 4;

            LOG_INTERVAL = 20;            // print log after `LOG_INTERVAL` epochs
            // NUM_SAMPLES = num_samples; // 1600;
            // NUM_FEATS = num_feats;     // 3;
            VAL_SPLIT = val_split;        // 0.5;
            NORMALIZE = true;
            VAL_THESH = 0.05;

            model = std::unique_ptr<peloton::brain::TimeSeriesLSTM>(new peloton::brain::TimeSeriesLSTM(
                peloton::brain::LSTMWorkloadDefaults::NFEATS,
                peloton::brain::LSTMWorkloadDefaults::NENCODED, peloton::brain::LSTMWorkloadDefaults::NHID,
                peloton::brain::LSTMWorkloadDefaults::NLAYERS, peloton::brain::LSTMWorkloadDefaults::LR,
                peloton::brain::LSTMWorkloadDefaults::DROPOUT_RATE,
                peloton::brain::LSTMWorkloadDefaults::CLIP_NORM,
                peloton::brain::LSTMWorkloadDefaults::BATCH_SIZE,
                BPTT, HORIZON,
                peloton::brain::CommonWorkloadDefaults::INTERVAL,
                EPOCHS));
            n =  peloton::brain::Normalizer(NORMALIZE);
            early_stop_patience = peloton::brain::CommonWorkloadDefaults::ESTOP_PATIENCE;
            early_stop_delta = peloton::brain::CommonWorkloadDefaults::ESTOP_DELTA;
            
            DCHECK(model->IsTFModel() == true);
        }

        void train(peloton::matrix_eig& data){
            auto val_interval = std::min<size_t>(LOG_INTERVAL, model->GetEpochs()); // size_t

            // Determine the split point
            size_t split_point =
                data.rows() - static_cast<size_t>(data.rows() * VAL_SPLIT);

            // Split into train/test data
            peloton::matrix_eig train_data = data.topRows(split_point);
            n.Fit(train_data);
            train_data = n.Transform(train_data);

            // test_data for validate
            peloton::matrix_eig test_data = 
                n.Transform(data.bottomRows(
                    static_cast<size_t>(data.rows() - split_point)));

            vector_eig train_loss_avg = vector_eig::Zero(val_interval);
            // float prev_train_loss = std::numeric_limits<float>::max();
            float val_loss = VAL_THESH * 2;
            std::vector<float> val_losses;
            for (int epoch = 1; epoch <= model->GetEpochs() &&
                                !brain::ModelUtil::EarlyStop(
                                    val_losses, early_stop_patience, early_stop_delta);
                epoch++) {
                // std::cout << train_data << std::endl;
                auto train_loss = model->TrainEpoch(train_data);
                size_t idx = (epoch - 1) % val_interval;
                train_loss_avg(idx) = train_loss;
                if (epoch % val_interval == 0) {
                    val_loss = model->ValidateEpoch(test_data);
                    train_loss = train_loss_avg.mean();
                    // Below check is not advisable - one off failure chance
                    // EXPECT_LE(val_loss, prev_valid_loss);
                    // An average on the other hand should surely pass
                    // DCHECK(train_loss < prev_train_loss);
                    LOG(WARNING) <<  "Train Loss: " << train_loss << " Prev Train Loss: " << val_loss;
                    LOG(INFO) << "Train Loss: " << train_loss <<", Valid Loss: " << val_loss;
                    // prev_train_loss = train_loss;
                }
            }
            LOG(INFO) << "Training done.";
            // DCHECK(val_loss < val_loss_thresh);
        }
    
        peloton::matrix_eig predict(peloton::matrix_eig& data){
            n.Fit(data);
            data = n.Transform(data);

            matrix_eig y, y_hat;
            float val_loss = model->ValidateEpoch(data, y, y_hat);
            matrix_eig C(y.rows(), y_hat.cols() + y_hat.cols());
            C << y, y_hat;

            float mean_, std_, min_;
            n.GetParameters(mean_, std_, min_);
            matrix_eig C_ = C.unaryExpr([&](double x){
                return std::exp(x* std_ + mean_) - min_;
            }).cast<float>();
            
            LOG(INFO) << "Predict done.";
            /** output y and y_hat**/
            std::ofstream ofs("/home/zqs/star/data/y_hat.xls", std::ios::trunc);
            for(int i = 0; i < C_.rows(); i ++ ){
                for(int j = 0; j < C_.cols(); j ++ ){
                    ofs << C_(i, j) << "\t";
                }
                ofs << "\n";
            }
            ofs.close();
            return C_;
        }
        peloton::brain::Normalizer& get_normalizer(){
            return n;
        }
    private:
    std::unique_ptr<peloton::brain::TimeSeriesLSTM> model;
    peloton::brain::Normalizer n;

    int PREIOD = 160; // 40 / 0.25
    int EPOCHS = 100;
    int CLIP_NORM = 0.25;
    int BPTT = PREIOD * 2;
    int HORIZON = PREIOD * 3;
    int INTERVAL = 20;
    int BATCH_SIZE = 4;

    size_t LOG_INTERVAL = 20;
    // size_t NUM_SAMPLES = 1600;
    // size_t NUM_FEATS = 3;
    float VAL_SPLIT = 0.5;
    bool NORMALIZE = true;
    float VAL_THESH = 0.05;

    size_t early_stop_patience;
    float early_stop_delta;
    };
}
}
    