//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tf_session_entity_input.cpp
//
// Identification: src/brain/util/tf_session_entity/tf_session_entity_input.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include "brain/util/tf_session_entity/tf_session_entity_input.h"

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tf_session_entity_input.h
//
// Identification:
// src/include/brain/util/tf_session_entity/tf_session_entity_input.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "tf_session_entity_io.h"

#define TFSEIN_TEMPLATE_ARGUMENTS template <typename InputType>
#define TFSEIN_TYPE TfSessionEntityInput<InputType>

namespace peloton {
namespace brain {

/**
 * TfSessionEntityInput is a wrapper class to make passing inputs
 * to TFHelper/Tensorflow-C much simpler. It can
 * take commonly used data structures(currently constants,
 * std::vector both 1d and 2d) which are processed to
 * native type flattened arrays needed by by the C-API.
 * Alternatively itll accept native type flattened arrays directly.
 * It minimized user inputs and attempts to maximize the work
 * of filling in the blanks for the TF-C API function calls
 * The TfSessionEntity::Eval requires a std::vector of TfSessionEntityInputs
 * TODO: Add support for other commonly used std containers
 */
TFSEIN_TEMPLATE_ARGUMENTS
class TfSessionEntityInput : public TfSessionEntityIOBase<InputType> {
 public:
  // Const Input
  TfSessionEntityInput(const InputType &input, const std::string &op);
  // 1d vector
  TfSessionEntityInput(const std::vector<InputType> &input,
                       const std::string &op);
  // 2d vector
  TfSessionEntityInput(const std::vector<std::vector<InputType>> &input,
                       const std::string &op);
  // raw flattened input
  TfSessionEntityInput(InputType *input, const std::vector<int64_t> &dims,
                       const std::string &op);

 private:
  // Flattens 2d inputs
  InputType *Flatten(const std::vector<std::vector<InputType>> &elems);
};

TFSEIN_TEMPLATE_ARGUMENTS
TFSEIN_TYPE::TfSessionEntityInput(const InputType &input,
                                  const std::string &op) {
  this->placeholder_name_ = op;
  this->DetermineDataType();
  InputType input_for_tf = input;
  this->tensor_ =
      TF_AllocateTensor(this->data_type_, nullptr, 0, sizeof(InputType));
  auto buff = (InputType *)TF_TensorData(this->tensor_);
  PELOTON_MEMCPY(buff, &input_for_tf, sizeof(InputType));
}

// 1d vector
TFSEIN_TEMPLATE_ARGUMENTS
TFSEIN_TYPE::TfSessionEntityInput(const std::vector<InputType> &input,
                                  const std::string &op) {
  this->placeholder_name_ = op;
  this->DetermineDataType();
  int64_t dims[] = {static_cast<int64_t>(input.size())};
  const InputType *input_for_tf = input.data();
  this->tensor_ =
      TF_AllocateTensor(this->data_type_, dims, 1, dims[0] * sizeof(InputType));
  auto buff = (InputType *)TF_TensorData(this->tensor_);
  PELOTON_MEMCPY(buff, input_for_tf, dims[0] * sizeof(InputType));
}

// 2d vector
TFSEIN_TEMPLATE_ARGUMENTS
TFSEIN_TYPE::TfSessionEntityInput(
    const std::vector<std::vector<InputType>> &input, const std::string &op) {
  this->placeholder_name_ = op;
  this->DetermineDataType();
  int64_t dims[] = {static_cast<int64_t>(input.size()),
                    static_cast<int64_t>(input[0].size())};
  InputType *input_for_tf = Flatten(input);
  this->tensor_ = TF_AllocateTensor(this->data_type_, dims, 2,
                                    dims[0] * dims[1] * sizeof(InputType));
  auto buff = (InputType *)TF_TensorData(this->tensor_);
  PELOTON_MEMCPY(buff, input_for_tf, dims[0] * dims[1] * sizeof(InputType));
}

// raw flattened input
TFSEIN_TEMPLATE_ARGUMENTS
TFSEIN_TYPE::TfSessionEntityInput(InputType *input,
                                  const std::vector<int64_t> &dims,
                                  const std::string &op) {
  this->placeholder_name_ = op;
  this->DetermineDataType();
  InputType *input_for_tf = input;
  int64_t num_elems = 1;
  for (auto elem : dims) {
    num_elems *= elem;
  }
  this->tensor_ = TF_AllocateTensor(this->data_type_, dims.data(), dims.size(),
                                    num_elems * sizeof(InputType));
  auto buff = (InputType *)TF_TensorData(this->tensor_);
  PELOTON_MEMCPY(buff, input_for_tf, num_elems * sizeof(InputType));
}

// Flattens 2d inputs
TFSEIN_TEMPLATE_ARGUMENTS
InputType *TFSEIN_TYPE::Flatten(
    const std::vector<std::vector<InputType>> &elems) {
  std::vector<InputType> flattened;
  for (auto row : elems) {
    for (float elem : row) {
      flattened.push_back(elem);
    }
  }
  return flattened.data();
}

// Explicit template Initialization
template class TfSessionEntityInput<float>;

}  // namespace brain
}  // namespace peloton
