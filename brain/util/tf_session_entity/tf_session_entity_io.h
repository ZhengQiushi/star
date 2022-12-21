//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tf_session_entity_io.cpp
//
// Identification: src/brain/util/tf_session_entity/tf_session_entity_io.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include "brain/util/tf_session_entity/tf_session_entity_io.h"

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tf_session_entity_io.h
//
// Identification:
// src/include/brain/util/tf_session_entity/tf_session_entity_io.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <tensorflow/c/c_api.h>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <type_traits>
#include <vector>
#include "common/macros.h"

#define TFSEIO_BASE_TEMPLATE_ARGUMENTS template <typename N>
#define TFSEIO_BASE_TYPE TfSessionEntityIOBase<N>

namespace peloton {
namespace brain {

/**
 * The `TfSessionEntityIOBase` is a base class
 * containing common member variables/functions
 * for the Input/Output wrapper classes to inherit and use.
 * Thus the member variables have been marked as `protected`.
 */
TFSEIO_BASE_TEMPLATE_ARGUMENTS
class TfSessionEntityIOBase {
 public:
  std::string GetPlaceholderName();
  TF_Tensor *&GetTensor();
  ~TfSessionEntityIOBase();

 protected:
  // name of io placeholder
  std::string placeholder_name_;
  // data type of io
  TF_DataType data_type_;
  // tensor holding/allocated for data
  TF_Tensor *tensor_;
  // fn to autodetermine the TF_ data type
  void DetermineDataType();
};


TFSEIO_BASE_TEMPLATE_ARGUMENTS
TFSEIO_BASE_TYPE::~TfSessionEntityIOBase() { TF_DeleteTensor(this->tensor_); }

TFSEIO_BASE_TEMPLATE_ARGUMENTS
void TFSEIO_BASE_TYPE::DetermineDataType() {
  if (std::is_same<N, int64_t>::value) {
    data_type_ = TF_INT64;
  } else if (std::is_same<N, int32_t>::value) {
    data_type_ = TF_INT32;
  } else if (std::is_same<N, int16_t>::value) {
    data_type_ = TF_INT16;
  } else if (std::is_same<N, int8_t>::value) {
    data_type_ = TF_INT8;
  } else if (std::is_same<N, int>::value) {
    data_type_ = TF_INT32;
  } else if (std::is_same<N, float>::value) {
    data_type_ = TF_FLOAT;
  } else if (std::is_same<N, double>::value) {
    data_type_ = TF_DOUBLE;
  }
}

TFSEIO_BASE_TEMPLATE_ARGUMENTS
std::string TFSEIO_BASE_TYPE::GetPlaceholderName() { return placeholder_name_; }

TFSEIO_BASE_TEMPLATE_ARGUMENTS
TF_Tensor *&TFSEIO_BASE_TYPE::GetTensor() { return tensor_; }

// Explicit template Initialization
template class TfSessionEntityIOBase<float>;

}  // namespace brain
}  // namespace peloton
