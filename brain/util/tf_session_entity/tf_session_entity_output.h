
#pragma once

#include "brain/util/tf_session_entity/tf_session_entity_output.h"



#include "tf_session_entity_io.h"

#define TFSEOUT_TEMPLATE_ARGUMENTS template <typename OutputType>
#define TFSEOUT_TYPE TfSessionEntityOutput<OutputType>

namespace LionBrain {
namespace brain {

/**
 * TfSessionEntityOutput is a wrapper class to make explaining output
 * expectations to TfSessionEntity much simpler.
 * Firstly, it needs the output opName so the TF-C API knows which
 * node to find the output in. It optionally accepts
 * a vector of expected dimensions of output to construct a corresponding
 * Tensor node to hold that output.
 * It minimized user inputs and attempts to maximize the work
 * of filling in the blanks for the TF-C API function calls
 * The TfSessionEntity::Eval requires a std::vector of TfSessionEntityOutput
 * If an output is not expected then this should not be passed.
 */
TFSEOUT_TEMPLATE_ARGUMENTS
class TfSessionEntityOutput : public TfSessionEntityIOBase<OutputType> {
 public:
  /**
   * Output wrappers for the session entity eval fn.
   * Output may be a scalar const in which case only an opname would be needed
   * Alternatively it might be a multidimensional output in which case the
   * constructor
   * with `dims` can be used.
   */
  // const output
  explicit TfSessionEntityOutput(const std::string &op);
  // n-d output
  explicit TfSessionEntityOutput(const std::vector<int64_t> &dims,
                                 const std::string &op);
};


TFSEOUT_TEMPLATE_ARGUMENTS
TFSEOUT_TYPE::TfSessionEntityOutput(const std::string &op) {
  this->placeholder_name_ = op;
  this->DetermineDataType();
  this->tensor_ =
      TF_AllocateTensor(this->data_type_, nullptr, 0, sizeof(OutputType));
}

TFSEOUT_TEMPLATE_ARGUMENTS
TFSEOUT_TYPE::TfSessionEntityOutput(const std::vector<int64_t> &dims,
                                    const std::string &op) {
  this->placeholder_name_ = op;
  this->DetermineDataType();
  int64_t num_elems = 1;
  for (auto elem : dims) {
    num_elems *= elem;
  }
  this->tensor_ = TF_AllocateTensor(this->data_type_, dims.data(), dims.size(),
                                    sizeof(OutputType) * num_elems);
}

// Explicit template Initialization
template class TfSessionEntityOutput<float>;
}  // namespace brain
}  // namespace LionBrain
