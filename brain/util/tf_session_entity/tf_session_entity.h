
#pragma once

#include "brain/util/tf_session_entity/tf_session_entity.h"
#include "brain/util/tf_session_entity/tf_session_entity_input.h"
#include "brain/util/tf_session_entity/tf_session_entity_output.h"



#include "tf_session_entity_io.h"

#define TFSE_TEMPLATE_ARGUMENTS \
  template <typename InputType, typename OutputType>
#define TFSE_TYPE TfSessionEntity<InputType, OutputType>

namespace LionBrain {
namespace brain {

// Forward Declarations
template <class InputType>
class TfSessionEntityInput;
template <class OutputType>
class TfSessionEntityOutput;

/**
 * The `TfSessionEntity` class is the main entity class for
 * handling Tensorflow sessions and evaluations within.
 * Since creating a graph in Tensorflow C API is pretty challenging
 * it is recommended to generally create a graph in a more stable API(eg.
 * python)
 * and import its serialized(.pb) version for use here.
 * This class supports and makes the following simplified:
 * (1) Session initialization
 * (2) Graph Imports: Using `ImportGraph`
 * (3) Evaluation of Ops: using different versions of the `Eval` fn.
 * The inputs and outputs are simplified by using the wrapper classes
 * `TfSessionEntityInput`, `TfSessionEntityOutput`.
 * (4) Misc Helper operations
 */
TFSE_TEMPLATE_ARGUMENTS
class TfSessionEntity {
 public:
  TfSessionEntity();
  ~TfSessionEntity();

  // Graph Import
  void ImportGraph(const std::string &filename);

  /**
   * Evaluation/Session.Run() options.
   * Provides a set of options to evaluate an Op either with
   * 1. No input/outputs eg. Global variable initialization
   * 2. Inputs, but no outputs eg. Backpropagation
   * 3. Inputs and 1 Output eg. Loss calculation, Predictions.
   * 3 with multiple outputs is easy to setup and can be done if need arises
   * based on (3) itself
   */
  void Eval(const std::string &opName);
  void Eval(const std::vector<TfSessionEntityInput<InputType> *> &helper_inputs,
            const std::string &op_name);
  OutputType *Eval(
      const std::vector<TfSessionEntityInput<InputType> *> &helper_inputs,
      TfSessionEntityOutput<OutputType> *helper_outputs);

  /** Helpers **/
  // Print the name of all operations(`ops`) in this graph
  void PrintOperations();
  /**
   * Checking if the status is ok - Tensorflow C is horrible in showing there's
   * an error
   * uptil the eval statement - adding status checks can help find in which step
   * the error
   * has occurred.
   */
  bool IsStatusOk();

 private:
  /**
   * Member variables to maintain state of the Session entity object
   * These variables are used throughout by functions of this class
   */
  TF_Graph *graph_;
  TF_Status *status_;
  TF_SessionOptions *session_options_;
  TF_Session *session_;

  // For Graph IO handling
  TF_Buffer *ReadFile(const std::string &filename);
  static void FreeBuffer(void *data, size_t length);
};

/**
 * Constructor/Desctructor
 **/

TFSE_TEMPLATE_ARGUMENTS
TFSE_TYPE::TfSessionEntity() {
  graph_ = TF_NewGraph();
  status_ = TF_NewStatus();
  session_options_ = TF_NewSessionOptions();
  session_ = TF_NewSession(graph_, session_options_, status_);
}

// TODO(saatvik): Valgrind will show errors due to a standing issue within
// Tensorflow https://github.com/tensorflow/tensorflow/issues/17739
TFSE_TEMPLATE_ARGUMENTS
TFSE_TYPE::~TfSessionEntity() {
  TF_CloseSession(session_, status_);
  TF_DeleteSession(session_, status_);
  TF_DeleteGraph(graph_);
  TF_DeleteStatus(status_);
  TF_DeleteSessionOptions(session_options_);
}

/*
 ********
 * Graph Import Utilities
 ********
 */

TFSE_TEMPLATE_ARGUMENTS
void TFSE_TYPE::FreeBuffer(void *data, UNUSED_ATTRIBUTE size_t length) {
  ::operator delete(data);
}

TFSE_TEMPLATE_ARGUMENTS
void TFSE_TYPE::ImportGraph(const std::string &filename) {
  TF_Buffer *graph_def = ReadFile(filename);
  TF_ImportGraphDefOptions *opts = TF_NewImportGraphDefOptions();
  TF_GraphImportGraphDef(graph_, graph_def, opts, status_);
  TF_DeleteImportGraphDefOptions(opts);
  TF_DeleteBuffer(graph_def);
  PELOTON_ASSERT(IsStatusOk());
}

TFSE_TEMPLATE_ARGUMENTS
TF_Buffer *TFSE_TYPE::ReadFile(const std::string &filename) {
  FILE *f = fopen(filename.c_str(), "rb");
  fseek(f, 0, SEEK_END);
  size_t fsize = (size_t)ftell(f);
  fseek(f, 0, SEEK_SET);  // same as rewind(f);
  // Reference:
  // https://stackoverflow.com/questions/14111900/using-new-on-void-pointer
  void *data = ::operator new(fsize);
  UNUSED_ATTRIBUTE size_t size_read = fread(data, fsize, 1, f);
  fclose(f);

  TF_Buffer *buf = TF_NewBuffer();
  buf->data = data;
  buf->length = fsize;
  buf->data_deallocator = TfSessionEntity::FreeBuffer;
  return buf;
}

/*
 ********
 * Evaluation/Session.Run()
 ********
 */

// Evaluate op with no inputs/outputs
TFSE_TEMPLATE_ARGUMENTS
void TFSE_TYPE::Eval(const std::string &opName) {
  TF_Operation *op = TF_GraphOperationByName(graph_, opName.c_str());
  TF_SessionRun(session_, nullptr, nullptr, nullptr, 0,  // inputs
                nullptr, nullptr, 0,                     // outputs
                &op, 1,                                  // targets
                nullptr, status_);
}

// Evaluate op with inputs and output
TFSE_TEMPLATE_ARGUMENTS
OutputType *TFSE_TYPE::Eval(
    const std::vector<TfSessionEntityInput<InputType> *> &helper_inputs,
    TfSessionEntityOutput<OutputType> *helper_output) {
  std::vector<TF_Tensor *> in_vals;
  std::vector<TF_Output> ins;
  for (auto helperIn : helper_inputs) {
    ins.push_back({TF_GraphOperationByName(
                       graph_, helperIn->GetPlaceholderName().c_str()),
                   0});
    in_vals.push_back(helperIn->GetTensor());
  }
  TF_Tensor *tensor_val = helper_output->GetTensor();
  TF_Tensor **tensor_loc = &helper_output->GetTensor();
  TF_Output out = {TF_GraphOperationByName(
                       graph_, helper_output->GetPlaceholderName().c_str()),
                   0};
  TF_SessionRun(session_, nullptr, &(ins.at(0)), &(in_vals.at(0)),
                ins.size(),           // Inputs
                &out, tensor_loc, 1,  // Outputs
                nullptr, 0,           // Operations
                nullptr, status_);
  PELOTON_ASSERT(TF_GetCode(status_) == TF_OK);
  // For some smart reason TF chooses to make 'tensor_loc' point
  // to a new location with actual output tensors instead of using the
  // already allocated tensors
  TF_DeleteTensor(tensor_val);
  return static_cast<OutputType *>(TF_TensorData(helper_output->GetTensor()));
}

// Evaluate op with only inputs(where nothing is output eg. Backprop)
TFSE_TEMPLATE_ARGUMENTS
void TFSE_TYPE::Eval(
    const std::vector<TfSessionEntityInput<InputType> *> &helper_inputs,
    const std::string &op_name) {
  std::vector<TF_Tensor *> in_vals;
  std::vector<TF_Output> ins;
  for (auto helperIn : helper_inputs) {
    ins.push_back({TF_GraphOperationByName(
                       graph_, helperIn->GetPlaceholderName().c_str()),
                   0});
    in_vals.push_back(helperIn->GetTensor());
  }
  TF_Operation *op = TF_GraphOperationByName(graph_, op_name.c_str());
  TF_SessionRun(session_, nullptr, &(ins.at(0)), &(in_vals.at(0)),
                ins.size(),           // Inputs
                nullptr, nullptr, 0,  // Outputs
                &op, 1,               // Operations
                nullptr, status_);
  PELOTON_ASSERT(TF_GetCode(status_) == TF_OK);
}

/*
 ********
 * Helper Operations
 ********
 */

TFSE_TEMPLATE_ARGUMENTS
void TFSE_TYPE::PrintOperations() {
  TF_Operation *oper;
  size_t pos = 0;
  std::string graph_ops = "Graph Operations List:";
  while ((oper = TF_GraphNextOperation(graph_, &pos)) != nullptr) {
    graph_ops += TF_OperationName(oper);
    graph_ops += "\n";
  }
  // LOG_DEBUG("%s", graph_ops.c_str());
}

TFSE_TEMPLATE_ARGUMENTS
bool TFSE_TYPE::IsStatusOk() { return TF_GetCode(status_) == TF_OK; }

// Explicit template Initialization
template class TfSessionEntity<float, float>;
}  // namespace brain
}  // namespace LionBrain
