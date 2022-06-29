//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "common/LockfreeQueue.h"
#include "common/Message.h"
#include <atomic>
#include <glog/logging.h>
#include <queue>

namespace star {

class Worker {
public:
  Worker(std::size_t coordinator_id, std::size_t id)
      : coordinator_id(coordinator_id), id(id) {
    n_commit.store(0);
    n_abort_no_retry.store(0);
    n_abort_lock.store(0);
    n_abort_read_validation.store(0);
    n_local.store(0);
    n_si_in_serializable.store(0);
    n_network_size.store(0);
  }

  virtual ~Worker() = default;

  virtual void start() = 0;

  virtual void onExit() {}

  virtual void push_message(Message *message) = 0;

  virtual Message *pop_message() = 0;

  ExecutorStatus merge_value_to_signal(uint32_t value, ExecutorStatus signal) {
    // the value is put into the most significant 24 bits
    uint32_t offset = 8;
    return static_cast<ExecutorStatus>((value << offset) |
                                       static_cast<uint32_t>(signal));
  }

  std::tuple<uint32_t, ExecutorStatus> split_signal(ExecutorStatus signal) {
    // the value is put into the most significant 24 bits
    uint32_t offset = 8, mask = 0xff;
    uint32_t value = static_cast<uint32_t>(signal);
    // return value and ``real" signal
    return std::make_tuple(value >> offset,
                           static_cast<ExecutorStatus>(value & mask));
  }
  ExecutorStatus signal_unmask(ExecutorStatus signal){
    uint32_t offset = 8, mask = 0xff;
    uint32_t value = static_cast<uint32_t>(signal);
    // return value and ``real" signal
    return static_cast<ExecutorStatus>(value & mask);
  }
public:
  std::size_t coordinator_id;
  std::size_t id;
  std::atomic<uint64_t> n_commit, n_abort_no_retry, n_abort_lock,
      n_abort_read_validation, n_local, n_si_in_serializable, n_network_size;

  int workload_type; // 0-5
};

} // namespace star
