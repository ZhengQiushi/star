//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "common/LockfreeQueue.h"
#include "common/Message.h"
#include "common/Percentile.h"
#include <atomic>
#include <glog/logging.h>
#include <queue>

namespace star {

class Worker {
public:
  Worker(std::size_t coordinator_id, std::size_t id)
      : coordinator_id(coordinator_id), id(id) {
    n_commit.store(0);
    n_remaster.store(0);
    n_migrate.store(0);
    n_abort_no_retry.store(0);
    n_abort_lock.store(0);
    n_abort_read_validation.store(0);
    n_local.store(0);
    n_si_in_serializable.store(0);
    n_network_size.store(0);
    clear_status.store(0);
    distributed_num.store(0);
    singled_num.store(0);
    
  }

  virtual ~Worker() = default;

  virtual void start() = 0;

  virtual void onExit() {}

  virtual void push_message(Message *message) = 0;

  virtual Message *pop_message() = 0;

  virtual void delay_push_message(Message *message){
    
  };
  virtual Message * delay_pop_message(){
    return nullptr;
  };
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

  void clear_time_status(){
    txn_statics.clear();
  }
public:
  std::size_t coordinator_id;
  std::size_t id;
  std::atomic<uint64_t> n_commit, 
      n_remaster, n_migrate, // 
      n_abort_no_retry, n_abort_lock,
      n_remaster_abort,
      n_abort_read_validation, n_local, n_si_in_serializable, n_network_size;

  int workload_type; // 0-5
  std::chrono::steady_clock::time_point start_time;

  std::atomic<uint64_t> distributed_num, singled_num;

  std::atomic<int> clear_status;

  

  // Percentile<int64_t> time_router;
  // Percentile<int64_t> time_scheuler;
  // Percentile<int64_t> time_local_locks;
  // Percentile<int64_t> time_remote_locks;
  // Percentile<int64_t> time_execute;
  // Percentile<int64_t> time_commit;
  // Percentile<int64_t> time_wait4serivce;
  // Percentile<int64_t> time_other_module;

  // Percentile<int64_t> time_total;
  
  Percentile<Breakdown> txn_statics;
  Percentile<int64_t> total_latency;

  Percentile<int64_t> router_percentile, execute_percentile, commit_percentile;
  Percentile<int64_t> analyze_percentile, execute_latency;
};

} // namespace star
