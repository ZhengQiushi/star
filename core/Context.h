//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include <cstddef>
#include <string>
#include <vector>

namespace star {
class Context {

public:
  void set_star_partitioner() {
    if (protocol != "Star") {
      return;
    }
    if (coordinator_id == 0) {
      partitioner = "StarS";
    } else {
      partitioner = "StarC";
    }
  }

public:
  std::size_t coordinator_id = 0;
  std::size_t partition_num = 0; // partitions in total
  std::size_t worker_num = 0;
  std::size_t coordinator_num = 0;
  std::size_t io_thread_num = 1;
  std::string protocol;
  std::string repartition_strategy;
  std::string replica_group;
  std::string lock_manager;
  std::size_t batch_size = 240; // star, calvin, dbx batch size
  std::size_t batch_flush = 10;
  std::size_t group_time = 40; // ms
  std::size_t sleep_time = 50; // us
  std::string partitioner;
  std::size_t delay_time = 0;
  std::string log_path;
  std::string cdf_path;
  std::size_t cpu_core_id = 0;
  std::size_t skew_factor = 0;
  std::size_t time_to_run = 25;
  std::size_t workload_time = 30;
  std::size_t init_time = 0;
  std::size_t sample_time_interval = 2;
                
  std::size_t data_transform_interval = 5;
  
  std::size_t lion_with_metis_init = 0;
  std::string data_src_path_dir = "/home/star/data/";
  std::size_t random_router = 0;
  std::size_t migration_only = 0;

  std::size_t nop_prob = 0; // out of 10000
  std::size_t n_nop = 10000;

  bool tcp_no_delay = true;
  bool tcp_quick_ack = false;
  bool enable_data_transfer = false;
  bool lion_no_switch = false;
  
  bool lion_with_trace_log = false;
  bool replica_sync = false;
  
  bool cpu_affinity = true;

  bool sleep_on_retry = false;

  bool read_on_replica = false;
  bool lion_self_remaster = false;
  bool local_validation = false;
  bool rts_sync = false;
  bool star_sync_in_single_master_phase = false;
  bool star_dynamic_batch_size = true;
  bool parallel_locking_and_validation = true;

  bool calvin_same_batch = false;

  bool kiva_read_only_optmization = true;
  bool kiva_reordering_optmization = true;
  bool kiva_snapshot_isolation = false;
  bool operation_replication = false;

  bool aria_read_only_optmization = true;
  bool aria_reordering_optmization = true;
  bool aria_snapshot_isolation = false;
  
  std::vector<std::string> peers;
};
} // namespace star
