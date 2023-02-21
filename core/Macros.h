//
// Created by Yi Lu on 3/18/19.
//

#pragma once

#include "glog/logging.h"
#include <boost/algorithm/string/split.hpp>

DEFINE_string(servers, "127.0.0.1:10010",
              "semicolon-separated list of servers");
DEFINE_int32(id, 0, "coordinator id");
DEFINE_int32(threads, 1, "the number of threads");
DEFINE_int32(io, 1, "the number of i/o threads");
DEFINE_int32(partition_num, 1, "the number of partitions");
DEFINE_string(partitioner, "hash", "database partitioner (hash, hash2, pb)");
DEFINE_bool(sleep_on_retry, false, "sleep when retry aborted transactions");
DEFINE_int32(batch_size, 100, "star or calvin batch size");
DEFINE_int32(group_time, 10, "group commit frequency");
DEFINE_int32(batch_flush, 50, "batch flush");
DEFINE_int32(sleep_time, 1000, "retry sleep time");
DEFINE_string(protocol, "Scar", "transaction protocol");
DEFINE_string(replica_group, "1,3", "calvin replica group"); 

DEFINE_int32(nop_prob, 0, "prob of transactions having nop, out of 10000");
DEFINE_int64(n_nop, 10000, "total number of nop");

// 哪几个coordinator组成一个replica group
// sum() = coordinator_num
// e.g. 2,1  => C0,C1 属于一个replica group, C2单独属于一个
//              每个group自己内部有一个主副本，意味着此时总共有两个主副本...很怪

DEFINE_string(lock_manager, "1,1", "calvin lock manager");
// 每个replica group的lock manager的数量
DEFINE_bool(read_on_replica, false, "read from replicas");
DEFINE_bool(local_validation, false, "local validation");
DEFINE_bool(rts_sync, false, "rts sync");
DEFINE_bool(star_sync, false, "synchronous write in the single-master phase");
DEFINE_bool(star_dynamic_batch_size, true, "dynamic batch size");
DEFINE_bool(plv, true, "parallel locking and validation");
DEFINE_bool(calvin_same_batch, false, "always run the same batch of txns.");
DEFINE_bool(kiva_read_only, true, "kiva read only optimization");
DEFINE_bool(kiva_reordering, true, "kiva reordering optimization");
DEFINE_bool(kiva_si, false, "kiva snapshot isolation");
DEFINE_int32(delay, 0, "delay time in us.");
DEFINE_string(cdf_path, "", "path to cdf");
DEFINE_string(log_path, "", "path to disk logging.");
DEFINE_bool(tcp_no_delay, true, "TCP Nagle algorithm, true: disable nagle");
DEFINE_bool(tcp_quick_ack, false, "TCP quick ack mode, true: enable quick ack");
DEFINE_bool(cpu_affinity, true, "pinning each thread to a separate core");
DEFINE_bool(enable_data_transfer, false, "enable data transfer or not");

DEFINE_bool(lion_no_switch, false, "");
DEFINE_int32(lion_with_metis_init, 0, "use metis to initialize");

DEFINE_string(data_src_path_dir, "/home/star/data/", "data-source");

DEFINE_int32(migration_only, 0, "migrate only");
DEFINE_int32(random_router, 0, "random transfer");
DEFINE_bool(lion_with_trace_log, false, "use metis to initialize");

DEFINE_int32(data_transform_interval, 5, "");

DEFINE_int32(time_to_run, 60, "running time");
DEFINE_int32(workload_time, 30, "workload switch");
DEFINE_int32(init_time, 0, "running time");
DEFINE_int32(sample_time_interval, 1, "running time");

DEFINE_int32(cpu_core_id, 0, "cpu core id");


DEFINE_int32(skew_factor, 0, "workload skew factor");


#define SETUP_CONTEXT(context)                                                 \
  boost::algorithm::split(context.peers, FLAGS_servers,                        \
                          boost::is_any_of(";"));                              \
  context.coordinator_num = context.peers.size() - 1;                          \
  context.coordinator_id = FLAGS_id;                                           \
  context.worker_num = FLAGS_threads;                                          \
  context.io_thread_num = FLAGS_io;                                            \
  context.partition_num = context.coordinator_num * FLAGS_partition_num;       \
  context.partitioner = FLAGS_partitioner;                                     \
  context.sleep_on_retry = FLAGS_sleep_on_retry;                               \
  context.batch_size = FLAGS_batch_size;                                       \
  context.group_time = FLAGS_group_time;                                       \
  context.batch_flush = FLAGS_batch_flush;                                     \
  context.sleep_time = FLAGS_sleep_time;                                       \
  context.protocol = FLAGS_protocol;                                           \
  context.replica_group = FLAGS_replica_group;                                 \
  context.lock_manager = FLAGS_lock_manager;                                   \
  context.read_on_replica = FLAGS_read_on_replica;                             \
  context.local_validation = FLAGS_local_validation;                           \
  context.rts_sync = FLAGS_rts_sync;                                           \
  context.star_sync_in_single_master_phase = FLAGS_star_sync;                  \
  context.star_dynamic_batch_size = FLAGS_star_dynamic_batch_size;             \
  context.parallel_locking_and_validation = FLAGS_plv;                         \
  context.calvin_same_batch = FLAGS_calvin_same_batch;                         \
  context.kiva_read_only_optmization = FLAGS_kiva_read_only;                   \
  context.kiva_reordering_optmization = FLAGS_kiva_reordering;                 \
  context.kiva_snapshot_isolation = FLAGS_kiva_si;                             \
  context.delay_time = FLAGS_delay;                                            \
  context.log_path = FLAGS_log_path;                                           \
  context.cdf_path = FLAGS_cdf_path;                                           \
  context.tcp_no_delay = FLAGS_tcp_no_delay;                                   \
  context.tcp_quick_ack = FLAGS_tcp_quick_ack;                                 \
  context.cpu_affinity = FLAGS_cpu_affinity;                                   \
  context.enable_data_transfer = FLAGS_enable_data_transfer;                   \
  context.migration_only = FLAGS_migration_only;                               \
  context.nop_prob = FLAGS_nop_prob;                                           \
  context.n_nop = FLAGS_n_nop;                                                 \
  context.time_to_run = FLAGS_time_to_run;                                     \
  context.workload_time = FLAGS_workload_time;                                 \
  context.init_time = FLAGS_init_time;                                         \
  context.sample_time_interval = FLAGS_sample_time_interval;                   \
  context.data_transform_interval = FLAGS_data_transform_interval;             \
  context.lion_no_switch = FLAGS_lion_no_switch;                               \
  context.lion_with_metis_init = FLAGS_lion_with_metis_init;                   \
  context.data_src_path_dir = FLAGS_data_src_path_dir;                         \
  context.random_router = FLAGS_random_router;                                 \
  context.lion_with_trace_log = FLAGS_lion_with_trace_log;                     \
  context.cpu_core_id = FLAGS_cpu_core_id;                                     \
  context.skew_factor = FLAGS_skew_factor;                                     \
  context.set_star_partitioner();
