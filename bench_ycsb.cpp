#include "benchmark/ycsb/Database.h"
#include "core/Coordinator.h"
#include "core/Macros.h"

DEFINE_bool(lotus_sp_parallel_exec_commit, false, "parallel execution and commit for Lotus");
DEFINE_int32(read_write_ratio, 80, "read write ratio");
DEFINE_int32(read_only_ratio, 0, "read only transaction ratio");
DEFINE_int32(cross_ratio, 0, "cross partition transaction ratio");
DEFINE_int32(keys, 200000, "keys in a partition.");
DEFINE_double(zipf, 0, "skew factor");
DEFINE_int32(cross_part_num, 2, "Cross-partition partion #");


// ./main --logtostderr=1 --id=1 --servers="127.0.0.1:10010;127.0.0.1:10011"
// cmake -DCMAKE_BUILD_TYPE=Release 



// test 
#include "common/MyMove.h"
#include "core/Defs.h"
#include <vector>
#include <metis.h>

template <class Context> class InferType {};

template <> class InferType<star::tpcc::Context> {
public:
  template <class Transaction>
  using WorkloadType = star::tpcc::Workload<Transaction>;

  // using KeyType = tpcc::tpcc::key;
  // using ValueType = tpcc::tpcc::value;
  // using KeyType = ycsb::ycsb::key;
  // using ValueType = ycsb::ycsb::value;
};

template <> class InferType<star::ycsb::Context> {
public:
  template <class Transaction>
  using WorkloadType = star::ycsb::Workload<Transaction>;

  // using KeyType = ycsb::ycsb::key;
  // using ValueType = ycsb::ycsb::value;
};

bool do_tid_check = false;


int main(int argc, char *argv[]) {
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler(); 
  google::ParseCommandLineFlags(&argc, &argv, true);

  star::ycsb::Context context;
  SETUP_CONTEXT(context);

  context.readWriteRatio = FLAGS_read_write_ratio;
  context.readOnlyTransaction = FLAGS_read_only_ratio;
  context.crossPartitionProbability = FLAGS_cross_ratio;
  context.keysPerPartition = FLAGS_keys;
  context.lotus_sp_parallel_exec_commit = FLAGS_lotus_sp_parallel_exec_commit;
  context.crossPartitionPartNum = FLAGS_cross_part_num;
  context.nop_prob = FLAGS_nop_prob;
  context.n_nop = FLAGS_n_nop;

  context.granules_per_partition = FLAGS_granule_count;
  context.keysPerGranule = context.keysPerPartition / context.granules_per_partition;

  LOG(INFO) << "checkpoint " << context.lotus_checkpoint << " to " << context.lotus_checkpoint_location;
  LOG(INFO) << "cross_part_num " << FLAGS_cross_part_num;
  LOG(INFO) << "lotus_sp_parallel_exec_commit " << FLAGS_lotus_sp_parallel_exec_commit;
  LOG(INFO) << "granules_per_partition " << context.granules_per_partition;
  LOG(INFO) << "keysPerGranule " << context.keysPerGranule;
  
  if (FLAGS_zipf > 0) {
    context.isUniform = false;
    star::Zipf::globalZipf().init(context.keysPerPartition, FLAGS_zipf);
  }
  DCHECK(context.peers.size() >= 2) << " The size of ip peers must gt 2.(At least one generator, one worker)";
  star::ycsb::Database db;
  db.initialize(context);
  star::Coordinator c(FLAGS_id, db, context);
  c.connectToPeers();
  c.start();

  google::ShutdownGoogleLogging();

  return 0;
}