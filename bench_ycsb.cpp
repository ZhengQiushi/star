#include "benchmark/ycsb/Database.h"
#include "core/Coordinator.h"
#include "core/Macros.h"

DEFINE_int32(read_write_ratio, 80, "read write ratio");
DEFINE_int32(read_only_ratio, 0, "read only transaction ratio");
DEFINE_int32(cross_ratio, 30, "cross partition transaction ratio");
DEFINE_int32(keys, 200000, "keys in a partition.");
DEFINE_double(zipf, 0, "skew factor");

DEFINE_int32(nop_prob, 0, "prob of transactions having nop, out of 10000");
DEFINE_int64(n_nop, 0, "total number of nop");

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


int main(int argc, char *argv[]) {
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler(); 
  google::ParseCommandLineFlags(&argc, &argv, true);
  // FLAGS_log_dir = "/Users/lion/project/01_star/star/logs/";
  // FLAGS_alsologtostderr = 1;
  // FLAGS_logtostderr = 0;

  star::ycsb::Context context;
  SETUP_CONTEXT(context);

  context.readWriteRatio = FLAGS_read_write_ratio;
  context.readOnlyTransaction = FLAGS_read_only_ratio;
  context.crossPartitionProbability = FLAGS_cross_ratio;
  context.keysPerPartition = FLAGS_keys;

  context.nop_prob = FLAGS_nop_prob;
  context.n_nop = FLAGS_n_nop;

  if (FLAGS_zipf > 0) {
    context.isUniform = false;
    star::Zipf::globalZipf().init(context.keysPerPartition, FLAGS_zipf);
  }
  DCHECK(context.peers.size() >= 2) << " The size of ip peers must gt 2.(At least one generator, one worker)";
  star::ycsb::Database db;
  db.initialize(context);
  // star::Coordinator c(FLAGS_id, db, context);
  // c.connectToPeers();
  // c.start();

  // test 
  using TransactionType = star::SiloTransaction;
  using WorkloadType =
          typename InferType<star::ycsb::Context>::template WorkloadType<TransactionType>;
  std::atomic<uint32_t> worker_status;
  std::unique_ptr<star::Clay<WorkloadType>> my_clay = std::make_unique<star::Clay<WorkloadType>>(context, db, worker_status);
  

  std::vector<idx_t> parts;
  my_clay->init_with_history("/home/star/data/result_test.xls");
  my_clay->metis_partition_graph(parts);

  // test done 


  google::ShutdownGoogleLogging();

  return 0;
}