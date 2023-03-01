#include "benchmark/ycsb/Database.h"
#include "core/Coordinator.h"
#include "core/Macros.h"

DEFINE_int32(read_write_ratio, 80, "read write ratio");
DEFINE_int32(read_only_ratio, 0, "read only transaction ratio");
DEFINE_int32(cross_ratio, 30, "cross partition transaction ratio");
DEFINE_int32(keys, 200000, "keys in a partition.");
DEFINE_double(zipf, 0, "skew factor");



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
  
  // LOG(INFO) << "start";
  // // my_clay->init_with_history("/home/star/data/resultss.xls", 0, 30 - 1);
  // my_clay->init_with_history(context.data_src_path_dir + "resultss.xls", 0, 30 - 1);
  // LOG(INFO) << "history init done";
  // // my_clay->metis_partition_graph("/home/star/data/resultss_partition_0_30.xls");
  // my_clay->my_find_clump(context.data_src_path_dir + "resultss_partition_0_30.xls");
  // LOG(INFO) << "done";
  // my_clay->clear_graph();



////
  // LOG(INFO) << "start";
  // LOG(INFO) << "history init done";
  // my_clay->init_with_history(context.data_src_path_dir + "resultss.xls", 0, 30);
  // // my_clay->metis_partition_graph("/home/star/data/resultss_partition_30_60.xls");
  // // my_clay->my_find_clump(context.data_src_path_dir + "test_resultss_partition_30_60.xls");
  // my_clay->find_clump();
  // my_clay->implement_clump();

  // LOG(INFO) << "first round done";
  // my_clay->clear_graph();
  // my_clay->init_with_history(context.data_src_path_dir + "resultss.xls", 0, 30);
  // // my_clay->metis_partition_graph("/home/star/data/resultss_partition_30_60.xls");
  // // my_clay->my_find_clump(context.data_src_path_dir + "test_resultss_partition_30_60.xls");
  // my_clay->find_clump();
  // my_clay->implement_clump();

  // my_clay->save_clay_moves(context.data_src_path_dir + "clay_resultss_partition_0_30.xls");
////

  std::string src_file = "resultss.xls";
  std::string dst_file = "clay_resultss_partition_0_30.xls"; 
  // my_clay->run_clay_offline(src_file, dst_file, 0, 30);
  my_clay->my_run_offline(src_file, dst_file, 0, 30);

  // dst_file = "clay_resultss_partition_30_60.xls"; 
  // my_clay->run_clay_offline(src_file, dst_file, 30, 60);

  // dst_file = "clay_resultss_partition_60_90.xls"; 
  // my_clay->run_clay_offline(src_file, dst_file, 60, 90);

  // dst_file = "clay_resultss_partition_90_120.xls"; 
  // my_clay->run_clay_offline(src_file, dst_file, 90, 120);



  // // LOG(INFO) << "start";
  // LOG(INFO) << "history init done";
  // my_clay->init_with_history(context.data_src_path_dir + "resultss.xls", 30, 60 - 1);
  // // my_clay->metis_partition_graph("/home/star/data/resultss_partition_30_60.xls");
  // my_clay->my_find_clump(context.data_src_path_dir + "resultss_partition_30_60.xls");
  // LOG(INFO) << "done";
  // my_clay->clear_graph();


  // // LOG(INFO) << "history init done";
  // // my_clay->init_with_history("/home/star/data/resultss.xls", 0, 60 - 1);
  // // // my_clay->metis_partition_graph("/home/star/data/resultss_partition_30_60.xls");
  // // my_clay->my_find_clump("/home/star/data/resultss_partition_0_60_.xls");
  // // LOG(INFO) << "done";
  // // my_clay->clear_graph();


  // LOG(INFO) << "start";
  // my_clay->init_with_history(context.data_src_path_dir + "resultss.xls", 60, 90 - 1);
  // LOG(INFO) << "history init done";
  // // my_clay->metis_partition_graph("/home/star/data/resultss_partition_60_90.xls");
  // my_clay->my_find_clump(context.data_src_path_dir + "resultss_partition_60_90.xls");
  // LOG(INFO) << "done";
  // my_clay->clear_graph();


  // LOG(INFO) << "start";
  // my_clay->init_with_history(context.data_src_path_dir + "resultss.xls", 90, 120 - 1);
  // LOG(INFO) << "history init done";
  // // my_clay->metis_partition_graph("/home/star/data/resultss_partition_90_120.xls");
  // my_clay->my_find_clump(context.data_src_path_dir + "resultss_partition_90_120.xls");
  // LOG(INFO) << "done";
  // my_clay->clear_graph();

  // LOG(INFO) << "start";
  // my_clay->init_with_history("/home/star/data/resultss.xls", 40, 60 - 1);
  // LOG(INFO) << "history init done";
  // my_clay->metis_partition_graph("/home/star/data/resultss_partition_40_60.xls");
  // LOG(INFO) << "done";

  // LOG(INFO) << "start read from file";
  // my_clay->metis_partiion_read_from_file("/home/star/data/resultss_partition_20_40.xls");
  // LOG(INFO) << "read from file done";

  // LOG(INFO) << "start read from file";
  // my_clay->metis_partiion_read_from_file("/home/star/data/resultss_partition_40_60.xls");
  // LOG(INFO) << "read from file done";

  // LOG(INFO) << "start read from file";
  // my_clay->metis_partiion_read_from_file("/home/star/data/resultss_partition_0_20.xls");
  // LOG(INFO) << "read from file done";

  // test done 


  google::ShutdownGoogleLogging();

  return 0;
}