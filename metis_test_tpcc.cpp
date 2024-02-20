#include "benchmark/tpcc/Database.h"
#include "core/Coordinator.h"
#include "core/Macros.h"
#include "brain/workload_cluster.h"

DEFINE_bool(operation_replication, false, "use operation replication");
DEFINE_string(query, "neworder", "tpcc query, mixed, neworder, payment");
DEFINE_int32(neworder_dist, 10, "new order distributed.");
DEFINE_int32(payment_dist, 15, "payment distributed.");

// ./main --logtostderr=1 --id=1 --servers="127.0.0.1:10010;127.0.0.1:10011"
// cmake -DCMAKE_BUILD_TYPE=Release
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

int main(int argc, char *argv[]) {

  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  google::ParseCommandLineFlags(&argc, &argv, true);
  star::tpcc::Context context;
  SETUP_CONTEXT(context);

  context.operation_replication = FLAGS_operation_replication;

  if (FLAGS_query == "mixed") {
    context.workloadType = star::tpcc::TPCCWorkloadType::MIXED;
  } else if (FLAGS_query == "neworder") {
    context.workloadType = star::tpcc::TPCCWorkloadType::NEW_ORDER_ONLY;
  } else if (FLAGS_query == "payment") {
    context.workloadType = star::tpcc::TPCCWorkloadType::PAYMENT_ONLY;
  } else {
    CHECK(false);
  }

  context.newOrderCrossPartitionProbability = FLAGS_neworder_dist;
  context.paymentCrossPartitionProbability = FLAGS_payment_dist;

  DCHECK(context.peers.size() >= 2) << " The size of ip peers must gt 2.(At least one generator, one worker)";
  star::tpcc::Database db;
  db.initialize(context);
  // star::Coordinator c(FLAGS_id, db, context);
  // c.connectToPeers();
  // c.start();

  // test 
  using TransactionType = star::SiloTransaction;
  using WorkloadType =
          typename InferType<star::tpcc::Context>::template WorkloadType<TransactionType>;
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
  std::string dst_file = "resultss_partition_30_60.xls"; 

  if(context.protocol == "MyClay"){
    LOG(INFO) << "MyClay";
    dst_file = "clay_resultss_partition_0_30.xls"; 
  }


  if(context.protocol == "Lion"){
    dst_file = "resultss_partition_0_30.xls"; 
    int time_start = 0;
    int time_end   = time_start + context.workload_time;
    my_clay->my_run_offline(src_file, dst_file, time_start, time_end);

    time_start += context.workload_time; 
    time_end += context.workload_time;

    dst_file = "resultss_partition_30_60.xls"; 
    my_clay->my_run_offline(src_file, dst_file, time_start + 10, time_end);

    time_start += context.workload_time; 
    time_end += context.workload_time;

    dst_file = "resultss_partition_60_90.xls"; 
    my_clay->my_run_offline(src_file, dst_file, time_start + 10, time_end);

    time_start += context.workload_time; 
    time_end += context.workload_time;
    
    dst_file = "resultss_partition_90_120.xls"; 
    my_clay->my_run_offline(src_file, dst_file, time_start + 10, time_end);
  } else {
    std::string src_file = "resultss.xls";
    std::string dst_file = "clay_resultss_partition_0_30.xls"; 
    int time_start = 0;
    int time_end   = time_start + context.workload_time;

    my_clay->run_clay_offline(src_file, dst_file, time_start, time_end);

    time_start += context.workload_time; 
    time_end += context.workload_time;

    dst_file = "clay_resultss_partition_30_60.xls"; 
    my_clay->run_clay_offline(src_file, dst_file, time_start + 10, time_end);

    time_start += context.workload_time; 
    time_end += context.workload_time;

    dst_file = "clay_resultss_partition_60_90.xls"; 
    my_clay->run_clay_offline(src_file, dst_file, time_start + 10, time_end);

    time_start += context.workload_time; 
    time_end += context.workload_time;

    dst_file = "clay_resultss_partition_90_120.xls"; 
    my_clay->run_clay_offline(src_file, dst_file, time_start + 10, time_end);
  }
  // dst_file = "clay_resultss_partition_90_120.xls"; 






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
  // my_clay->lion_partiion_read_from_file("/home/star/data/resultss_partition_20_40.xls");
  // LOG(INFO) << "read from file done";

  // LOG(INFO) << "start read from file";
  // my_clay->lion_partiion_read_from_file("/home/star/data/resultss_partition_40_60.xls");
  // LOG(INFO) << "read from file done";

  // LOG(INFO) << "start read from file";
  // my_clay->lion_partiion_read_from_file("/home/star/data/resultss_partition_0_20.xls");
  // LOG(INFO) << "read from file done";

  // test done 


  google::ShutdownGoogleLogging();

  return 0;
}