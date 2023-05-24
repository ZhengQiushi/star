//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include "core/Defs.h"
#include "core/Executor.h"
#include "core/Manager.h"
#include "core/group_commit/Generator.h"



#include "benchmark/tpcc/Workload.h"
#include "benchmark/ycsb/Workload.h"

#include "protocol/Silo/Silo.h"
#include "protocol/Silo/SiloExecutor.h"
#include "protocol/TwoPL/TwoPL.h"
#include "protocol/TwoPL/TwoPLExecutor.h"

#include "core/group_commit/Executor.h"
#include "core/group_commit/Manager.h"
#include "protocol/SiloGC/SiloGC.h"
#include "protocol/SiloGC/SiloGCExecutor.h"
#include "protocol/TwoPLGC/TwoPLGC.h"
#include "protocol/TwoPLGC/TwoPLGCExecutor.h"

#include "protocol/Star/Star.h"
#include "protocol/Star/StarExecutor.h"
#include "protocol/Star/StarManager.h"
#include "protocol/Star/StarGenerator.h"


#include "protocol/MyClay/MyClay.h"
#include "protocol/MyClay/MyClayExecutor.h"
#include "protocol/MyClay/MyClayManager.h"
#include "protocol/MyClay/MyClayGenerator.h"
#include "protocol/MyClay/MyClayMeitsExecutor.h"
#include "protocol/MyClay/MyClayMetisGenerator.h"


#include "protocol/Lion/Lion.h"
#include "protocol/Lion/LionExecutor.h"
#include "protocol/Lion/LionManager.h"
#include "protocol/Lion/LionTransaction.h"
// #include "protocol/Lion/LionRecorder.h"
#include "protocol/Lion/LionGenerator.h"

#include "protocol/Lion/LionMetisExecutor.h"
#include "protocol/Lion/LionMetisGenerator.h"

// #include "protocol/LionNS/LionNSExecutor.h"
// #include "protocol/LionNS/LionNSManager.h"


#include "protocol/Calvin/Calvin.h"
#include "protocol/Calvin/CalvinExecutor.h"
#include "protocol/Calvin/CalvinManager.h"
#include "protocol/Calvin/CalvinTransaction.h"
#include "protocol/Calvin/CalvinGenerator.h"

#include "protocol/Hermes/Hermes.h"
#include "protocol/Hermes/HermesExecutor.h"
#include "protocol/Hermes/HermesManager.h"
#include "protocol/Hermes/HermesTransaction.h"
#include "protocol/Hermes/HermesGenerator.h"


#include "protocol/Aria/Aria.h"
#include "protocol/Aria/AriaExecutor.h"
#include "protocol/Aria/AriaManager.h"
#include "protocol/Aria/AriaTransaction.h"
#include "protocol/Aria/AriaGenerator.h"

#include <unordered_set>

namespace star {

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

class WorkerFactory {

public:
/**
 * @brief Create a workers object
 * 
 * @tparam Database 
 * @tparam Context 
 * @param coordinator_id 
 * @param db 
 * @param context 
 * @param stop_flag 
 * @return std::vector<std::shared_ptr<Worker>> 
 *         0 ~ worknum - 1 : worker
 *         worknum         : manager
 *         others          : recorder & predictor
 */
  template <class Database, class Context>
  static std::vector<std::shared_ptr<Worker>>
  create_workers(std::size_t coordinator_id, Database &db,
                 const Context &context, std::atomic<bool> &stop_flag) {

    std::unordered_set<std::string> protocols = {"Silo",  "SiloGC",  "Star",
                                                 "TwoPL", "TwoPLGC", "Calvin",
                                                 "Lion", "LionNS", "Hermes", "MyClay", "Aria"};
    LOG(INFO) << "context.protocol: " << context.protocol;

    CHECK(protocols.count(context.protocol) == 1);

    std::vector<std::shared_ptr<Worker>> workers;

    if (context.protocol == "Silo") {

      using TransactionType = star::SiloTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      auto manager = std::make_shared<Manager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<SiloExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }

      workers.push_back(manager);

    } else if (context.protocol == "SiloGC") {

      using TransactionType = star::SiloTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      auto manager = std::make_shared<group_commit::Manager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<SiloGCExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }
      workers.push_back(manager);

    } else if (context.protocol == "Star") {

      CHECK(context.partition_num %
                (context.worker_num * context.coordinator_num) ==
            0)
          << "In Star, each partition is managed by only one thread.";

      using TransactionType = star::SiloTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      auto manager = std::make_shared<StarManager<WorkloadType>>(
          coordinator_id, context.worker_num, context, stop_flag, db);

      // add recorder for data-transformation
      // auto recorder = std::make_shared<StarRecorder<WorkloadType> >(  // 
      //     coordinator_id, context.worker_num + 1, context, stop_flag, db,
      //     manager->recorder_status, manager->transmit_status, 
      //     manager->n_completed_workers, manager->n_started_workers);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<StarExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->batch_size,
            manager->worker_status, manager->n_completed_workers,
            manager->n_started_workers)); // , manager->recorder_status
      }
      workers.push_back(manager);
      // workers.push_back(recorder);

    } else if (context.protocol == "Lion") {

      CHECK(context.partition_num %
                (context.worker_num * context.coordinator_num) ==
            0)
          << "In Lion, each partition is managed by only one thread.";

      using TransactionType = star::LionTransaction ;// TwoPLTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      int manager_thread_id = context.worker_num;
      // if(context.lion_with_metis_init){
        manager_thread_id += 1;
      // }

      auto manager = std::make_shared<LionManager<WorkloadType>>(
          coordinator_id, manager_thread_id, context, stop_flag, db);

      // add recorder for data-transformation
      // auto recorder = std::make_shared<LionRecorder<WorkloadType> >(
      //     coordinator_id, context.worker_num + 1, context, stop_flag, db,
      //     manager->recorder_status, manager->transmit_status, 
      //     manager->n_completed_workers, manager->n_started_workers);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<LionExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->batch_size,
            manager->worker_status, manager->n_completed_workers,
            manager->n_started_workers,
            manager->skip_s_phase,
            manager->transactions_prepared,
            manager->txn_meta
            // manager->s_transactions_queue,
            // manager->c_transactions_queue,
            // manager->s_l,
            // manager->c_l,
            // manager->router_transactions_queue,
            // manager->s_txn_id_queue,
            // manager->c_txn_id_queue,
            // manager->storages
            )); // , manager->recorder_status // recorder->data_pack_map
      }
      // 
      // if(context.lion_with_metis_init){
        workers.push_back(std::make_shared<LionMetisExecutor<WorkloadType>>(
            coordinator_id, workers.size(), db, context, manager->batch_size,
            manager->worker_status, manager->n_completed_workers,
            manager->n_started_workers));
      // }

      workers.push_back(manager);
      // workers.push_back(recorder);  
    }
    //  else if (context.protocol == "LionWithBrain") {

    //   CHECK(context.partition_num %
    //             (context.worker_num * context.coordinator_num) ==
    //         0)
    //       << "In Lion, each partition is managed by only one thread.";

    //   using TransactionType = star::LionTransaction ;// TwoPLTransaction;
    //   using WorkloadType =
    //       typename InferType<Context>::template WorkloadType<TransactionType>;

    //   auto manager = std::make_shared<LionManager<WorkloadType>>(
    //       coordinator_id, context.worker_num, context, stop_flag, db);

    //   // add recorder for data-transformation
    //   // auto recorder = std::make_shared<LionRecorder<WorkloadType> >(
    //   //     coordinator_id, context.worker_num + 1, context, stop_flag, db,
    //   //     manager->recorder_status, manager->transmit_status, 
    //   //     manager->n_completed_workers, manager->n_started_workers);

    //   for (auto i = 0u; i < context.worker_num; i++) {
    //     workers.push_back(std::make_shared<LionExecutor<WorkloadType>>(
    //         coordinator_id, i, db, context, manager->batch_size,
    //         manager->worker_status, manager->n_completed_workers, 
    //         manager->n_started_workers)); // , manager->recorder_status , recorder->data_pack_map
    //   }
    //   workers.push_back(manager);
    //   // workers.push_back(recorder);  
    // } 
    // else if (context.protocol == "LionNS") {

    //   CHECK(context.partition_num %
    //             (context.worker_num * context.coordinator_num) ==
    //         0)
    //       << "In Lion, each partition is managed by only one thread.";

    //   using TransactionType = star::LionTransaction ;// TwoPLTransaction;
    //   using WorkloadType =
    //       typename InferType<Context>::template WorkloadType<TransactionType>;

    //   auto manager = std::make_shared<LionNSManager<WorkloadType>>(
    //       coordinator_id, context.worker_num, context, stop_flag, db);

    //   // add recorder for data-transformation
    //   auto recorder = std::make_shared<LionRecorder<WorkloadType> >(
    //       coordinator_id, context.worker_num + 1, context, stop_flag, db,
    //       manager->recorder_status, manager->transmit_status, 
    //       manager->n_completed_workers, manager->n_started_workers);

    //   for (auto i = 0u; i < context.worker_num; i++) {
    //     workers.push_back(std::make_shared<LionNSExecutor<WorkloadType>>(
    //         coordinator_id, i, db, context, manager->batch_size,
    //         manager->worker_status, manager->n_completed_workers,
    //         manager->n_started_workers, recorder->data_pack_map)); // , manager->recorder_status
    //   }
    //   workers.push_back(manager);
    //   // workers.push_back(recorder);  
    // } 
    
    else if (context.protocol == "TwoPL") {

      using TransactionType = star::TwoPLTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      auto manager = std::make_shared<Manager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<TwoPLExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }

      workers.push_back(manager);
    } else if (context.protocol == "TwoPLGC") {

      using TransactionType = star::TwoPLTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      auto manager = std::make_shared<group_commit::Manager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<TwoPLGCExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }

      workers.push_back(manager);
    } else if (context.protocol == "Calvin") {

      using TransactionType = star::CalvinTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      // create manager

      auto manager = std::make_shared<CalvinManager<WorkloadType>>(
          coordinator_id, context.worker_num, db, context, stop_flag);

      // create worker

      std::vector<CalvinExecutor<WorkloadType> *> all_executors;

      for (auto i = 0u; i < context.worker_num; i++) {

        auto w = std::make_shared<CalvinExecutor<WorkloadType>>(
            coordinator_id, i, db, context, 
            manager->transactions,
            manager->storages, 
            manager->lock_manager_status,
            manager->worker_status, manager->n_completed_workers,
            manager->n_started_workers);
        workers.push_back(w);
        manager->add_worker(w);
        all_executors.push_back(w.get());
      }
      // push manager to workers
      workers.push_back(manager);

      for (auto i = 0u; i < context.worker_num; i++) {
        static_cast<CalvinExecutor<WorkloadType> *>(workers[i].get())
            ->set_all_executors(all_executors);
      }
    } 
    else if (context.protocol == "Hermes") {

      using TransactionType = star::HermesTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      // create manager

      auto manager = std::make_shared<HermesManager<WorkloadType>>(
          coordinator_id, context.worker_num, db, context, stop_flag);

      // create worker

      std::vector<HermesExecutor<WorkloadType> *> all_executors;

      for (auto i = 0u; i < context.worker_num; i++) {

        auto w = std::make_shared<HermesExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->transactions,
            manager->storages, manager->lock_manager_status,
            manager->worker_status, manager->n_completed_workers,
            manager->n_started_workers);
        workers.push_back(w);
        manager->add_worker(w);
        all_executors.push_back(w.get());
      }
      // push manager to workers
      workers.push_back(manager);

      for (auto i = 0u; i < context.worker_num; i++) {
        static_cast<HermesExecutor<WorkloadType> *>(workers[i].get())
            ->set_all_executors(all_executors);
      }
    } else if (context.protocol == "Aria") {

      using TransactionType = star::AriaTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      // create manager

      auto manager = std::make_shared<AriaManager<WorkloadType>>(
          coordinator_id, context.worker_num, db, context, stop_flag);

      // create worker

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<AriaExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->transactions,
            manager->storages, manager->epoch, manager->worker_status,
            manager->total_abort, manager->n_completed_workers,
            manager->n_started_workers, manager->transactions_prepared));
      }

      workers.push_back(manager);
    }
    else if (context.protocol == "MyClay") {
      CHECK(context.partition_num %
                (context.worker_num * context.coordinator_num) ==
            0)
          << "In MyClay, each partition is managed by only one thread.";

      using TransactionType = star::SiloTransaction ;// TwoPLTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      int manager_thread_id = context.worker_num;
      // if(context.lion_with_metis_init){
        manager_thread_id += 1;
      // }

      auto manager = std::make_shared<group_commit::MyClayManager<WorkloadType>>(
          coordinator_id, manager_thread_id, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<MyClayExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers,
            manager->transactions_prepared,
            manager->r_transactions_queue,
            manager->txn_id_queue,
            manager->storages
            ));
      }

      // if(context.lion_with_metis_init){
        workers.push_back(std::make_shared<MyClayMetisExecutor<WorkloadType>>(
            coordinator_id, workers.size(), db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      // }

      workers.push_back(manager);
    } 

    return workers;
  }

  template <class Database, class Context>
  static std::vector<std::shared_ptr<Worker>> 
  create_generator(std::size_t coordinator_id, Database &db,
                 const Context &context, std::atomic<bool> &stop_flag){

    std::vector<std::shared_ptr<Worker>> workers;
    if (context.protocol == "SiloGC") {

      using TransactionType = star::SiloTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;
      using DatabaseType = 
          typename WorkloadType::DatabaseType;

      auto manager = std::make_shared<group_commit::Manager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<group_commit::Generator<WorkloadType, Silo<DatabaseType>>>(
              coordinator_id, i, db, context, manager->worker_status,
              manager->n_completed_workers, manager->n_started_workers));
      }

      workers.push_back(manager);

    } else if (context.protocol == "Star") {
      //  CHECK(context.partition_num %
      //           (context.worker_num * context.coordinator_num) ==
      //       0)
      //     << "In Star, each partition is managed by only one thread.";

      using TransactionType = star::SiloTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;
      using DatabaseType = 
          typename WorkloadType::DatabaseType;
          
      auto manager = std::make_shared<StarManager<WorkloadType>>(
          coordinator_id, context.worker_num, context, stop_flag, db);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<group_commit::StarGenerator<WorkloadType, Silo<DatabaseType>>>(
              coordinator_id, i, db, context, manager->worker_status,
              manager->n_completed_workers, manager->n_started_workers));
      }
      workers.push_back(manager);
      // workers.push_back(recorder);

    }
    else if (context.protocol == "Lion") {

      CHECK(context.partition_num %
                (context.worker_num * context.coordinator_num) ==
            0)
          << "In Lion, each partition is managed by only one thread.";

      using TransactionType = star::LionTransaction ;// TwoPLTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;
      using DatabaseType = 
          typename WorkloadType::DatabaseType;

      int manager_thread_id = context.worker_num + 1;

      auto manager = std::make_shared<LionManager<WorkloadType>>(
          coordinator_id, manager_thread_id, context, stop_flag, db);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<group_commit::LionGenerator<WorkloadType, Lion<DatabaseType>>>(
              coordinator_id, i, db, context, manager->worker_status,
              manager->n_completed_workers, manager->n_started_workers, 
              manager->skip_s_phase,
              manager->transactions_queue, manager->is_full_signal,
              manager->schedule_meta));
      }
      // 
      // if(context.lion_with_metis_init){
        workers.push_back(std::make_shared<group_commit::LionMetisGenerator<WorkloadType, Lion<DatabaseType>>>(
              coordinator_id, workers.size(), db, context, manager->worker_status,
              manager->n_completed_workers, manager->n_started_workers));
      // }
      workers.push_back(manager);
      // workers.push_back(recorder);  
    } else if (context.protocol == "MyClay") {
      CHECK(context.partition_num %
                (context.worker_num * context.coordinator_num) ==
            0)
          << "In MyClay, each partition is managed by only one thread.";

      using TransactionType = star::SiloTransaction ;// TwoPLTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;
      using DatabaseType = 
          typename WorkloadType::DatabaseType;

      int manager_thread_id = context.worker_num;
      // if(context.lion_with_metis_init){
        manager_thread_id += 1;
      // }

      auto manager = std::make_shared<group_commit::MyClayManager<WorkloadType>>(
          coordinator_id, manager_thread_id, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<group_commit::MyClayGenerator<WorkloadType, MyClay<DatabaseType>>>(
              coordinator_id, i, db, context, manager->worker_status,
              manager->n_completed_workers, manager->n_started_workers,
              manager->transactions_queue, 
              manager->is_full_signal
              ));
      }

        workers.push_back(std::make_shared<group_commit::MyClayMetisGenerator<WorkloadType, MyClay<DatabaseType>>>(
              coordinator_id, workers.size(), db, context, manager->worker_status,
              manager->n_completed_workers, manager->n_started_workers));


      workers.push_back(manager);

    // }
    // else if (context.protocol == "LionWithBrain") {

    //   CHECK(context.partition_num %
    //             (context.worker_num * context.coordinator_num) ==
    //         0)
    //       << "In Lion, each partition is managed by only one thread.";

    //   using TransactionType = star::LionTransaction ;// TwoPLTransaction;
    //   using WorkloadType =
    //       typename InferType<Context>::template WorkloadType<TransactionType>;

    //   auto manager = std::make_shared<LionManager<WorkloadType>>(
    //       coordinator_id, context.worker_num, context, stop_flag, db);

    //   for (auto i = 0u; i < context.worker_num; i++) {
    //     workers.push_back(std::make_shared<group_commit::Generator<WorkloadType>>(
    //           coordinator_id, i, db, context, manager->worker_status,
    //           manager->n_completed_workers, manager->n_started_workers));
    //   }
    //   workers.push_back(manager);
    //   // workers.push_back(recorder);  
    // } else if (context.protocol == "LionNS") {

    //   CHECK(context.partition_num %
    //             (context.worker_num * context.coordinator_num) ==
    //         0)
    //       << "In Lion, each partition is managed by only one thread.";

    //   using TransactionType = star::LionTransaction ;// TwoPLTransaction;
    //   using WorkloadType =
    //       typename InferType<Context>::template WorkloadType<TransactionType>;

    //   auto manager = std::make_shared<LionNSManager<WorkloadType>>(
    //       coordinator_id, context.worker_num, context, stop_flag, db);

    //   for (auto i = 0u; i < context.worker_num; i++) {
    //     workers.push_back(std::make_shared<group_commit::Generator<WorkloadType>>(
    //           coordinator_id, i, db, context, manager->worker_status,
    //           manager->n_completed_workers, manager->n_started_workers));
    //   }
    //   workers.push_back(manager);
    //   // workers.push_back(recorder);  
    // } 
    // else if (context.protocol == "TwoPL") {

    //   using TransactionType = star::TwoPLTransaction;
    //   using WorkloadType =
    //       typename InferType<Context>::template WorkloadType<TransactionType>;

    //   auto manager = std::make_shared<Manager>(
    //       coordinator_id, context.worker_num, context, stop_flag);

    //   for (auto i = 0u; i < context.worker_num; i++) {
    //     workers.push_back(std::make_shared<group_commit::Generator<WorkloadType>>(
    //           coordinator_id, i, db, context, manager->worker_status,
    //           manager->n_completed_workers, manager->n_started_workers));
    //   }

    //   workers.push_back(manager);
    // } else if (context.protocol == "TwoPLGC") {

    //   using TransactionType = star::TwoPLTransaction;
    //   using WorkloadType =
    //       typename InferType<Context>::template WorkloadType<TransactionType>;

    //   auto manager = std::make_shared<group_commit::Manager>(
    //       coordinator_id, context.worker_num, context, stop_flag);

    //   for (auto i = 0u; i < context.worker_num; i++) {
    //     workers.push_back(std::make_shared<group_commit::Generator<WorkloadType>>(
    //           coordinator_id, i, db, context, manager->worker_status,
    //           manager->n_completed_workers, manager->n_started_workers));
    //   }

    //   workers.push_back(manager);
    } else if (context.protocol == "Calvin") {

      using TransactionType = star::CalvinTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;
      using DatabaseType = 
          typename WorkloadType::DatabaseType;

      // create manager
      auto manager = std::make_shared<CalvinManager<WorkloadType>>(
          coordinator_id, context.worker_num, db, context, stop_flag);

      // create worker
      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<group_commit::CalvinGenerator<WorkloadType, Calvin<DatabaseType>>>(
              coordinator_id, i, db, context, manager->worker_status,
              manager->n_completed_workers, manager->n_started_workers));
      }
      
      // push manager to workers
      workers.push_back(manager);
    } 
    else if (context.protocol == "Hermes") {

      using TransactionType = star::HermesTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;
      using DatabaseType = 
          typename WorkloadType::DatabaseType;
      // create manager

      auto manager = std::make_shared<HermesManager<WorkloadType>>(
          coordinator_id, context.worker_num, db, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<group_commit::HermesGenerator<WorkloadType, Hermes<DatabaseType>>>(
              coordinator_id, i, db, context, manager->worker_status,
              manager->n_completed_workers, manager->n_started_workers));
      }
      // push manager to workers
      workers.push_back(manager);
    } else if (context.protocol == "Aria") {

      using TransactionType = star::AriaTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;
      using DatabaseType = 
          typename WorkloadType::DatabaseType;
      // create manager

      auto manager = std::make_shared<AriaManager<WorkloadType>>(
          coordinator_id, context.worker_num, db, context, stop_flag);

      // create worker

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<group_commit::AriaGenerator<WorkloadType, Aria<DatabaseType>>>(
              coordinator_id, i, db, context, manager->worker_status,
              manager->n_completed_workers, manager->n_started_workers,
              manager->is_full_signal,
              manager->schedule_done));
      }

      workers.push_back(manager);
    }


    return workers;
  }
};
} // namespace star