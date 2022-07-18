//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include "core/Partitioner.h"

#include "common/BufferedFileWriter.h"
#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Lion/Lion.h"
#include "protocol/Lion/LionQueryNum.h"

#include "protocol/Lion/LionExecutor.h"

#include <limits.h>
#include <chrono>
#include <deque>
#include <unordered_set>
#include <unordered_map>

namespace star {


template <class Workload> class LionNSExecutor : public star::LionExecutor<Workload> {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;
  using TransactionType = typename WorkloadType::TransactionType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = Lion<DatabaseType>;

  using MessageType = LionMessage;
  using MessageFactoryType = LionMessageFactory;
  using MessageHandlerType = LionMessageHandler<DatabaseType>;

  LionNSExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
               const ContextType &context, uint32_t &batch_size,
               std::atomic<uint32_t> &worker_status,
               std::atomic<uint32_t> &n_complete_workers,
               std::atomic<uint32_t> &n_started_workers, 
               std::unordered_map<std::string, int> &data_pack_map)

               // LockfreeQueueMulti<data_pack*, 8064 > &data_pack_queue)
      : LionExecutor<Workload>(coordinator_id, id, db,
               context, batch_size,
               worker_status,
               n_complete_workers,
               n_started_workers,
               data_pack_map) {
               }

  void start() override {

    LOG(INFO) << "Executor " << this->id << " starts.";
    ExecutorStatus status;

    // C-Phase to S-Phase, to C-phase ...
    int times = 0;
    auto now = std::chrono::steady_clock::now();
    while ((status = static_cast<ExecutorStatus>(this->worker_status.load())) !=
           ExecutorStatus::START) {
      std::this_thread::yield();
    }

    for (;;) {
        ExecutorStatus status;
        do {
                status = static_cast<ExecutorStatus>(this->worker_status.load());

                if (status == ExecutorStatus::EXIT) {
                LOG(INFO) << "Executor " << this->id << " exits.";
                return;
                }
        } while (status != ExecutorStatus::START);

        // commit transaction in s_phase;
        this->commit_transactions();


        this->n_started_workers.fetch_add(1);

        auto begin = std::chrono::steady_clock::now();
        size_t lion_king_coordinator_id;
        bool is_lion_king = false;
        times ++ ;
        

        VLOG_IF(DEBUG_V, this->id == 0) << "worker " << this->id << " prepare_transactions_to_run";

        WorkloadType c_workload = WorkloadType (this->coordinator_id, this->db, this->random, *this->l_partitioner.get(), this->start_time);
        WorkloadType s_workload = WorkloadType (this->coordinator_id, this->db, this->random, *this->s_partitioner.get(), this->start_time);
        StorageType storage;

        is_lion_king = (this->coordinator_id == lion_king_coordinator_id);
        // 准备transaction
        now = std::chrono::steady_clock::now();

        this->prepare_transactions_to_run(c_workload, s_workload, storage, is_lion_king);

        VLOG_IF(DEBUG_V, this->id == 0) << "prepare_transactions_to_run "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - now)
                        .count()
                << " milliseconds.";
        now = std::chrono::steady_clock::now();

        VLOG_IF(DEBUG_V, this->id == 0) << this->id << " prepare_transactions_to_run \n" << 
                "c cross \\ single = " << this->c_transactions_queue.size() << " \\" << this->c_single_transactions_queue.size() << " \n" << 
                "r cross \\ single = " << this->r_transactions_queue.size() << " \\" << this->r_single_transactions_queue.size() << " \n" << 
                "s single = " << this->s_transactions_queue.size();

        // c_phase


        this->run_transaction(ExecutorStatus::C_PHASE, &this->c_transactions_queue, this->async_message_num, true, true);

        VLOG_IF(DEBUG_V, this->id == 0) << "c_phase "
                        << std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - now)
                        .count()
                        << " milliseconds.";
        now = std::chrono::steady_clock::now();

        this->run_transaction(ExecutorStatus::C_PHASE, &this->r_single_transactions_queue, this->async_message_num, true, true);
        this->run_transaction(ExecutorStatus::C_PHASE, &this->c_single_transactions_queue, this->async_message_num, true, true);

        this->replication_fence();

        VLOG_IF(DEBUG_V, this->id == 0) << "c_single_transactions_queue "
        << std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - now)
                .count()
        << " milliseconds.";
        now = std::chrono::steady_clock::now();

        // commit transaction in c_phase;
        this->commit_transactions();
        
        // 
        this->run_transaction(ExecutorStatus::C_PHASE, &this->s_transactions_queue, this->async_message_num, true, true);

        this->replication_fence();

        VLOG_IF(DEBUG_V, this->id == 0) << "s_phase "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - now)
                        .count()
                << " milliseconds.";
        now = std::chrono::steady_clock::now();


        VLOG_IF(DEBUG_V, this->id == 0) << "wait for switch back "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - now)
                        .count()
                << " milliseconds.";
        now = std::chrono::steady_clock::now();

        VLOG_IF(DEBUG_V, this->id == 0) << "whole batch "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - begin)
                        .count()
                << " milliseconds.";

        this->n_complete_workers.fetch_add(1);

        // once all workers are stop, we need to process the replication
        // requests

        while (static_cast<ExecutorStatus>(this->worker_status.load()) !=
                ExecutorStatus::CLEANUP) {
        this->process_request();
        }

        this->process_request();
        this->n_complete_workers.fetch_add(1);
    }
    VLOG_IF(DEBUG_V, this->id == 0) << "TIMES : " << times; 

  }


private:

private:

};
} // namespace star