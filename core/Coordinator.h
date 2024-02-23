//
// Created by Yi Lu on 7/24/18.
//

#pragma once

#include "common/LockfreeQueue.h"
#include "common/Message.h"
#include "common/Socket.h"
#include "common/cpuStat.h"

// #include "common/PinThreadToCore.h"

#include "core/ControlMessage.h"
#include "core/Dispatcher.h"
#include "core/Executor.h"
#include "core/Worker.h"
#include "core/factory/WorkerFactory.h"

#include <boost/algorithm/string.hpp>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include <thread>
#include <vector>
#include <sstream>

namespace star {

using namespace get_system_usage_linux;

class Coordinator {
public:

  template <class Database, class Context>
  Coordinator(std::size_t id, Database &db, const Context &context)
      : id(id), coordinator_num(context.peers.size()), peers(context.peers),
        context(context) {
    workerStopFlag.store(false);
    ioStopFlag.store(false);
    LOG(INFO) << "Coordinator initializes " << context.worker_num
              << " workers.";
    if(context.coordinator_num == id){
      // last in peer is generator
      workers = WorkerFactory::create_generator(id, db, context, workerStopFlag);
    } else {
      workers = WorkerFactory::create_workers(id, db, context, workerStopFlag);
    }

    // init sockets vector
    inSockets.resize(context.io_thread_num);
    outSockets.resize(context.io_thread_num);

    for (auto i = 0u; i < context.io_thread_num; i++) {
      inSockets[i].resize(peers.size());
      outSockets[i].resize(peers.size());
    }
    
    auto getAddressPort = [](const std::string &addressPort) {
      std::vector<std::string> result;
      boost::algorithm::split(result, addressPort, boost::is_any_of(":"));
      return result;
    };

    for (auto i = 0u; i < context.peers.size(); i++) {
      std::vector<std::string> addressPort = getAddressPort(peers[i]);
      peers_ip.push_back(addressPort[0]);
    }

  }

  ~Coordinator() = default;

  void start() {
    std::vector<std::thread> threads;

    LOG(INFO) << "Coordinator starts to run " << workers.size() << " workers.";

    for (auto i = 0u; i < workers.size(); i++) {
      threads.emplace_back(&Worker::start, workers[i].get());
      if (context.cpu_affinity) {
        ControlMessageFactory::pin_thread_to_core(context, threads[i]);
      }
    }

    // init dispatcher vector
    iDispatchers.resize(context.io_thread_num);
    oDispatchers.resize(context.io_thread_num);

    // start dispatcher threads

    std::vector<std::thread> iDispatcherThreads, oDispatcherThreads;

    for (auto i = 0u; i < context.io_thread_num; i++) {

      iDispatchers[i] = std::make_unique<IncomingDispatcher>(
          id, i, context.io_thread_num, peers_ip, inSockets[i], workers, in_queue,
          ioStopFlag);
      oDispatchers[i] = std::make_unique<OutgoingDispatcher>(
          id, i, context.io_thread_num, peers_ip, outSockets[i], workers, out_queue,
          ioStopFlag);

      iDispatcherThreads.emplace_back(&IncomingDispatcher::start,
                                      iDispatchers[i].get());
      oDispatcherThreads.emplace_back(&OutgoingDispatcher::start,
                                      oDispatchers[i].get());
      if (context.cpu_affinity) {
        ControlMessageFactory::pin_thread_to_core(context, iDispatcherThreads[i]);
        ControlMessageFactory::pin_thread_to_core(context, oDispatcherThreads[i]);
      }
    }



    // if(context.lion_with_metis_init){
    //   LOG(INFO) << "wait initialization done ... ";
    //   std::this_thread::sleep_for(std::chrono::seconds(20));
    //   LOG(INFO) << "lion with metis initialization done ... ";
    // }

    // run timeToRun seconds
    uint64_t timeToRun = context.time_to_run;
    uint64_t warmup = 10, cooldown = 5;
    std::chrono::steady_clock::time_point startTime = std::chrono::steady_clock::now();

    uint64_t total_commit = 0, total_abort_no_retry = 0, total_abort_lock = 0,
             total_abort_read_validation = 0, total_local = 0,
             total_si_in_serializable = 0, total_network_size = 0;
    uint64_t count = 0;
    for(auto i = 0u ; i < workers.size(); i ++ ){
      workers[i]->start_time = startTime;
    }

    std::ofstream outfile_excel;
    char output[256];
    sprintf(output, "/home/star/data/commits_%d.xls", (int)id);
    outfile_excel.open(output, std::ios::trunc); // ios::trunc

    outfile_excel << "n_commit" << "\t" 
                  << "metis_commit" << "\t" 
                  << "n_remaster" << "\t" 
                  << "n_migrate" << "\t" 
                  << "metis_remaster | metis_migrate" << "\t" 
                  << "cpu_usage" << "\t" 
                  << "n_network_size" << "\t" 
                  << "abort" << "\t" 
                  << "r_abort" << "\n";

    std::ofstream outfile_excel_breakdown;
    char output2[256];
    sprintf(output2, "/home/star/data/breakdown_%d.xls", (int)id);
    outfile_excel_breakdown.open(output2, std::ios::trunc); // ios::trunc

    outfile_excel_breakdown 
                  << "time_router10%" << "\t" 
                  << "time_scheuler10%" << "\t" 
                  << "time_local_locks10%" << "\t" 
                  << "time_remote_locks10%" << "\t" 
                  << "time_execute10%" << "\t" 
                  << "time_commit10%" << "\t" 
                  << "time_wait4serivce10%" << "\t" 
                  << "time_other_module10%" << "\t"
                  // << "time_total10%" << "\t"
                  << "time_latency10%" << "\t"
                  << "total_latency10%" << "\t"
                  
                  << "time_router50%" << "\t" 
                  << "time_scheuler50%" << "\t" 
                  << "time_local_locks50%" << "\t" 
                  << "time_remote_locks50%" << "\t" 
                  << "time_execute50%" << "\t" 
                  << "time_commit50%" << "\t" 
                  << "time_wait4serivce50%" << "\t" 
                  << "time_other_module50%" << "\t"
                  // << "time_total50%" << "\t"
                  << "time_latency50%" << "\t"
                  << "total_latency50%" << "\t"

                  << "time_router95%" << "\t" 
                  << "time_scheuler95%" << "\t" 
                  << "time_local_locks95%" << "\t" 
                  << "time_remote_locks95%" << "\t" 
                  << "time_execute95%" << "\t" 
                  << "time_commit95%" << "\t" 
                  << "time_wait4serivce95%" << "\t" 
                  << "time_other_module95%" << "\t"
                  // << "time_total95%" << "\t"
                  << "time_latency95%" 
                  << "total_latency50%" << "\t"
                  << "\n";

    do {
      LOG(INFO) << "SLEEP : " << std::to_string(context.sample_time_interval);


      CPU_stats t1 = read_cpu_data();

      std::this_thread::sleep_for(std::chrono::milliseconds(context.sample_time_interval * 1000));

      CPU_stats t2 = read_cpu_data();
      

      auto cpu_usage = get_cpu_usage(t1, t2);


      uint64_t n_commit = 0, metis_commit = 0, 
               n_remaster = 0, n_migrate = 0, // 
               metis_remaster = 0, metis_migrate = 0, // 
               n_abort_no_retry = 0, n_abort_lock = 0,
               n_abort_read_validation = 0, n_local = 0,
               n_remaster_abort = 0,
               n_si_in_serializable = 0, n_network_size = 0, 
               metis_n_network_size = 0;
      
      uint64_t n_singled = 0, n_distributed = 0;

      // switch type in every 10 second;
      int cur_workload_type = std::chrono::duration_cast<std::chrono::seconds>(
                 std::chrono::steady_clock::now() - startTime)
                 .count() / context.workload_time % 6;

      for (auto i = 0u; i < workers.size(); i++) {
        // if(i == 1){
        //   outfile_excel_breakdown  
        //                 << workers[i]->txn_statics.nth(10).time_router << "\t" 
        //                 << workers[i]->txn_statics.nth(10).time_scheuler << "\t" 
        //                 << workers[i]->txn_statics.nth(10).time_local_locks << "\t" 
        //                 << workers[i]->txn_statics.nth(10).time_remote_locks << "\t" 
        //                 << workers[i]->txn_statics.nth(10).time_execute << "\t" 
        //                 << workers[i]->txn_statics.nth(10).time_commit << "\t" 
        //                 << workers[i]->txn_statics.nth(10).time_wait4serivce << "\t" 
        //                 << workers[i]->txn_statics.nth(10).time_other_module << "\t" 
        //                 << workers[i]->txn_statics.nth(10).time_latency << "\t" 
        //                 << workers[i]->total_latency.nth(10) << "\t" 

        //                 << workers[i]->txn_statics.nth(50).time_router << "\t" 
        //                 << workers[i]->txn_statics.nth(50).time_scheuler << "\t" 
        //                 << workers[i]->txn_statics.nth(50).time_local_locks << "\t" 
        //                 << workers[i]->txn_statics.nth(50).time_remote_locks << "\t" 
        //                 << workers[i]->txn_statics.nth(50).time_execute << "\t" 
        //                 << workers[i]->txn_statics.nth(50).time_commit << "\t" 
        //                 << workers[i]->txn_statics.nth(50).time_wait4serivce << "\t" 
        //                 << workers[i]->txn_statics.nth(50).time_other_module << "\t" 
        //                 << workers[i]->txn_statics.nth(50).time_latency << "\t" 
        //                 << workers[i]->total_latency.nth(50) << "\t" 

        //                 << workers[i]->txn_statics.nth(95).time_router << "\t" 
        //                 << workers[i]->txn_statics.nth(95).time_scheuler << "\t" 
        //                 << workers[i]->txn_statics.nth(95).time_local_locks << "\t" 
        //                 << workers[i]->txn_statics.nth(95).time_remote_locks << "\t" 
        //                 << workers[i]->txn_statics.nth(95).time_execute << "\t" 
        //                 << workers[i]->txn_statics.nth(95).time_commit << "\t" 
        //                 << workers[i]->txn_statics.nth(95).time_wait4serivce << "\t" 
        //                 << workers[i]->txn_statics.nth(95).time_other_module << "\t"
        //                 << workers[i]->txn_statics.nth(95).time_latency << "\t"
        //                 << workers[i]->total_latency.nth(95) << "\n" ;
        //                 ;
        // }
        
        if((context.protocol.find("Lion") != context.protocol.npos || 
            context.protocol.find("LION") != context.protocol.npos ||
            context.protocol.find("CLAY-S") != context.protocol.npos ||
            context.protocol == "MyClay") && i == context.worker_num){
          metis_commit += workers[i]->n_commit.load();
          workers[i]->n_commit.store(0);

          metis_remaster += workers[i]->n_remaster.load();
          workers[i]->n_remaster.store(0);

          metis_migrate += workers[i]->n_migrate.load();
          workers[i]->n_migrate.store(0);

          metis_n_network_size += workers[i]->n_network_size.load();
          workers[i]->n_network_size.store(0);

          n_remaster_abort += workers[i]->n_remaster_abort.load();
          workers[i]->n_remaster_abort.store(0);
          continue;
        }
        n_commit += workers[i]->n_commit.load();
        workers[i]->n_commit.store(0);
        // 
        n_remaster += workers[i]->n_remaster.load();
        workers[i]->n_remaster.store(0);

        n_migrate += workers[i]->n_migrate.load();
        workers[i]->n_migrate.store(0);

        n_abort_no_retry += workers[i]->n_abort_no_retry.load();
        workers[i]->n_abort_no_retry.store(0);

        n_abort_lock += workers[i]->n_abort_lock.load();
        workers[i]->n_abort_lock.store(0);

        n_abort_read_validation += workers[i]->n_abort_read_validation.load();
        workers[i]->n_abort_read_validation.store(0);

        n_remaster_abort += workers[i]->n_remaster_abort.load();
        workers[i]->n_remaster_abort.store(0);

        n_local += workers[i]->n_local.load();
        workers[i]->n_local.store(0);

        n_si_in_serializable += workers[i]->n_si_in_serializable.load();
        workers[i]->n_si_in_serializable.store(0);

        n_network_size += workers[i]->n_network_size.load();
        workers[i]->n_network_size.store(0);

        workers[i]->workload_type = cur_workload_type;

        n_singled += workers[i]->singled_num.load();
        workers[i]->singled_num.store(0);

        n_distributed += workers[i]->distributed_num.load();
        workers[i]->distributed_num.store(0);

        workers[i]->clear_status.store(true);
        workers[i]->total_latency.clear();

        // workers[i]->time_router.clear();
        // workers[i]->time_scheuler.clear();
        // workers[i]->time_local_locks.clear();
        // workers[i]->time_remote_locks.clear();
        // workers[i]->time_execute.clear();
        // workers[i]->time_commit.clear();
        // workers[i]->time_wait4serivce.clear();
        // workers[i]->time_other_module.clear();

      }

      outfile_excel 
                  << n_commit << "\t" 
                  << metis_commit << "\t" 
                  << n_remaster << "\t" 
                  << n_migrate << "\t" 
                  << metis_remaster << " | " << metis_migrate << "\t" 
                  << cpu_usage << "\t" 
                  << n_network_size << "\t"
                  << metis_n_network_size << "\t"
                  << n_abort_no_retry + n_abort_lock + n_abort_read_validation << "\t" 
                  << n_remaster_abort << "\t"
                  << 1.0 * n_network_size / n_commit << "\t"
                  << 1.0 * n_distributed / (n_distributed + n_singled) << "\t"
                  << n_distributed << "\t"
                  << n_singled << "\n";
                  // n_singled << " | " << n_distributed << " = " << 1.0 * n_distributed / (n_distributed + n_singled) << "\n";


      LOG(INFO) << "workload: " << cur_workload_type << " commit: " << n_commit 
                << " metis_commit: " << metis_commit
                << " n_remaster: " << n_remaster 
                << " n_migrate: " << n_migrate 
                << " metis_remaster: " << metis_remaster 
                << " metis_migrate: " << metis_migrate 
                << " abort: " << n_abort_no_retry + n_abort_lock + n_abort_read_validation
                << " (" << n_abort_no_retry << "/" << n_abort_lock << "/"
                << n_abort_read_validation
                << "), network size: " << n_network_size
                << ", avg network size: " << 1.0 * n_network_size / n_commit
                << ", metis_n_network_size : " << metis_n_network_size 
                << ", avg metis_n_network_size : " << 1.0 * metis_n_network_size / metis_commit
                << ", si_in_serializable: " << n_si_in_serializable << " "
                << 100.0 * n_si_in_serializable / n_commit << " %"
                << ", local: " << 100.0 * n_local / n_commit << " %\n"
                << "n_singled: " << n_singled << " , n_distributed: " << n_distributed << " " <<  1.0 * n_distributed / (n_distributed + n_singled)  << " % ";
      count++;
      if (count > warmup && count <= timeToRun - cooldown) {
        total_commit += n_commit;
        total_abort_no_retry += n_abort_no_retry;
        total_abort_lock += n_abort_lock;
        total_abort_read_validation += n_abort_read_validation;
        total_local += n_local;
        total_si_in_serializable += n_si_in_serializable;
        total_network_size += n_network_size;
      }

    } while (std::chrono::duration_cast<std::chrono::seconds>(
                 std::chrono::steady_clock::now() - startTime)
                 .count() < (int)timeToRun);

    count = timeToRun - warmup - cooldown;

    LOG(INFO) << "average commit: " << 1.0 * total_commit / count << " abort: "
              << 1.0 *
                     (total_abort_no_retry + total_abort_lock +
                      total_abort_read_validation) /
                     count
              << " (" << 1.0 * total_abort_no_retry / count << "/"
              << 1.0 * total_abort_lock / count << "/"
              << 1.0 * total_abort_read_validation / count
              << "), network size: " << total_network_size
              << ", avg network size: "
              << 1.0 * total_network_size / total_commit
              << ", si_in_serializable: " << total_si_in_serializable << " "
              << 100.0 * total_si_in_serializable / total_commit << " %"
              << ", local: " << 100.0 * total_local / total_commit << " %";

    outfile_excel.close();
    workerStopFlag.store(true);

    for (auto i = 0u; i < threads.size(); i++) {
      workers[i]->onExit();
      threads[i].join();
    }

    // gather throughput
    double sum_commit = gather(1.0 * total_commit / count);
    if (id == 0) {
      LOG(INFO) << "total commit: " << sum_commit;
    }

    // make sure all messages are sent
    std::this_thread::sleep_for(std::chrono::seconds(1));

    ioStopFlag.store(true);

    for (auto i = 0u; i < context.io_thread_num; i++) {
      iDispatcherThreads[i].join();
      oDispatcherThreads[i].join();
    }

    close_sockets();
    
    LOG(INFO) << "Coordinator exits." << output; 
  }

  void connectToPeers() {

    // single node test mode
    // if (peers.size() == 1) {
    //   return;
    // }

    auto getAddressPort = [](const std::string &addressPort) {
      std::vector<std::string> result;
      boost::algorithm::split(result, addressPort, boost::is_any_of(":"));
      return result;
    };

    // start some listener threads

    std::vector<std::thread> listenerThreads;

    for (auto i = 0u; i < context.io_thread_num; i++) {

      listenerThreads.emplace_back(
          [id = this->id, peers = this->peers, &inSockets = this->inSockets[i],
           &getAddressPort,
           tcp_quick_ack = context.tcp_quick_ack](std::size_t listener_id) {
            // thread 
            std::vector<std::string> addressPort = getAddressPort(peers[id]);

            Listener l(addressPort[0].c_str(),
                       atoi(addressPort[1].c_str()) + listener_id, 100);
            LOG(INFO) << "Listener " << listener_id << " on coordinator " << id
                      << " listening on " << peers[id];

            auto n = peers.size();

            for (std::size_t i = 0; i < n - 1; i++) {
              Socket socket = l.accept();
              std::size_t c_id;
              socket.read_number(c_id);
              // set quick ack flag
              socket.set_quick_ack_flag(tcp_quick_ack);
              inSockets[c_id] = std::move(socket);
            }

            LOG(INFO) << "Listener " << listener_id << " on coordinator " << id
                      << " exits.";
          },
          i);
    }

    // connect to peers
    auto n = peers.size();
    constexpr std::size_t retryLimit = 50;

    // connect to multiple remote coordinators
    for (auto i = 0u; i < n; i++) {
      if (i == id)
        continue;
      std::vector<std::string> addressPort = getAddressPort(peers[i]);
      // connnect to multiple remote listeners
      for (auto listener_id = 0u; listener_id < context.io_thread_num;
           listener_id++) {
        for (auto k = 0u; k < retryLimit; k++) {
          Socket socket;

          int ret = socket.connect(addressPort[0].c_str(),
                                   atoi(addressPort[1].c_str()) + listener_id);
          if (ret == -1) {
            socket.close();
            if (k == retryLimit - 1) {
              LOG(FATAL) << "failed to connect to peers, exiting ...";
              exit(1);
            }

            // listener on the other side has not been set up.
            LOG(INFO) << "Coordinator " << id << " failed to connect " << i
                      << "(" << peers[i] << ")'s listener " << listener_id
                      << ", retry in 5 seconds.";
            std::this_thread::sleep_for(std::chrono::seconds(5));
            continue;
          }
          if (context.tcp_no_delay) {
            socket.disable_nagle_algorithm();
          }

          LOG(INFO) << "Coordinator " << id << " connected to " << i;
          socket.write_number(id);
          outSockets[listener_id][i] = std::move(socket);
          break;
        }
      }
    }

    for (auto i = 0u; i < listenerThreads.size(); i++) {
      listenerThreads[i].join();
    }

    LOG(INFO) << "Coordinator " << id << " connected to all peers.";
  }

  double gather(double value) {

    auto init_message = [](Message *message, std::size_t coordinator_id,
                           std::size_t dest_node_id) {
      message->set_source_node_id(coordinator_id);
      message->set_dest_node_id(dest_node_id);
      message->set_worker_id(0);
    };

    double sum = value;

    if (id == 0) {
      for (std::size_t i = 0; i < coordinator_num - 1; i++) {

        in_queue.wait_till_non_empty();
        std::unique_ptr<Message> message(in_queue.front());
        bool ok = in_queue.pop();
        CHECK(ok);
        CHECK(message->get_message_count() == 1);

        MessagePiece messagePiece = *(message->begin());

        CHECK(messagePiece.get_message_type() ==
              static_cast<uint32_t>(ControlMessage::STATISTICS));
        CHECK(messagePiece.get_message_length() ==
              MessagePiece::get_header_size() + sizeof(double));
        Decoder dec(messagePiece.toStringPiece());
        double v;
        dec >> v;
        sum += v;
      }

    } else {
      auto message = std::make_unique<Message>();
      init_message(message.get(), id, 0);
      ControlMessageFactory::new_statistics_message(*message, value);
      out_queue.push(message.release());
      LOG(INFO) << "new_statistics_message send";
    }
    return sum;
  }

private:
  void close_sockets() {
    for (auto i = 0u; i < inSockets.size(); i++) {
      for (auto j = 0u; j < inSockets[i].size(); j++) {
        inSockets[i][j].close();
      }
    }
    for (auto i = 0u; i < outSockets.size(); i++) {
      for (auto j = 0u; j < outSockets[i].size(); j++) {
        outSockets[i][j].close();
      }
    }
  }



private:
  /*
   * A coordinator may have multilpe inSockets and outSockets, connected to one
   * remote coordinator to fully utilize the network
   *
   * inSockets[0][i] receives w_id % io_threads from coordinator i
   */

  std::size_t id, coordinator_num;
  const std::vector<std::string> &peers; // ip and port ?
  std::vector<std::string> peers_ip; // ip and port ?

  const Context &context;
  std::vector<std::vector<Socket>> inSockets, outSockets;
  std::atomic<bool> workerStopFlag, ioStopFlag;
  std::vector<std::shared_ptr<Worker>> workers;
  std::vector<std::unique_ptr<IncomingDispatcher>> iDispatchers;
  std::vector<std::unique_ptr<OutgoingDispatcher>> oDispatchers;
  LockfreeQueue<Message *> in_queue, out_queue;
};
} // namespace star
