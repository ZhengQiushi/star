//
// Created by Yi Lu on 9/10/18.
//

#pragma once
#include <vector>
#include <set>
namespace star {



#define DEBUG_V3 3
#define DEBUG_V4 4
#define DEBUG_V 5
#define DEBUG_V6 6    // 分区情况

#define DEBUG_V8 8    // 对比 no switch
#define DEBUG_V9 9    // 对比 no switch

#define DEBUG_V11 11 // router_only
#define DEBUG_V12 12  //
#define DEBUG_V14 14  // lock and unlock
#define DEBUG_V16 16  // lock and unlock


enum class ExecutorStatus {
  START,
  CLEANUP,
  C_PHASE,
  S_PHASE,
  Analysis,
  Execute,
  Kiva_READ,
  Kiva_COMMIT,
  STOP,
  EXIT,
  LION_KING_START,
  LION_NORMAL_START,
  LION_KING_EXECUTE
};

enum myTestSet {
  YCSB,
  TPCC
};

enum class TransactionResult { COMMIT, READY_TO_COMMIT, ABORT, ABORT_NORETRY, NOT_LOCAL_NORETRY, TRANSMIT_REQUEST };

enum class ReadMethods {
  REMOTE_READ_WITH_TRANSFER,
  LOCAL_READ,
  REMOTE_READ_ONLY
};

enum class RouterTxnOps {
  LOCAL,
  REMASTER,
  TRANSFER
};

// 64bit
// 4bit    | 8bit | 12bit | 18bit | 22bit
// tableID | W_id | D_id  | C_id  | OL_id

#define RECORD_COUNT_C_ID_OFFSET     22
#define RECORD_COUNT_D_ID_OFFSET     40
#define RECORD_COUNT_W_ID_OFFSET     52
#define RECORD_COUNT_TABLE_ID_OFFSET 60

#define RECORD_COUNT_OL_ID_VALID              0x03fffff
#define RECORD_COUNT_C_ID_VALID           0x0ffffC00000
#define RECORD_COUNT_D_ID_VALID        0x0fff0000000000
#define RECORD_COUNT_W_ID_VALID      0x0ff0000000000000
#define RECORD_COUNT_TABLE_ID_VALID  0xf000000000000000


struct simpleTransaction {
  size_t idx_;
  size_t metis_idx_;

  std::vector<uint64_t> keys;
  std::vector<bool> update;
  RouterTxnOps op;
  uint64_t size;
  uint64_t partition_id;    // for clay, it means the destination coordinator_id
  bool is_distributed;
  bool is_transmit_request; // only for clay
  uint64_t destination_coordinator;
};

struct ExecutionStep {
  size_t router_coordinator_id;
  RouterTxnOps ops;
};

struct data_pack {
  double timestamp;
  std::string template_name;
  int times;
  data_pack(const std::string& n, int t, double ts){
    timestamp = ts;
    template_name = n;
    t = times;
  }
};

} // namespace star
