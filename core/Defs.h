//
// Created by Yi Lu on 9/10/18.
//

#pragma once

namespace star {

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
  EXIT
};

enum myTestSet {
  YCSB,
  TPCC
};

enum class TransactionResult { COMMIT, READY_TO_COMMIT, ABORT, ABORT_NORETRY };

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

} // namespace star
