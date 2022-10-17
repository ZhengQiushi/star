//
// Created by Yi Lu on 9/11/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include <glog/logging.h>

namespace star {

class SiloRWKey {
public:
  // local index read bit

  void set_local_index_read_bit() {
    clear_local_index_read_bit();
    bitvec |= LOCAL_INDEX_READ_BIT_MASK << LOCAL_INDEX_READ_BIT_OFFSET;
  }

  void clear_local_index_read_bit() {
    bitvec &= ~(LOCAL_INDEX_READ_BIT_MASK << LOCAL_INDEX_READ_BIT_OFFSET);
  }

  uint32_t get_local_index_read_bit() const {
    return (bitvec >> LOCAL_INDEX_READ_BIT_OFFSET) & LOCAL_INDEX_READ_BIT_MASK;
  }

  // read respond bit

  void set_read_respond_bit() {
    clear_read_respond_bit();
    bitvec |= READ_RESPOND_BIT_MASK << READ_RESPOND_BIT_OFFSET;
  }

  void clear_read_respond_bit() {
    bitvec &= ~(READ_RESPOND_BIT_MASK << READ_RESPOND_BIT_OFFSET);
  }

  uint32_t get_read_respond_bit() const {
    return (bitvec >> READ_RESPOND_BIT_OFFSET) & READ_RESPOND_BIT_MASK;
  }

  // write request bit

  void set_write_request_bit() {
    clear_write_request_bit();
    bitvec |= WRITE_REQUEST_BIT_MASK << WRITE_REQUEST_BIT_OFFSET;
  }

  void clear_write_request_bit() {
    bitvec &= ~(WRITE_REQUEST_BIT_MASK << WRITE_REQUEST_BIT_OFFSET);
  }

  uint32_t get_write_request_bit() const {
    return (bitvec >> WRITE_REQUEST_BIT_OFFSET) & WRITE_REQUEST_BIT_MASK;
  }

  // read request bit

  void set_read_request_bit() {
    clear_read_request_bit();
    bitvec |= READ_REQUEST_BIT_MASK << READ_REQUEST_BIT_OFFSET;
  }

  void clear_read_request_bit() {
    bitvec &= ~(READ_REQUEST_BIT_MASK << READ_REQUEST_BIT_OFFSET);
  }

  uint32_t get_read_request_bit() const {
    return (bitvec >> READ_REQUEST_BIT_OFFSET) & READ_REQUEST_BIT_MASK;
  }

  // write lock bit
  void set_write_lock_bit() {
    clear_write_lock_bit();
    bitvec |= WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET;
  }

  void clear_write_lock_bit() {
    bitvec &= ~(WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
  }

  bool get_write_lock_bit() const {
    return (bitvec >> WRITE_LOCK_BIT_OFFSET) & WRITE_LOCK_BIT_MASK;
  }

  // table id

  void set_table_id(uint32_t table_id) {
    DCHECK(table_id < (1 << 5));
    clear_table_id();
    bitvec |= table_id << TABLE_ID_OFFSET;
  }

  void clear_table_id() { bitvec &= ~(TABLE_ID_MASK << TABLE_ID_OFFSET); }

  uint32_t get_table_id() const {
    return (bitvec >> TABLE_ID_OFFSET) & TABLE_ID_MASK;
  }

  // dynamic coordinator id
  void set_dynamic_coordinator_id(uint32_t dynamic_coordinator_id) {
    DCHECK(dynamic_coordinator_id < (1 << 5));
    clear_dynamic_coordinator_id();
    bitvec |= dynamic_coordinator_id << DYNAMIC_COORDINATOR_ID_OFFSET;
  }

  void clear_dynamic_coordinator_id() { bitvec &= ~(DYNAMIC_COORDINATOR_ID_MASK << DYNAMIC_COORDINATOR_ID_OFFSET); }

  uint32_t get_dynamic_coordinator_id() const {
    return (bitvec >> DYNAMIC_COORDINATOR_ID_OFFSET) & DYNAMIC_COORDINATOR_ID_MASK;
  }

  // secondary coordinator id
  void set_router_value(uint32_t dynamic_coordinator_id, uint64_t secondary_coordinator_id) {
    router_val.set_dynamic_coordinator_id(dynamic_coordinator_id);
    router_val.copy_secondary_coordinator_id(secondary_coordinator_id);
  }

  const RouterValue* const get_router_value() const {
    return &router_val;
  }

  // partition id

  void set_partition_id(uint32_t partition_id) {
    DCHECK(partition_id < (1 << 8));
    clear_partition_id();
    bitvec |= partition_id << PARTITION_ID_OFFSET;
  }

  void clear_partition_id() {
    bitvec &= ~(PARTITION_ID_MASK << PARTITION_ID_OFFSET);
  }

  uint32_t get_partition_id() const {
    return (bitvec >> PARTITION_ID_OFFSET) & PARTITION_ID_MASK;
  }

  // tid
  uint64_t get_tid() const { return tid; }

  void set_tid(uint64_t tid) { this->tid = tid; }

  // key
  void set_key(const void *key) { this->key = key; }

  const void *get_key() const { return key; }

  // value
  void set_value(void *value) { this->value = value; }

  void *get_value() const { return value; }

private:
  /*
   * A bitvec is a 32-bit word.
   *
   * [ table id (5) ] | partition id (8) | 
   *   dynamic replica coordinator id (5) | secondary replica coordinator id(5) | unused bit (5) |
   *   read respond bit(1)
   *   write lock bit(1) | read request bit (1) | local index read (1)  ]
   *
   * write lock bit is set when a write lock is acquired.
   * read request bit is set when the read response is received.
   * local index read  is set when the read is from a local read only index.
   *
   */

  uint32_t bitvec = 0;
  RouterValue router_val;
  uint64_t tid = 0;
  const void *key = nullptr;
  void *value = nullptr;

public:
  static constexpr uint32_t TABLE_ID_MASK = 0x1f;
  static constexpr uint32_t TABLE_ID_OFFSET = 28;

  static constexpr uint32_t PARTITION_ID_MASK = 0xff;
  static constexpr uint32_t PARTITION_ID_OFFSET = 20;

  static constexpr uint32_t DYNAMIC_COORDINATOR_ID_MASK = 0x1f;
  static constexpr uint32_t DYNAMIC_COORDINATOR_ID_OFFSET = 15;

  static constexpr uint32_t SECONDARY_COORDINATOR_ID_BIT_MASK = 0x1f;
  static constexpr uint32_t SECONDARY_COORDINATOR_ID_BIT_OFFSET = 10;

  static constexpr uint32_t READ_RESPOND_BIT_MASK = 0x1;
  static constexpr uint32_t READ_RESPOND_BIT_OFFSET = 4;

  static constexpr uint32_t WRITE_LOCK_BIT_MASK = 0x1;
  static constexpr uint32_t WRITE_LOCK_BIT_OFFSET = 3;

  static constexpr uint32_t READ_REQUEST_BIT_MASK = 0x1;
  static constexpr uint32_t READ_REQUEST_BIT_OFFSET = 2;

  static constexpr uint32_t WRITE_REQUEST_BIT_MASK = 0x1;
  static constexpr uint32_t WRITE_REQUEST_BIT_OFFSET = 1;

  static constexpr uint32_t LOCAL_INDEX_READ_BIT_MASK = 0x1;
  static constexpr uint32_t LOCAL_INDEX_READ_BIT_OFFSET = 0;
};
} // namespace star
