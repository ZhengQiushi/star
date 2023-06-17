//
// Created by Yi Lu on 9/11/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>
#include <set>
#include <unordered_set>
#include <glog/logging.h>

namespace star {

class RouterValue {
public:
  RouterValue(){}
  ~RouterValue(){}
  
  // dynamic coordinator id
  void set_dynamic_coordinator_id(uint64_t dynamic_coordinator_id) {
    DCHECK(dynamic_coordinator_id < (1 << 8));
    clear_dynamic_coordinator_id();
    bitvec |= dynamic_coordinator_id << DYNAMIC_COORDINATOR_ID_BIT_OFFSET;
  }

  void clear_dynamic_coordinator_id() { bitvec &= ~(DYNAMIC_COORDINATOR_ID_BIT_MASK << DYNAMIC_COORDINATOR_ID_BIT_OFFSET); }

  uint64_t get_dynamic_coordinator_id() const {
    return (bitvec >> DYNAMIC_COORDINATOR_ID_BIT_OFFSET) & DYNAMIC_COORDINATOR_ID_BIT_MASK;
  }

  void copy_secondary_coordinator_id(uint64_t secondary_coordinator_id) {
    clear_secondary_coordinator_id();
    bitvec |= secondary_coordinator_id << SECONDARY_COORDINATOR_IDS_BIT_OFFSET;
  }

  // secondary coordinator id
  void set_secondary_coordinator_id(uint64_t secondary_coordinator_id) {
    // LOW BIT -> SMALL C_ID
    DCHECK(secondary_coordinator_id < (1 << 8));
    bitvec |= (1 << secondary_coordinator_id) << SECONDARY_COORDINATOR_IDS_BIT_OFFSET;
  }

  void clear_secondary_coordinator_id() { 
    bitvec &= ~(SECONDARY_COORDINATOR_IDS_BIT_MASK << SECONDARY_COORDINATOR_IDS_BIT_OFFSET); 
  }

  uint64_t get_secondary_coordinator_id() const {
    return (bitvec >> SECONDARY_COORDINATOR_IDS_BIT_OFFSET) & SECONDARY_COORDINATOR_IDS_BIT_MASK;
  }
  
  bool count_secondary_coordinator_id(uint64_t secondary_coordinator_id) const {
    return ((get_secondary_coordinator_id() >> secondary_coordinator_id) & 1);
  }

  std::unordered_set<int> get_secondary_coordinator_id_array() {
    std::unordered_set<int> ret; 
    uint64_t tmp = get_secondary_coordinator_id();
    for(int i = 0; i < 64 - 8; i ++ ){
        if(tmp & 1){
            ret.insert(i);
        }
        tmp = tmp >> 1;
    }
    return ret;// (bitvec >> SECONDARY_COORDINATOR_ID_BIT_OFFSET) & SECONDARY_COORDINATOR_ID_BIT_MASK;
  }

  std::string get_secondary_coordinator_id_printed() const {
    std::string ret; 
    ret += "[";
    uint64_t tmp = get_secondary_coordinator_id();
    for(int i = 0; i < 64 - 8; i ++ ){
        if(tmp & 1){
            ret += std::to_string(i) + " ";
        }
        tmp = tmp >> 1;
    }
    ret += "]";
    return ret;// (bitvec >> SECONDARY_COORDINATOR_ID_BIT_OFFSET) & SECONDARY_COORDINATOR_ID_BIT_MASK;
  }


private:
  /*
   * A bitvec is a 64-bit word.
   *
   * [ secondary_coordinator (64 - 8) | 
   *   dynamic_coordinator_id (8)]
   *
   */

  uint64_t bitvec = 0;

public:
  static constexpr uint64_t SECONDARY_COORDINATOR_IDS_BIT_MASK = 0xffffffffffffff; // 64 - 8
  static constexpr uint64_t SECONDARY_COORDINATOR_IDS_BIT_OFFSET = 8;

  static constexpr uint64_t DYNAMIC_COORDINATOR_ID_BIT_MASK = 0xff;
  static constexpr uint64_t DYNAMIC_COORDINATOR_ID_BIT_OFFSET = 0;
};
} // namespace star
