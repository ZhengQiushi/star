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
  RouterValue(){
    // pass
  }
  RouterValue(uint64_t dynamic_coordinator_id, uint64_t secondary_coordinator_id){
    set_dynamic_coordinator_id(dynamic_coordinator_id);
    // set_secondary_coordinator_id(dynamic_coordinator_id);
    set_secondary_coordinator_id(secondary_coordinator_id);
  }
  // dynamic coordinator id
  void set_dynamic_coordinator_id(uint64_t dynamic_coordinator_id) {
    DCHECK(dynamic_coordinator_id < (1 << 8));
    
    if(secondary_cnt > 0){
      set_secondary_coordinator_id(get_dynamic_coordinator_id());
    }
    
    clear_dynamic_coordinator_id();
    bitvec |= dynamic_coordinator_id << DYNAMIC_COORDINATOR_ID_BIT_OFFSET;
    // 判断是否存在于 secondary 中

    // uint64_t secondary_coordinator_ids = get_secondary_coordinator_ids();
    if(count_secondary_coordinator_id(dynamic_coordinator_id)){
      // 
      clear_secondary_coordinator_id(dynamic_coordinator_id);
      // 在lru中移出它
      clear_lru_secondary(dynamic_coordinator_id);
    }
  }

  void clear_dynamic_coordinator_id() { bitvec &= ~(DYNAMIC_COORDINATOR_ID_BIT_MASK << DYNAMIC_COORDINATOR_ID_BIT_OFFSET); }

  uint64_t get_dynamic_coordinator_id() const {
    return (bitvec >> DYNAMIC_COORDINATOR_ID_BIT_OFFSET) & DYNAMIC_COORDINATOR_ID_BIT_MASK;
  }

  void copy_secondary_coordinator_id(uint64_t secondary_coordinator_id) {
    // 
    clear_secondary_coordinator_ids();
    bitvec |= secondary_coordinator_id << SECONDARY_COORDINATOR_IDS_BIT_OFFSET;
    // 构造lru
    lru_bitvec = 0;
  }

  // secondary coordinator id
  void set_secondary_coordinator_id(uint64_t secondary_coordinator_id) {
    // LOW BIT -> SMALL C_ID
    if(count_secondary_coordinator_id(secondary_coordinator_id)){
      return;
    }
    DCHECK(secondary_coordinator_id < (1 << 8));
    bitvec |= (1 << secondary_coordinator_id) << SECONDARY_COORDINATOR_IDS_BIT_OFFSET;
    secondary_cnt ++;
    if(secondary_cnt > MAX_CNT){
      // 取出最高位  
      uint64_t remove_id = ((1 << 8) - 1) & (lru_bitvec >> 8 * (MAX_CNT - 1)); 
      clear_secondary_coordinator_id(remove_id);
    } 
    lru_bitvec <<= 8;
    lru_bitvec |= secondary_coordinator_id;
    lru_bitvec &= LRU_BITVEC_BIT_MASK;
  }

  void clear_secondary_coordinator_id(uint64_t secondary_coordinator_id) {
    DCHECK(secondary_coordinator_id < (1 << 8));
    uint64_t tmp = 0xffffffffffffffff;
    tmp ^= (1 << secondary_coordinator_id) << SECONDARY_COORDINATOR_IDS_BIT_OFFSET;
    bitvec &= tmp;
    secondary_cnt -= 1;
  }
  void clear_secondary_coordinator_ids() { 
    bitvec &= ~(SECONDARY_COORDINATOR_IDS_BIT_MASK << SECONDARY_COORDINATOR_IDS_BIT_OFFSET); 
    lru_bitvec = 0;
    secondary_cnt = 0;
  }

  uint64_t get_secondary_coordinator_ids() const {
    return (bitvec >> SECONDARY_COORDINATOR_IDS_BIT_OFFSET) & SECONDARY_COORDINATOR_IDS_BIT_MASK;
  }
  
  bool count_secondary_coordinator_id(uint64_t secondary_coordinator_id) const {
    return ((get_secondary_coordinator_ids() >> secondary_coordinator_id) & 1);
  }


  static bool contain_secondary_coordinator_id(uint64_t secondary_coordinator_ids, uint64_t coordinator_id) {
    return ((secondary_coordinator_ids >> coordinator_id) & 1);
  }

  std::unordered_set<int> get_secondary_coordinator_id_array() {
    std::unordered_set<int> ret; 
    uint64_t tmp = get_secondary_coordinator_ids();
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
    uint64_t tmp = get_secondary_coordinator_ids();
    for(int i = 0; i < 64 - 8; i ++ ){
        if(tmp & 1){
            ret += std::to_string(i) + " ";
        }
        tmp = tmp >> 1;
    }
    ret += "]";
    return ret;// (bitvec >> SECONDARY_COORDINATOR_ID_BIT_OFFSET) & SECONDARY_COORDINATOR_ID_BIT_MASK;
  }
  void clear_lru_secondary(uint64_t coordinator_id){
      for(int i = 0 ; i < MAX_CNT; i ++ ){
        uint64_t cur_ = (lru_bitvec >> (i * 8)) & ((1 << 8) - 1);
        if(cur_ == coordinator_id){
          // the left side should be shift
          // | a | b | c |
          //       x
          //   
          
          uint64_t tmp = lru_bitvec & LRU_BITVEC_BIT_MASK;
          tmp &= (LRU_BITVEC_BIT_MASK << ((i + 1) * 8));  // | a | 0 | 0 |
          tmp &= LRU_BITVEC_BIT_MASK;
          tmp >>= 8;                                      // | 0 | a | 0 |

          uint64_t clear_bit_mask = ((1 << (i * 8)) - 1); 
          lru_bitvec &= clear_bit_mask; // keep c         // | 0 | 0 | c |
          lru_bitvec |= tmp;
          //
          break;
        }
      }
  }

private:
  /*
   * A bitvec is a 64-bit word.
   *
   * [ secondary_coordinator (64 - 8) | 
   *   dynamic_coordinator_id (8)]
   *
   */

  /***
   * 
   * [lru_bit_vec]  8bit | 8bit | 8bit
   *               <------------------
   *               old               new
  */
  
  uint64_t bitvec = 0;
  uint64_t lru_bitvec = 0;
  uint64_t secondary_cnt = 0;

public:
  static constexpr uint64_t MAX_CNT = 3;

  static constexpr uint64_t SECONDARY_COORDINATOR_IDS_BIT_MASK = 0xffffffffffffff; // 64 - 8
  static constexpr uint64_t SECONDARY_COORDINATOR_IDS_BIT_OFFSET = 8;

  static constexpr uint64_t DYNAMIC_COORDINATOR_ID_BIT_MASK = 0xff;
  static constexpr uint64_t DYNAMIC_COORDINATOR_ID_BIT_OFFSET = 0;
  // 

  static constexpr uint64_t LRU_BITVEC_BIT_MASK = (((1 << (8 * MAX_CNT)) - 1));
};
} // namespace star
