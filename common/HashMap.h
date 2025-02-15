//
// Created by Yi Lu on 7/14/18.
//

#pragma once

#include "SpinLock.h"
#include <atomic>
#include <glog/logging.h>
#include <unordered_map>

namespace star {
/**
 * 先hash，再map，减小树的大小！
 * 
*/
template <std::size_t N, class KeyType, class ValueType> class HashMap {
public:
  using hasher = typename std::unordered_map<KeyType, ValueType>::hasher;

  bool remove(const KeyType &key) {
    return _applyAt(
        [&key](std::unordered_map<KeyType, ValueType> &map) {
          auto it = map.find(key);
          if (it == map.end()) {
            return false;
          } else {
            map.erase(it);
            return true;
          }
        },
        bucketNo(key));
  }

  bool contains(const KeyType &key) {
    return _applyAt(
        [&key](const std::unordered_map<KeyType, ValueType> &map) {
          return map.find(key) != map.end();
        },
        bucketNo(key));
  }

  bool insert(const KeyType &key, const ValueType &value) {
    return _applyAt(
        [&key, &value](std::unordered_map<KeyType, ValueType> &map) {
          if (map.find(key) != map.end()) {
            return false;
          }
          map[key] = value;
          return true;
        },
        bucketNo(key));
  }

  ValueType &operator[](const KeyType &key) {
    return _applyAtRef(
        [&key](std::unordered_map<KeyType, ValueType> &map) -> ValueType & {
          return map[key];
        },
        bucketNo(key));
  }

  std::size_t size() {
    return _fold(0, [](std::size_t totalSize,
                       const std::unordered_map<KeyType, ValueType> &map) {
      return totalSize + map.size();
    });
  }

  void clear() {
    _map([](std::unordered_map<KeyType, ValueType> &map) { map.clear(); });
  }

  const KeyType get_global_key(const int32_t &local_key) {
    /**
     * @brief 返回当前分区，第local_key个数据的全局key
    */
    int32_t count = 0;
    KeyType result;
    for(size_t i = 0 ; i < N ; i ++ ){
      locks_[i].lock();

      std::unordered_map<KeyType, ValueType> &cur_maps = maps_[i];
      typename std::unordered_map<KeyType, ValueType>::iterator it = cur_maps.begin();
      while(it != cur_maps.end()){
        // 
        if(count == local_key){
          break;
        } else {
          count ++;
        }
      }
      locks_[i].unlock();
      if(count == local_key){
        result = it->first;
        break;
      }
    }
  
    return result;
  }
private:
  template <class ApplyFunc>
  auto &_applyAtRef(ApplyFunc applyFunc, std::size_t i) {
    DCHECK(i < N) << "index " << i << " is greater than " << N;
    locks_[i].lock();
    auto &result = applyFunc(maps_[i]);
    locks_[i].unlock();
    return result;
  }

  template <class ApplyFunc> auto _applyAt(ApplyFunc applyFunc, std::size_t i) {
    DCHECK(i < N) << "index " << i << " is greater than " << N;
    locks_[i].lock();
    auto result = applyFunc(maps_[i]);
    locks_[i].unlock();
    return result;
  }

  template <class MapFunc> void _map(MapFunc mapFunc) {
    for (auto i = 0u; i < N; i++) {
      locks_[i].lock();
      mapFunc(maps_[i]);
      locks_[i].unlock();
    }
  }

  template <class T, class FoldFunc>
  auto _fold(const T &firstValue, FoldFunc foldFunc) {
    T finalValue = firstValue;
    for (auto i = 0u; i < N; i++) {
      locks_[i].lock();
      finalValue = foldFunc(finalValue, maps_[i]);
      locks_[i].unlock();
    }
    return finalValue;
  }

  auto bucketNo(const KeyType &key) { return hasher_(key) % N; }

private:
  hasher hasher_;
  std::unordered_map<KeyType, ValueType> maps_[N];
  SpinLock locks_[N];
};

} // namespace star
