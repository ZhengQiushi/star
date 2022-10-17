//
// Created by qiushi
//

#pragma once

#include <queue>
#include <mutex>          // std::mutex, std::lock_guard

namespace star {
namespace group_commit {

template <class T, std::size_t N = 4096> class ShareQueue {
  public:
    size_t size(){
      return queue.size();
    }
    bool push_no_wait(const T& val){
      if(size() > N){
        return false;
      } else {
        std::lock_guard<std::mutex> l(lock);
        queue.push(val);
        return true;
      }
    }
    bool pop_no_wait(T& val){
      if(size() == 0){
        return false;
      } else {
        std::lock_guard<std::mutex> l(lock);
        val = queue.front();
        queue.pop();
        return true;
      }
    }
    T pop_no_wait(bool& success){
      T ret;
      if(size() == 0){
        success = false;
      } else {
        success = true;
        std::lock_guard<std::mutex> l(lock);
        ret = queue.front();
        queue.pop();
      }
      return ret;
    }
  private:
    std::mutex lock;
    std::queue<T> queue;
};

} // namespace group_commit

} // namespace star