//
// Created by qiushi
//

#pragma once

#include <queue>
#include <mutex>          // std::mutex, std::lock_guard

namespace star {
// namespace group_commit {

template <class T, std::size_t N = 409600> class ShareQueue {
  public:
    size_t size(){
      return queue.size();
    }
    bool push_no_wait(const T& val){
      std::lock_guard<std::mutex> l(lock);
      if(size() > N){
        return false;
      } else {
        queue.push(val);
        return true;
      }
    }
    bool pop_no_wait(T& val){
      std::lock_guard<std::mutex> l(lock);
      if(size() == 0){
        return false;
      } else {
        val = queue.front();
        queue.pop();
        return true;
      }
    }
    T pop_no_wait(bool& success){
      T ret;
      std::lock_guard<std::mutex> l(lock);
      if(size() == 0){
        success = false;
      } else {
        success = true;
        ret = queue.front();
        queue.pop();
      }
      return ret;
    }
    void clear(){
      std::lock_guard<std::mutex> l(lock);

      while(!queue.empty()){
        queue.pop();
      }

    }
  private:
    std::mutex lock;
    std::queue<T> queue;
};

// } // namespace group_commit

} // namespace star