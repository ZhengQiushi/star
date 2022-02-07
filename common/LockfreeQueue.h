//
// Created by Yi Lu on 8/29/18.
//

#pragma once
#include <thread>
#include "glog/logging.h"
#include <boost/lockfree/spsc_queue.hpp>
// #include <immintrin.h>

#if defined(__i386__) || defined(__x86_64__)
#  define SPINLOCK_YIELD __asm volatile("pause" : :)
#else
#  define SPINLOCK_YIELD std::this_thread::yield()
#endif

namespace star {

/*
 * boost::lockfree::spsc_queue does not support move only objects, e.g.,
 * std::unique_ptr<T>. As a result, only Message* can be pushed into
 * MessageQueue. Usage: std::unique_ptr<Message> ptr; MessageQueue q;
 * q.push(ptr.release());
 *
 * std::unique_ptr<Message> ptr1(q.front());
 * q.pop();
 *
 */

template <class T, std::size_t N = 1024>
class LockfreeQueue
    : public boost::lockfree::spsc_queue<T, boost::lockfree::capacity<N>> {
public:
  using element_type = T;
  using base_type =
      boost::lockfree::spsc_queue<T, boost::lockfree::capacity<N>>;

  void push(const T &value) {
    while (base_type::write_available() == 0) {
      nop_pause();
    }
    bool ok = base_type::push(value);
    CHECK(ok);
  }

  void wait_till_non_empty() {
    while (base_type::empty()) {
      nop_pause();
    }
  }

  bool wait_till_non_empty_timeout() {
    while (base_type::empty()) {
      nop_pause();
    }
    return true;
  }

  auto capacity() { return N; }

private:
  void nop_pause() { 
    // SPINLOCK_YIELD;
    __asm__ volatile("nop" : :);
     }
};
} // namespace star

