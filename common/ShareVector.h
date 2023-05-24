// //
// // Created by qiushi
// //

// #pragma once

// #include <vector>
// #include <mutex>          // std::mutex, std::lock_guard

// namespace star {
// // namespace group_commit {

// template <class T> class ShareVector: public std::vector {
//   public:
//      push_back(const T& val){
//       std::lock_guard<std::mutex> l(lock);
//       base_type::push_back(val);
//     }
//     bool pop_no_wait(T& val){
//       std::lock_guard<std::mutex> l(lock);
//       if(size() == 0){
//         return false;
//       } else {
//         val = queue.front();
//         queue.pop();
//         return true;
//       }
//     }
//     T pop_no_wait(bool& success){
//       T ret;
//       std::lock_guard<std::mutex> l(lock);
//       if(size() == 0){
//         success = false;
//       } else {
//         success = true;
//         ret = queue.front();
//         queue.pop();
//       }
//       return ret;
//     }
//     void clear(){
//       std::lock_guard<std::mutex> l(lock);

//       while(!queue.empty()){
//         queue.pop();
//       }

//     }
//   private:
//     std::mutex lock;
//     std::vector<T> vec;
// };

// // } // namespace group_commit

// } // namespace star