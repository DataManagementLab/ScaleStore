#pragma once 
#include "scalestore/storage/buffermanager/Page.hpp"
#include "scalestore/storage/buffermanager/BufferFrame.hpp" 
#include "scalestore/storage/buffermanager/PartitionedQueue.hpp"
// -------------------------------------------------------------------------------------
namespace scalestore {
namespace threads {
// ensure that every thread in scalestore gets this thread context initialized 
struct ThreadContext{
   // -------------------------------------------------------------------------------------
   static inline thread_local ThreadContext* tlsPtr = nullptr;
   static inline ThreadContext& my() {
      return *ThreadContext::tlsPtr;
   }
   // -------------------------------------------------------------------------------------
   storage::PartitionedQueue<PID, PARTITIONS, BATCH_SIZE, utils::Stack>::BatchHandle pid_handle;
   storage::PartitionedQueue<storage::BufferFrame*, PARTITIONS, BATCH_SIZE, utils::Stack>::BatchHandle bf_handle;
   storage::PartitionedQueue<storage::Page*, PARTITIONS, BATCH_SIZE, utils::Stack>::BatchHandle page_handle;

// debug stack can be used like this
//               threads::ThreadContext::my().debug_stack.push({{"lookup tuple in b-tree"},PID(0),start,0,0});
//    struct DebugInfo{
//       std::string_view msg;
//       PID pid;
//       uint64_t version;
//       uint64_t g_epoch;
//       uintptr_t bf_ptr;
//    };
//    template <typename T, size_t N>
//    struct DebugStack {
//       void push(const T& e)
//          {
//             assert(size <= N);
//             if (full()) {
//                size = 0; // overwrite
//             }
//             buffer[size++] = e;
//          }

//       [[nodiscard]] bool try_pop(T& e)
//          {
//             if (empty()) {
//                return false;
//             }
//             e = buffer[--size];
//             return true;
//          }
//       bool empty() { return size == 0; }
//       bool full() { return size == (N); }
//       uint64_t get_size() { return size; }
//       void reset() {
// /*
//          for(auto& e: buffer){
//             e.msg = "";
//             e.pid.id =0;
//             e.version = 0;
//             e.g_epoch = 0;
//             e.bf_ptr = 0;
//          }
// */
//          size = 0;
//       }

//      private:
//       uint64_t size = 0;
//       std::array<T, N> buffer{};
//    }; 
//    DebugStack<DebugInfo,128> debug_stack;


};

}  // threads
}  // scalestore
