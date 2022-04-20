#pragma once
#include "scalestore/utils/MPMCQueue.hpp"
#include "BufferFrame.hpp"
// -------------------------------------------------------------------------------------
namespace scalestore {
namespace storage {
struct Partition{
   // optimized rigtorp queue with power of two arithmetic
   // however, this queue is memory hungry to prevent false sharing thus add batching?
   // -------------------------------------------------------------------------------------
   template <typename T>
   struct MPMPCQueueWrapper{
      rigtorp::MPMCQueue<T,true> q;
      std::atomic<u64> elements {0};
      // -------------------------------------------------------------------------------------
      MPMPCQueueWrapper(u64 size) : q(size){};

      u64 approxSize(){
         return elements.load();
      }
      // push
      void push(T e){
         q.push(e);
         elements++;
      };
      // pop

      T pop(){
         T e;
         q.pop(e);
         elements--;
         return e;
      }      
   };
   // -------------------------------------------------------------------------------------
   MPMPCQueueWrapper<BufferFrame*> frameFreeList;
   MPMPCQueueWrapper<Page*> pageFreeList;
   MPMPCQueueWrapper<PID> pidFreeList;
   // -------------------------------------------------------------------------------------
   Partition(u64 freeFramesSize, u64 freePagesSize, u64 freePIDsSize);
};
// -------------------------------------------------------------------------------------
}  // storage
}  // scalestore
