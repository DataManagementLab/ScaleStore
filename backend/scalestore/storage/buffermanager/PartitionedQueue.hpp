#pragma once
#include "scalestore/utils/BatchQueue.hpp"
#include "scalestore/utils/RandomGenerator.hpp"

namespace scalestore {
namespace storage {

// Attention: Best over-allocate space due to half full batches
template <typename T, size_t Partitions, size_t BatchSize, template <class, size_t> class Container>
struct PartitionedQueue {
   static_assert(Partitions && ((Partitions & (Partitions - 1)) == 0), "Partitions not power of 2");
   // -------------------------------------------------------------------------------------
   using B = utils::Batch<T, BatchSize, Container>;
   // using Batch = Container<T, BatchSize>;
   using BQueue = utils::BatchQueue<T, BatchSize, Container>;
   // -------------------------------------------------------------------------------------
   struct BatchHandle {
      B* batch = nullptr;
      BQueue* queue = nullptr;

      ~BatchHandle() {
         if (batch != nullptr) {
            if (batch->container.empty()) {
               queue->push_empty(batch);  // return full batch to full queue
            } else {
               queue->push_full(batch);  // return full batch to full queue
            }
            batch = nullptr;
            queue = nullptr;
         }
      }
   };
   // -------------------------------------------------------------------------------------
   std::vector<std::unique_ptr<BQueue>> queues;  // std::vector<std::unique_ptr<BatchQueue>> queues;
   // -------------------------------------------------------------------------------------
   // tl data
   inline static thread_local uint64_t seed = 0;
   // -------------------------------------------------------------------------------------
   PartitionedQueue(size_t expectedElements) {
      uint64_t queueSize = (expectedElements / Partitions);
      queues.reserve(Partitions);
      for (uint64_t p_i = 0; p_i < Partitions; p_i++) {
         queues.emplace_back(std::make_unique<BQueue>(queueSize));
      }
   }
   // -------------------------------------------------------------------------------------
   void push(const T& e, BatchHandle& b_handle) {
      while (!try_push(e, b_handle))
         ;
   }
   // -------------------------------------------------------------------------------------
   bool try_push(const T& e, BatchHandle& b_handle) {
      if (b_handle.batch == nullptr) {
         auto p_id = utils::RandomGenerator::getRandU64Fast() & (Partitions - 1);
         auto rc = queues[p_id]->try_pop_empty(b_handle.batch);
         b_handle.queue = queues[p_id].get();
         if (!rc) {
            b_handle.batch = nullptr;
            b_handle.queue = nullptr;
            return false;
         }
      }
      // -------------------------------------------------------------------------------------
      if (b_handle.batch->container.try_push(e)) [[likely]] return true;
      // -------------------------------------------------------------------------------------
      b_handle.queue->push_full(b_handle.batch);  // cannot fail
      // -------------------------------------------------------------------------------------
      // get empty batch
      auto p_id = utils::RandomGenerator::getRandU64Fast() & (Partitions - 1);
      auto rc = queues[p_id]->try_pop_empty(b_handle.batch);
      b_handle.queue = queues[p_id].get();
      if (rc) return b_handle.batch->container.try_push(e);
      // -------------------------------------------------------------------------------------
      for (auto& q_ptr : queues) {
         if (q_ptr->try_pop_empty(b_handle.batch)) {
            b_handle.queue = q_ptr.get();
            return b_handle.batch->container.try_push(e);
         }
      }
      b_handle.batch = nullptr;
      b_handle.queue = nullptr;
      return false;
   }
   // -------------------------------------------------------------------------------------
   T pop(BatchHandle& b_handle){
      T result;
      while(!try_pop(result,b_handle))
         ;
      return result;
   }
   // -------------------------------------------------------------------------------------
   bool try_pop(T& e, BatchHandle& b_handle) {
      if (b_handle.batch == nullptr) {
         auto p_id = utils::RandomGenerator::getRandU64Fast() & (Partitions - 1);
         auto rc = queues[p_id]->try_pop_full(b_handle.batch);
         b_handle.queue = queues[p_id].get();
         if (!rc) {
            b_handle.batch = nullptr;
            b_handle.queue = nullptr;
            return false;
         }
      }
      // -------------------------------------------------------------------------------------
      if (b_handle.batch->container.try_pop(e)) [[likely]] return true;
      // -------------------------------------------------------------------------------------
      assert(b_handle.batch->container.get_size() == 0);
      b_handle.queue->push_empty(b_handle.batch);
      // -------------------------------------------------------------------------------------
      // get full batch
      auto p_id = utils::RandomGenerator::getRandU64Fast() & (Partitions - 1);
      if (queues[p_id]->try_pop_full(b_handle.batch)) {
         b_handle.queue = queues[p_id].get();
         return b_handle.batch->container.try_pop(e);
      }
      // -------------------------------------------------------------------------------------
      for (auto& q_ptr : queues) {
         if (q_ptr->try_pop_full(b_handle.batch)) {
            b_handle.queue = q_ptr.get();
            return b_handle.batch->container.try_pop(e);
         }
      }
      b_handle.batch = nullptr;
      b_handle.queue = nullptr;
      return false;
   }


   // -------------------------------------------------------------------------------------
   // util functions
   // -------------------------------------------------------------------------------------
   void assert_no_duplicates() {
      for (auto& q_ptr : queues) {
         q_ptr->assert_no_duplicates();
      }
   }
   // -------------------------------------------------------------------------------------
   void reset() {
      for (auto& q_ptr : queues) {
         q_ptr->reset();
      }
   }
   // -------------------------------------------------------------------------------------
   void assert_no_leaks() {
      for (auto& q_ptr : queues) {
         q_ptr->assert_no_leaks();
      }
   }
   // -------------------------------------------------------------------------------------
   uint64_t approx_size() {
      uint64_t size = 0;
      std::for_each(std::begin(queues), std::end(queues), [&](auto& queue) { size += queue->approx_size(); });
      return size;
   }
   // -------------------------------------------------------------------------------------
   // cheap random function

   // -------------------------------------------------------------------------------------
};

}  // storage
}  // scalestore
