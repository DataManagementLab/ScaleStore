#pragma once
// -------------------------------------------------------------------------------------
#include "AccessFunctors.hpp"
#include "BufferFrame.hpp"
#include "Defs.hpp"
#include "Guard.hpp"
#include "Partition.hpp"
#include "PartitionedQueue.hpp"
#include "scalestore/profiling/counters/WorkerCounters.hpp"
#include "scalestore/rdma/CommunicationManager.hpp"
#include "scalestore/rdma/messages/Messages.hpp"
#include "scalestore/syncprimitives/HybridLatch.hpp"
#include "scalestore/threads/Worker.hpp"
#include "scalestore/utils/MemoryManagement.hpp"
#include "scalestore/utils/Parallelize.hpp"
#include "scalestore/utils/FNVHash.hpp"
// -------------------------------------------------------------------------------------
#include <cstdint>
#include <mutex>
#include <stack>
#include <vector>
// -------------------------------------------------------------------------------------

namespace scalestore
{
namespace profiling
{
struct BMCounters;
}
namespace rdma
{
struct MessageHandler;
}
namespace storage
{
struct PageProvider;
struct BuffermanagerSampler;
// -------------------------------------------------------------------------------------
struct PageTable {
   // -------------------------------------------------------------------------------------
   static inline uint64_t Rotr64(uint64_t x, std::size_t n) {
      return (((x) >> n) | ((x) << (64 - n)));
   }
   // -------------------------------------------------------------------------------------
   static inline uint64_t FasterHash(uint64_t input) {
      uint64_t local_rand = input;
      uint64_t local_rand_hash = 8;
      local_rand_hash = 40343 * local_rand_hash + ((local_rand) & 0xFFFF);
      local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 16) & 0xFFFF);
      local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 32) & 0xFFFF);
      local_rand_hash = 40343 * local_rand_hash + (local_rand >> 48);
      local_rand_hash = 40343 * local_rand_hash;
      return local_rand_hash; // if 64 do not rotate
      // else:
      // return Rotr64(local_rand_hash, 56);
   }
   // ---------------------------------------------------------------------------------- ---
   const uint64_t size;
   utils::HugePages<BufferFrame>& bfs; // complete buffer frame array
   // -------------------------------------------------------------------------------------
   PageTable(uint64_t numberBuckets, utils::HugePages<BufferFrame>& bfs) : size(Helper::nextPowerTwo(numberBuckets)), bfs(bfs) {
      ensure(numberBuckets < bfs.get_size());
   }
   // -------------------------------------------------------------------------------------
   inline BufferFrame& operator[](PID pid) { return bfs[FasterHash(pid) & (size - 1)]; }
};
// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------
class Buffermanager
{
   // -------------------------------------------------------------------------------------
   friend rdma::MessageHandler;
   friend storage::PageProvider;
   friend storage::BuffermanagerSampler;
   // -------------------------------------------------------------------------------------
  public:
   Buffermanager(rdma::CM<rdma::InitMessage>& cm, NodeID nodeId, s32 ssd_fd);
   ~Buffermanager() noexcept;
   // -------------------------------------------------------------------------------------
   // Deleted constructors
   //! Move constructor
   Buffermanager(Buffermanager&& other) noexcept = delete;
   //! Copy assignment operator
   Buffermanager& operator=(const Buffermanager& other) = delete;
   //! Move assignment operator
   Buffermanager& operator=(Buffermanager&& other) noexcept = delete;
   //! Copy constructor
   Buffermanager(const Buffermanager& other) = delete;
   // -------------------------------------------------------------------------------------
   // Counter specific functions
   // todo: sample one partition and multiplex?
   inline uint64_t getConsumedPages()
   {
      auto consumedPages = pidFreeList.approx_size();
      if (consumedPages == 0) throw std::runtime_error("No PIDs left in PID free list!");
      return (ssdSlotsSize - consumedPages);
   }
   inline uint64_t getFreePages()
   {
      return pageFreeList.approx_size();
   }
   // 
   inline uint64_t getFreeFrames()
   {
      auto freeFrames = frameFreeList.approx_size();
      if (freeFrames == 0) throw std::runtime_error("No frames left in free list!");
      return freeFrames;
   }
   inline uint64_t getDramPoolSize() { return dramPoolNumberPages; };
   inline uint64_t getSSDSlots() { return ssdSlotsSize; };
   inline uint64_t getNumberBufferframes() { return bufferFrames; };
   inline utils::HugePages<BufferFrame>&  getBufferframes() { return bfs; };
   inline uint64_t getGlobalEpoch() { return globalEpoch.load(); }
   // -------------------------------------------------------------------------------------
   template <typename ACCESS>
   Guard fix(PID pid, ACCESS functor);
   // -------------------------------------------------------------------------------------
   BufferFrame& newPage();
   BufferFrame& newRemotePage(NodeID nodeId);  // allocates page on a remote BM
   void readPageSync(PID pid, uint8_t* destination);
   void reclaimPage(BufferFrame& frame);
   void writeAllPages();  // do not execute worker in between
   // -------------------------------------------------------------------------------------
   void reportHashTableStats();
   // -------------------------------------------------------------------------------------
   // global epoch for LRU approximation
   std::atomic<uint64_t> globalEpoch{0};
  // private: // for testing
   // -------------------------------------------------------------------------------------
   // dram page pool
   const uint64_t dramPoolSize = 0;
   const uint64_t dramPoolNumberPages = 0;  // pageslots
   const uint64_t ssdSlotsSize = 0;
   std::vector<std::pair<uint64_t,Page*>> dramPagePool; // number of pages in partition and ptr;
   // Page* dramPagePool;                // INIT
   const uint64_t bufferFrames = 0;         // number bfs
   utils::HugePages<BufferFrame> bfs;
   PageTable pTable;              // page table maps PID to BufferFrame
   const NodeID nodeId;
   const s32 ssd_fd;
   // -------------------------------------------------------------------------------------
   // free lists
   // -------------------------------------------------------------------------------------
   PartitionedQueue<BufferFrame*,PARTITIONS,BATCH_SIZE, utils::Stack> frameFreeList;
   PartitionedQueue<Page*,PARTITIONS,BATCH_SIZE, utils::Stack> pageFreeList;
   PartitionedQueue<PID,PARTITIONS,BATCH_SIZE, utils::Stack> pidFreeList;
   // -------------------------------------------------------------------------------------
   // private helper functions
   // -------------------------------------------------------------------------------------
   template <typename InitFunc>
   BufferFrame& insertFrame(PID pid,InitFunc&& init_bf_func){
      auto& bucket_bf =pTable[pid];
      auto& ht_latch = bucket_bf.ht_bucket_latch; 
   restart:
      // -------------------------------------------------------------------------------------
      RESTART(!ht_latch.tryLatchExclusive() , restart);
      if(bucket_bf.state == BF_STATE::FREE){ // important to set it only to free when we have the HT latch or are done with the frame
         init_bf_func(bucket_bf);
         ht_latch.unlatchExclusive();
         ensure(bucket_bf.latch.isLatched());
         return bucket_bf;
      }
      // -------------------------------------------------------------------------------------
      BufferFrame** f = &bucket_bf.next;
      while(*f){
         if ((*f)->pid == pid) { throw std::runtime_error("Duplicate pid"); }
         f = &(*f)->next;
      }
      // -------------------------------------------------------------------------------------
      if (!frameFreeList.try_pop(*f, threads::ThreadContext::my().bf_handle)){
         ht_latch.unlatchExclusive();
         goto restart;
      }
      // -------------------------------------------------------------------------------------
      init_bf_func(**f);
      ht_latch.unlatchExclusive();
      ensure((**f).latch.isLatched());
      return **f;
   }

   // insert latched frame
   template <typename PageFunc>
   void removeFrame(BufferFrame& frame, PageFunc handle_page_func) {
      ensure(frame.latch.isLatched()); 
      // fast path check if by chance the frame we want to delete is the bucket then we save the hash function
      if (frame.isHtBucket) {
         // -------------------------------------------------------------------------------------
         while(!frame.ht_bucket_latch.tryLatchExclusive())
            ;
         // -------------------------------------------------------------------------------------
         ensure(frame.state != BF_STATE::FREE); 
         handle_page_func(frame);
         frame.reset();
         frame.ht_bucket_latch.unlatchExclusive();
         frame.latch.unlatchExclusive();
         return;
      }
      // -------------------------------------------------------------------------------------
      // otherwise hash
      auto& bucket_bf = pTable[frame.pid];
      auto& ht_latch = bucket_bf.ht_bucket_latch; 
   restart:
      // -------------------------------------------------------------------------------------
      RESTART(!ht_latch.tryLatchExclusive(), restart);
      // -------------------------------------------------------------------------------------
      BufferFrame** f = &bucket_bf.next;
      while (*f) {
         if ((*f)->pid == frame.pid) {
            *f = (*f)->next;  // remove frame
            ensure(frame.state != BF_STATE::FREE); 
            handle_page_func(frame);
            frame.reset();
            ht_latch.unlatchExclusive();
            frame.latch.unlatchExclusive();
            frameFreeList.try_push(&frame,threads::ThreadContext::my().bf_handle);
            return;
         }
         f = &(*f)->next; // iterate
      }
      throw std::runtime_error("removeFrame failed - page not in buffer"); 
   }
   // -------------------------------------------------------------------------------------
   template <CONTENTION_METHOD method, typename ACCESS>
   Guard findFrame(PID pid, ACCESS functor, NodeID nodeId_);
   // find frame or insert function 
   template <CONTENTION_METHOD method, typename ACCESS>
   Guard findFrameOrInsert(PID pid, ACCESS functor, NodeID nodeId_);
   // -------------------------------------------------------------------------------------

};
// -------------------------------------------------------------------------------------
class BM
{
  public:
   static Buffermanager* global;
};
// -------------------------------------------------------------------------------------
// TEMPLATE FUNCTIONS
// -------------------------------------------------------------------------------------
#include "Buffermanager.tpp"
// -------------------------------------------------------------------------------------

}  // namespace storage
}  // namespace scalestore
