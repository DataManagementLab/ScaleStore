#pragma once
// -------------------------------------------------------------------------------------
#include "../../syncprimitives/HybridLatch.hpp"
#include "../datastructures/Bitmap.hpp"
#include "Defs.hpp"
#include "Page.hpp"
// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace storage
{
// -------------------------------------------------------------------------------------
// Which node posses the bufferframe and in which mode
// -------------------------------------------------------------------------------------
union Possessors {
   NodeID exclusive;
   StaticBitmap<64> shared;
};

enum class POSSESSION : uint8_t {
   NOBODY = 0,  // INIT & FREE STATE
   EXCLUSIVE = 1,
   SHARED = 2,
};

// -------------------------------------------------------------------------------------
// STATES
// -------------------------------------------------------------------------------------
enum class BF_STATE : uint8_t {
   FREE = 0,
   HOT = 1,
   EVICTED = 2,          // evicted but still installed in HT because of remote ownership
   IO_RDMA = 3,           
   IO_SSD = 4,
   
   INVALIDATION_EXPECTED = 8,   
};

// -------------------------------------------------------------------------------------
struct BufferFrame {
   PID pid = EMPTY_PID;          // INIT
   BufferFrame* next = nullptr;  // Intrusive Datastructures
   Page* page = nullptr;
   std::atomic<uint64_t>  pVersion = 0;
   std::atomic<uint64_t> epoch {EMPTY_EPOCH}; // used
   OptimisticLatch ht_bucket_latch;
   Possessors possessors = {0};  // active uinion is managed by POSSESSION
   std::atomic<BF_STATE> state = BF_STATE::FREE; // important as it protects the ht buckets
   std::atomic<bool> mhWaiting = false;
   std::atomic<bool> cooling = false;
   bool dirty = false;
   bool isHtBucket = false; // inlined bf into HT
   POSSESSION possession = POSSESSION::NOBODY;
   HybridLatch latch;

   // -------------------------------------------------------------------------------------
   void fill(PID pid, Page* page, POSSESSION possession)
   {
      this->pid = pid;
      this->page = page;
      this->possession = possession;
   }
   // -------------------------------------------------------------------------------------
   void reset()
   {
      pVersion = 0;
      pid = EMPTY_PID;
      if(!isHtBucket)
         next = nullptr;
      page = nullptr;
      possession = POSSESSION::NOBODY;
      possessors = {0};
      mhWaiting = false;
      epoch = EMPTY_EPOCH;
      dirty = false;
      state = BF_STATE::FREE;
   }
   // -------------------------------------------------------------------------------------
   // Protocol possessor information
   // -------------------------------------------------------------------------------------
   void setPossession(POSSESSION possession_) { possession = possession_; }

   void setPossessor(NodeID nodeId)
   {
      if (possession == POSSESSION::NOBODY)
         throw std::runtime_error("Object not in possesion");
      else if (possession == POSSESSION::EXCLUSIVE)
         possessors.exclusive = nodeId;
      else if (possession == POSSESSION::SHARED)
         possessors.shared.set(nodeId);
   }

   
   // -------------------------------------------------------------------------------------
   bool isPossessor(NodeID nodeId)
   {
      if (possession == POSSESSION::EXCLUSIVE)
         return (possessors.exclusive == nodeId);
      else if (possession == POSSESSION::SHARED)
         return possessors.shared.test(nodeId);
      return false;
   }
   
   bool isDirty(){
      return dirty;
   }

   void setDirty(){
      dirty = true;
   }
};
// -------------------------------------------------------------------------------------
static_assert(sizeof(BufferFrame) <= 128, "Bufferframe spans more than two cachelines");
}  // namespace storage
}  // namespace scalestore
