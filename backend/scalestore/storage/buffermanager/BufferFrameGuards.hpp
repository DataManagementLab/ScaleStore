#pragma once
// -------------------------------------------------------------------------------------
#include "Buffermanager.hpp"
// -------------------------------------------------------------------------------------

namespace scalestore
{
namespace storage
{
struct SharedBFGuard;
struct OptimisticBFGuard;

struct ExclusiveBFGuard {
   Guard g;
   ExclusiveBFGuard();  // creates new page
   ExclusiveBFGuard(PID pid);
   explicit ExclusiveBFGuard(NodeID remoteNodeId); // allocates page on remote node
   // -------------------------------------------------------------------------------------
   ~ExclusiveBFGuard();
   // -------------------------------------------------------------------------------------
   ExclusiveBFGuard(ExclusiveBFGuard&& xGuard);
   ExclusiveBFGuard& operator=(ExclusiveBFGuard&& xGuard);
   // -------------------------------------------------------------------------------------
   // upgrade shared guards to exclusive guard
   // S - X does not work because of sha
   ExclusiveBFGuard(SharedBFGuard&& sGuard);
   // O - X
   ExclusiveBFGuard(OptimisticBFGuard&& oGuard);
   // -------------------------------------------------------------------------------------
   ExclusiveBFGuard& operator=(ExclusiveBFGuard& other) = delete;
   ExclusiveBFGuard(ExclusiveBFGuard& other) = delete;  // copy constructor
   // -------------------------------------------------------------------------------------
   BufferFrame& getFrame() { return *g.frame; }
   Version getVersion() { return g.vAcquired; }
   STATE getState() { return g.state; }
   template <typename T>
   T& as(uint64_t idx)
   {
      return ((g.frame->page->getTuple<T>(idx)));
   }

   template <typename T>
   T& allocate()
   {
      return *(new (g.frame->page->begin()) T);
   }
   
   template <typename T>
   T* asPtr(uint64_t idx)
   {      
      auto* page = g.frame->page;
      return &((page->getTuple<T>(idx)));
   }
   
   void reclaim();
   bool retry()
   {
      if (g.state == STATE::RETRY)
         return true;
      return false;
   }
   void swapWith(ExclusiveBFGuard& other){
      g.swapWith(other.g);
   }
};

struct SharedBFGuard {
   Guard g;
   SharedBFGuard(PID pid);
   ~SharedBFGuard();
   // -------------------------------------------------------------------------------------
   SharedBFGuard(SharedBFGuard&& sGuard);
   SharedBFGuard& operator=(SharedBFGuard&& sGuard);
   
   // downgrade X - S
   // no need to update remote possession information
   SharedBFGuard(ExclusiveBFGuard&& xGuard);
   // -------------------------------------------------------------------------------------
   // upgrade O - S
   // check possession information because optimistic coul have changed
   SharedBFGuard(OptimisticBFGuard&& oGuard);
   // -------------------------------------------------------------------------------------
   SharedBFGuard& operator=(SharedBFGuard& other) = delete;
   SharedBFGuard(SharedBFGuard& other) = delete;  // copy constructor
   // -------------------------------------------------------------------------------------
   BufferFrame& getFrame() { return *g.frame; }
   Version getVersion() { return g.vAcquired; }
   STATE getState() { return g.state; }
   template <typename T>
   T& as(uint64_t idx)
   {
      return ((g.frame->page->getTuple<T>(idx)));
   }
   bool retry()
   {
      if (g.state == STATE::RETRY)
         return true;
      return false;
   }



};

struct OptimisticBFGuard {
   Guard g;
   OptimisticBFGuard(PID pid);
   // -------------------------------------------------------------------------------------
   OptimisticBFGuard(OptimisticBFGuard&& oGuard);
   OptimisticBFGuard& operator=(OptimisticBFGuard&& oGuard);
   // -------------------------------------------------------------------------------------
   // downgrades
   // no need to account for remote possession information
   OptimisticBFGuard(ExclusiveBFGuard&& xGuard);
   OptimisticBFGuard(SharedBFGuard&& sGuard);
   // -------------------------------------------------------------------------------------
   OptimisticBFGuard& operator=(OptimisticBFGuard& other) = delete;
   OptimisticBFGuard(OptimisticBFGuard& other) = delete;  // copy constructor
   // -------------------------------------------------------------------------------------
   BufferFrame& getFrame() { return *g.frame; }
   Version getVersion() { return g.vAcquired; }
   STATE getState() { return g.state; }

   // template <typename T>
   // T& as(uint64_t idx)
   // {
   //    // special care must me taken because of optimistic nature
   //    return ((g.frame->page->getTuple<T>(idx)));
   // }

   template <typename T>
   T* asPtr(uint64_t idx)
   {      
      auto* page = g.frame->page;
      if(retry()) return nullptr;
      return &((page->getTuple<T>(idx)));
   }
   bool retry()
   {
      if (g.state == STATE::RETRY)
         return true;

      if (!getFrame().latch.optimisticCheckOrRestart(g.vAcquired)) {
         g.state = STATE::RETRY;
         return true;
      }
      return false;
   }
};
}  // namespace storage
}  // namespace scalestore
