#pragma once
#include "BufferFrame.hpp"

namespace scalestore {
namespace storage {

enum class STATE : uint8_t{
   UNINITIALIZED, SSD, REMOTE, LOCAL_POSSESSION_CHANGE, REMOTE_POSSESSION_CHANGE, RETRY, INITIALIZED, MOVED, NOT_FOUND,
};
enum class CONTENTION_METHOD { BLOCKING, NON_BLOCKING};
enum class LATCH_STATE { UNLATCHED, OPTIMISTIC, SHARED, EXCLUSIVE };
struct Guard{
   Version vAcquired = 0; // version at which the guard was acquired
   BufferFrame* frame = nullptr;
   STATE state = STATE::UNINITIALIZED;
   CONTENTION_METHOD method = CONTENTION_METHOD::BLOCKING;
   LATCH_STATE latchState = LATCH_STATE::UNLATCHED;

   Guard() = default;
   Guard(Guard&& other)    noexcept : vAcquired(other.vAcquired), frame(other.frame),state(other.state), method(other.method), latchState(other.latchState) // move constructor
    {
       other.state = STATE::MOVED;
    }
 
   Guard& operator=(Guard&& other) noexcept // move assignment
    {
       // destruct visible ressources
       if(latchState == LATCH_STATE::EXCLUSIVE)
          frame->latch.unlatchExclusive();
       if(latchState == LATCH_STATE::SHARED)
          frame->latch.unlatchShared();

       vAcquired = other.vAcquired;
       frame =  other.frame;
       state =  other.state;
       method = other.method;
       latchState = other.latchState;

       other.vAcquired = 0;
       other.frame = nullptr;
       other.latchState = LATCH_STATE::UNLATCHED;
       other.state = STATE::MOVED; // is this ok?
       return *this;
    }
   // -------------------------------------------------------------------------------------
   Guard& operator=(Guard& other) = delete;
   Guard(Guard& other) = delete;  // copy constructor 
   // -------------------------------------------------------------------------------------

   void swapWith(Guard& other)
   {
      std::swap(vAcquired, other.vAcquired);
      std::swap(frame, other.frame);
      std::swap(state, other.state);
      std::swap(method, other.method);
      std::swap(latchState, other.latchState);
   }
   // downgrade and upgrade check remote state   


   bool needDowngrade(LATCH_STATE desired){
      if((desired == LATCH_STATE::EXCLUSIVE) && (desired != latchState))
         throw std::runtime_error("Would need latch update ");
      return (desired != latchState);
   }

   bool downgrade(LATCH_STATE desired)
   {
      if (desired == LATCH_STATE::EXCLUSIVE) {
         ensure(desired == latchState);
         return true;
         
      } else if (desired == LATCH_STATE::SHARED) {
         // ATTENTION
         // we have a bug in glibc https://sourceware.org/bugzilla/show_bug.cgi?id=23861
         // due some nodes beeing ubuntu 18.04
         // therefore we currently cannot downgrade
         ensure(frame->latch.isLatched());
         auto v = frame->latch.version.load();
         frame->latch.unlatchExclusive();
         if(!frame->latch.tryLatchShared()){
            latchState = LATCH_STATE::UNLATCHED;
            state = STATE::RETRY;
            return false;
         }
         // frame->latch.latchShared();
         if( (v + 2) != frame->latch.version){
            frame->latch.unlatchShared();
            vAcquired = v+2;
            latchState = LATCH_STATE::UNLATCHED;
            state = STATE::RETRY;
            return false;
         }
         vAcquired = frame->latch.version;
         latchState = LATCH_STATE::SHARED;
         return true;
      } else if (desired == LATCH_STATE::OPTIMISTIC) {
         ensure(vAcquired == frame->latch.version);
         if (latchState == LATCH_STATE::EXCLUSIVE){
            vAcquired = frame->latch.downgradeExclusiveToOptimistic();
            latchState = LATCH_STATE::OPTIMISTIC; 
         }else if (latchState == LATCH_STATE::SHARED){
            vAcquired = frame->latch.downgradeSharedToOptimistic();
            latchState = LATCH_STATE::OPTIMISTIC; 
         }
      }
      return true;
   }

   // if we have an optimistic guard we need to check retry method?
   void unlock(){
      if(latchState == LATCH_STATE::EXCLUSIVE){
         ensure(frame->latch.isLatched());
         frame->latch.unlatchExclusive();
         state = STATE::UNINITIALIZED;
      }
      else if(latchState == LATCH_STATE::SHARED){
         frame->latch.unlatchShared();
         state = STATE::UNINITIALIZED;
      }
      else if(latchState == LATCH_STATE::OPTIMISTIC){
         if(!frame->latch.optimisticReadUnlatchOrRestart(vAcquired)){
            state = STATE::RETRY;
         }
      }
      latchState = LATCH_STATE::UNLATCHED;
      return;
   }
};
   
}  // storage
}  // scalestore
