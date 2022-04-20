#pragma once
#include "Guard.hpp"
// -------------------------------------------------------------------------------------
namespace scalestore {
namespace storage {
// -------------------------------------------------------------------------------------
// Access Functors
struct Exclusive {
   LATCH_STATE type = LATCH_STATE::EXCLUSIVE;

   void operator()(Guard& g, NodeID nodeId) {
      // -------------------------------------------------------------------------------------
      // Optimistic
      // -------------------------------------------------------------------------------------
      auto version = g.frame->latch.optimisticLatchOrRestart();
      if (!version.has_value() || (g.frame->mhWaiting)) {
         g.state = STATE::RETRY;
         g.latchState = LATCH_STATE::UNLATCHED;
         return;
      }
      // -------------------------------------------------------------------------------------
      if (g.frame->possession != POSSESSION::EXCLUSIVE || !(g.frame->isPossessor(nodeId))) {
         g.state = (g.frame->pid.getOwner() == nodeId) ? STATE::LOCAL_POSSESSION_CHANGE : STATE::REMOTE_POSSESSION_CHANGE;
      } else
         g.state = STATE::INITIALIZED;
      // -------------------------------------------------------------------------------------
      // Exclusive
      // -------------------------------------------------------------------------------------
      if (!g.frame->latch.optimisticUpgradeToExclusive(version.value())){
         g.state = STATE::RETRY;
         g.vAcquired = g.frame->latch.version;
         g.latchState = LATCH_STATE::UNLATCHED;
         return;
      }

      g.vAcquired = g.frame->latch.version;
      g.latchState = LATCH_STATE::EXCLUSIVE;
   }

   void undo(Guard& g) {
      ensure(g.state != STATE::RETRY);
      ensure(g.frame->latch.isLatched());
      g.frame->latch.unlatchExclusive();
      g.state = STATE::UNINITIALIZED;
      g.latchState = LATCH_STATE::UNLATCHED;
   }
   // if page is already in desired state no op
};
struct Shared {
   LATCH_STATE type = LATCH_STATE::SHARED;
   void operator()(Guard& g, NodeID nodeId) {
      // -------------------------------------------------------------------------------------
      // Optimistic
      // -------------------------------------------------------------------------------------
      auto version = g.frame->latch.optimisticLatchOrRestart();

      if (!version.has_value() || (g.frame->mhWaiting)) {
         g.latchState = LATCH_STATE::UNLATCHED;
         g.state = STATE::RETRY;
         return;
      }
      // -------------------------------------------------------------------------------------
      // can be shared or exclusive as long as we are in possession
      if (!(g.frame->isPossessor(nodeId))) {
         g.state = (g.frame->pid.getOwner() == nodeId) ? STATE::LOCAL_POSSESSION_CHANGE : STATE::REMOTE_POSSESSION_CHANGE;

      } else
         g.state = STATE::INITIALIZED;
      // -------------------------------------------------------------------------------------
      if (g.state != STATE::INITIALIZED) {  // latch exclusive to process other stages
         if (!g.frame->latch.optimisticUpgradeToExclusive(version.value())){
            g.state = STATE::RETRY;
            g.latchState = LATCH_STATE::UNLATCHED;
         }else{
            g.latchState = LATCH_STATE::EXCLUSIVE;
         }
         g.vAcquired = g.frame->latch.version;
         return;
      }

      if (!g.frame->latch.optimisticUpgradeToShared(version.value())) {
         g.state = STATE::RETRY;
         g.vAcquired = g.frame->latch.version;
         g.latchState = LATCH_STATE::UNLATCHED;
         return;
      }
      g.vAcquired = g.frame->latch.version;
      g.latchState = LATCH_STATE::SHARED;
   }

   void undo(Guard& g) {
      if (g.latchState == LATCH_STATE::EXCLUSIVE) {
         ensure(g.state != STATE::RETRY);
         ensure(g.frame->latch.isLatched());
         g.frame->latch.unlatchExclusive();
         g.state = STATE::UNINITIALIZED;
         g.latchState = LATCH_STATE::UNLATCHED;
      } else {
         ensure(g.latchState == LATCH_STATE::SHARED);
         ensure(g.state != STATE::RETRY);
         ensure(!g.frame->latch.isLatched());
         g.frame->latch.unlatchShared();
         g.state = STATE::UNINITIALIZED;
         g.latchState = LATCH_STATE::UNLATCHED;
      }
   }
};
struct Optimistic {
   LATCH_STATE type = LATCH_STATE::OPTIMISTIC;

   void operator()(Guard& g, NodeID nodeId) {
      // -------------------------------------------------------------------------------------
      // Optimistic
      // -------------------------------------------------------------------------------------
      auto version = g.frame->latch.optimisticLatchOrRestart();

      if (!version.has_value() || (g.frame->mhWaiting)) {
         g.state = STATE::RETRY;
         g.latchState = LATCH_STATE::UNLATCHED;
         return;
      }
      // -------------------------------------------------------------------------------------
      // can be shared or exclusive as long as we are in possession
      if (!(g.frame->isPossessor(nodeId))) {
         g.state = (g.frame->pid.getOwner() == nodeId) ? STATE::LOCAL_POSSESSION_CHANGE : STATE::REMOTE_POSSESSION_CHANGE;
      } else
         g.state = STATE::INITIALIZED;
      // -------------------------------------------------------------------------------------
      if (g.state != STATE::INITIALIZED) {  // latch exclusive to process other stages
         if (!g.frame->latch.optimisticUpgradeToExclusive(version.value()))
            g.state = STATE::RETRY;
         else {
            g.latchState = LATCH_STATE::EXCLUSIVE;
            g.vAcquired = g.frame->latch.version;
         }
         return;
      }
      // optimistic
      if (!g.frame->latch.optimisticCheckOrRestart(version.value())) g.state = STATE::RETRY;
      g.vAcquired = version.value();
      g.latchState = LATCH_STATE::OPTIMISTIC;
   }

   void undo(Guard& g) {
      if (g.latchState == LATCH_STATE::EXCLUSIVE) {
         ensure(g.state != STATE::RETRY);
         ensure(g.frame->latch.isLatched());
         g.frame->latch.unlatchExclusive();
         g.state = STATE::UNINITIALIZED;
         g.latchState = LATCH_STATE::UNLATCHED;
      } else {
         g.state = STATE::UNINITIALIZED;
         g.latchState = LATCH_STATE::UNLATCHED;
      }
   }
};
}  // namespace storage
}  // namespace scalestore
