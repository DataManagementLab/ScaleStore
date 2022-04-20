#include "BufferFrameGuards.hpp"
// -------------------------------------------------------------------------------------

namespace scalestore {
namespace storage {
// -------------------------------------------------------------------------------------
// ExclusiveBFGuard
// -------------------------------------------------------------------------------------
// new Page

ExclusiveBFGuard::ExclusiveBFGuard(){
   g.frame = &BM::global->newPage();
   g.frame->dirty = true;
   g.latchState = LATCH_STATE::EXCLUSIVE;
   g.state = STATE::INITIALIZED;
   g.vAcquired = g.frame->latch.version;
};

ExclusiveBFGuard::ExclusiveBFGuard(NodeID remoteNodeId){
   g.frame = &BM::global->newRemotePage(remoteNodeId);
   g.frame->dirty = true;
   g.latchState = LATCH_STATE::EXCLUSIVE;
   g.state = STATE::INITIALIZED;
   g.vAcquired = g.frame->latch.version;
}

ExclusiveBFGuard::ExclusiveBFGuard(PID pid){
   g = BM::global->fix(pid, Exclusive());
   g.frame->dirty = true;
};
ExclusiveBFGuard::ExclusiveBFGuard(ExclusiveBFGuard&& xGuard) {
   g = std::move(xGuard.g);
}
ExclusiveBFGuard& ExclusiveBFGuard::operator=(ExclusiveBFGuard&& xGuard){
   g = std::move(xGuard.g);
   return *this;
}
// -------------------------------------------------------------------------------------
ExclusiveBFGuard::ExclusiveBFGuard(SharedBFGuard&& sGuard){
   ensure(sGuard.g.latchState == LATCH_STATE::SHARED);
   ensure(g.state == STATE::UNINITIALIZED);
   // -------------------------------------------------------------------------------------
   PID pid = sGuard.getFrame().pid;
   // -------------------------------------------------------------------------------------
   sGuard.g.state = STATE::MOVED;
   sGuard.g.latchState = LATCH_STATE::UNLATCHED;
   sGuard.getFrame().latch.unlatchShared();
   // -------------------------------------------------------------------------------------
   // fix again
   // check version
   // check pointer
   g =  BM::global->fix(pid, Exclusive());
   ensure(g.state == STATE::INITIALIZED);
   // check ptr could be changed due to remote 
   if((&getFrame() != &sGuard.getFrame())){
      g.state = STATE::RETRY;
   }
   // check version must be updated now 
   if((g.vAcquired != sGuard.g.vAcquired + 0b10)){
      g.state = STATE::RETRY;
   }

   if(g.state != STATE::RETRY){
      g.frame->dirty = true;
   }
}
// -------------------------------------------------------------------------------------
// O - X
ExclusiveBFGuard::ExclusiveBFGuard(OptimisticBFGuard&& oGuard) {
   ensure(oGuard.g.latchState == LATCH_STATE::OPTIMISTIC);
   ensure(g.state == STATE::UNINITIALIZED);
   // -------------------------------------------------------------------------------------
   PID pid = oGuard.getFrame().pid;
   // -------------------------------------------------------------------------------------
   oGuard.g.state = STATE::MOVED;
   oGuard.g.latchState = LATCH_STATE::UNLATCHED;
   // -------------------------------------------------------------------------------------
   g.state = STATE::RETRY;
   // -------------------------------------------------------------------------------------
   if (!oGuard.retry()) {
      g.state = STATE::INITIALIZED;
      // -------------------------------------------------------------------------------------
      g = BM::global->fix(pid, Exclusive());
      ensure(g.state == STATE::INITIALIZED);
      ensure(g.latchState == LATCH_STATE::EXCLUSIVE);
      // -------------------------------------------------------------------------------------
      // check ptr could be changed due to remote
      if ((&getFrame() != &oGuard.getFrame())) {
         g.state = STATE::RETRY;
      }
      // check version must be updated now
      if ((g.vAcquired != oGuard.g.vAcquired + 0b10)) {
         g.state = STATE::RETRY;
      }      
   }
   
   if(g.state != STATE::RETRY){
      g.frame->dirty = true;
   }

   if (g.state == STATE::RETRY && g.latchState == LATCH_STATE::EXCLUSIVE) {
      ensure(g.frame->latch.isLatched());
      g.frame->latch.unlatchExclusive();
      g.latchState = LATCH_STATE::UNLATCHED;
   }
   // -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
void ExclusiveBFGuard::reclaim(){
   BM::global->reclaimPage(*g.frame);
   g.state = STATE::UNINITIALIZED;
   g.latchState = LATCH_STATE::UNLATCHED;
}
// -------------------------------------------------------------------------------------
ExclusiveBFGuard::~ExclusiveBFGuard() {


   if(g.state == STATE::INITIALIZED && g.latchState == LATCH_STATE::EXCLUSIVE){
      assert(g.frame->latch.isLatched());
      g.frame->latch.unlatchExclusive();
      g.state = STATE::UNINITIALIZED;
      g.latchState = LATCH_STATE::UNLATCHED;
   }   
}
// -------------------------------------------------------------------------------------
// SharedBFGuard
// -------------------------------------------------------------------------------------
SharedBFGuard::SharedBFGuard(PID pid)
{
   g = BM::global->fix(pid, Shared());
};
// -------------------------------------------------------------------------------------
SharedBFGuard::SharedBFGuard(SharedBFGuard&& sGuard) {
   g = std::move(sGuard.g);
}
SharedBFGuard& SharedBFGuard::operator=(SharedBFGuard&& sGuard){
   g = std::move(sGuard.g);
   return *this;
}
// downgrade X - S
// no need to update remote possession information
SharedBFGuard::SharedBFGuard(ExclusiveBFGuard&& xGuard) {
   ensure(xGuard.g.latchState == LATCH_STATE::EXCLUSIVE);
   ensure(g.state == STATE::UNINITIALIZED);
   ensure(xGuard.getFrame().latch.isLatched());
   // -------------------------------------------------------------------------------------
   g = std::move(xGuard.g);
   // -------------------------------------------------------------------------------------
   g.frame->latch.unlatchExclusive();
   // -------------------------------------------------------------------------------------
   // try lock shared
   g.state = STATE::RETRY;
   if(getFrame().latch.tryLatchShared()){
      if((getFrame().latch.version == (g.vAcquired + 0b10))){
         g.state = STATE::INITIALIZED;
         g.latchState = LATCH_STATE::SHARED;
         g.vAcquired = g.vAcquired + 0b10;
      }else{
         getFrame().latch.unlatchShared();   
      }
   }
}
//
// upgrade O - S
SharedBFGuard::SharedBFGuard(OptimisticBFGuard&& oGuard)
{
   ensure(oGuard.g.latchState == LATCH_STATE::OPTIMISTIC);
   ensure(g.state == STATE::UNINITIALIZED);
   // -------------------------------------------------------------------------------------
   g = std::move(oGuard.g);  // copy
   // -------------------------------------------------------------------------------------
   // try lock shared
   g.state = STATE::RETRY;
   if (getFrame().latch.tryLatchShared()) {
      if ((getFrame().latch.version == (g.vAcquired))) {
         g.state = STATE::INITIALIZED;
         g.latchState = LATCH_STATE::SHARED;
      } else {
         getFrame().latch.unlatchShared();
      }
   }
}
SharedBFGuard::~SharedBFGuard(){
   if(g.state == STATE::INITIALIZED && g.latchState == LATCH_STATE::SHARED){
      g.frame->latch.unlatchShared();
      g.state = STATE::INITIALIZED;
      g.latchState = LATCH_STATE::UNLATCHED;
   }

}
// -------------------------------------------------------------------------------------
// OptimisticBFGuard
// -------------------------------------------------------------------------------------
OptimisticBFGuard::OptimisticBFGuard(PID pid)
{
   g = BM::global->fix(pid, Optimistic());
};
OptimisticBFGuard::OptimisticBFGuard(OptimisticBFGuard&& oGuard){
   g = std::move(oGuard.g);
}
OptimisticBFGuard& OptimisticBFGuard::operator=(OptimisticBFGuard&& oGuard){
   g = std::move(oGuard.g);
   return *this;
}
// -------------------------------------------------------------------------------------
// downgrades
// no need to account for remote possession information
OptimisticBFGuard::OptimisticBFGuard(ExclusiveBFGuard&& xGuard)
{
   ensure(xGuard.g.latchState == LATCH_STATE::EXCLUSIVE);
   ensure(g.state == STATE::UNINITIALIZED);
   ensure(xGuard.getFrame().latch.isLatched());
   // -------------------------------------------------------------------------------------
   g = std::move(xGuard.g);
   // -------------------------------------------------------------------------------------
   g.frame->latch.unlatchExclusive();
   // -------------------------------------------------------------------------------------
   // try lock shared
   g.state = STATE::RETRY;
   if (getFrame().latch.optimisticCheckOrRestart(g.vAcquired + 0b10)) {
      g.state = STATE::INITIALIZED;
      g.latchState = LATCH_STATE::OPTIMISTIC;
      g.vAcquired = g.vAcquired + 0b10;
   }
}
// -------------------------------------------------------------------------------------
OptimisticBFGuard::OptimisticBFGuard(SharedBFGuard&& sGuard)
{
   ensure(sGuard.g.latchState == LATCH_STATE::SHARED);
   ensure(g.state == STATE::UNINITIALIZED);
   // -------------------------------------------------------------------------------------
   g = std::move(sGuard.g);
   // -------------------------------------------------------------------------------------
   g.frame->latch.unlatchShared();
   // -------------------------------------------------------------------------------------
   // try lock shared
   g.state = STATE::RETRY;
   if (getFrame().latch.optimisticCheckOrRestart(g.vAcquired)) {
      g.state = STATE::INITIALIZED;
      g.latchState = LATCH_STATE::OPTIMISTIC;
      g.vAcquired = g.vAcquired;
   }
}
// -------------------------------------------------------------------------------------
}  // storage
}  // scalestore

