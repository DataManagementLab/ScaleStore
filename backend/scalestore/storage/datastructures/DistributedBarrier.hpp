#pragma once
// -------------------------------------------------------------------------------------

namespace scalestore {
namespace storage {
struct DistributedBarrier {
   struct Body {
      void initialize(std::size_t threads)
      {
         threadCount = threads;
         cntr = threadCount;
         round = 0;
      };
      std::size_t threadCount;
      alignas(64) std::size_t cntr;
      alignas(64) uint8_t round;
   };

   PID pid;
   
   explicit DistributedBarrier(uint64_t threads){
      ExclusiveBFGuard guard;
      auto& body = guard.allocate<Body>();
      guard.getFrame().epoch = (~uint64_t(0)); // max to never evict barrier
      body.initialize(threads);
      ensure(body.threadCount == threads);
      pid = guard.getFrame().pid;
   }
   
   explicit DistributedBarrier(PID pid): pid(pid){}

   template <typename F>
   bool wait(F finalizer)
   {
      uint8_t prevRound;
      {
         ExclusiveBFGuard guard(pid);
         auto& barrier = guard.as<Body>(0);
         guard.getFrame().epoch = (~uint64_t(0)); // max to never evict barrier
         prevRound = barrier.round;  // Must happen before fetch_sub
         auto prev = barrier.cntr--;
         if (prev == 1) {
            // last thread arrived
            barrier.cntr = barrier.threadCount;
            auto r = finalizer();
            barrier.round++;
            return r;
         }
      }
      
      while(true){
         OptimisticBFGuard guard(pid); // must be optimistic such that other nodes can increment 
         auto* barrier = guard.asPtr<Body>(0);
         guard.getFrame().epoch = (~uint64_t(0)); // max to never evict barrier
         // -------------------------------------------------------------------------------------
         if(guard.retry()) continue;
         auto round = barrier->round;
         if(guard.retry()) continue;
         // -------------------------------------------------------------------------------------
         if(round != prevRound){
            return false;
         }
         // -------------------------------------------------------------------------------------
         auto rndSleep = utils::RandomGenerator::getRandU64(1,100); //
         for(uint64_t s_i = 0; s_i < rndSleep; s_i++){
            _mm_pause();
         }
         // -------------------------------------------------------------------------------------
      }
   }
   
   inline bool wait()
   {
      return wait([]() { return true; });
   }
};

}  // storage
}  // scalestore
