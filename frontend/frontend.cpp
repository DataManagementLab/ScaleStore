
#include "Defs.hpp"
#include "PerfEvent.hpp"
#include "scalestore/Config.hpp"
#include "scalestore/ScaleStore.hpp"
#include "scalestore/threads/Concurrency.hpp"
#include "scalestore/profiling/counters/WorkerCounters.hpp"
#include "scalestore/profiling/ProfilingThread.hpp"
#include "scalestore/utils/RandomGenerator.hpp"
#include "scalestore/utils/Time.hpp"

// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <iostream>
#include <chrono>
#include <fstream>
#include <random>
// -------------------------------------------------------------------------------------
using namespace scalestore;
using namespace storage;

DEFINE_uint64(RUNS, 1, "Increment Runs");


inline int intRand(const size_t & min, const size_t & max, const size_t seed) {
   static thread_local std::mt19937 generator(seed);
    std::uniform_int_distribution<int> distribution(min,max);
    return distribution(generator);
   }


static constexpr uint64_t hash(uint64_t k) {
   // MurmurHash64A
   const uint64_t m = 0xc6a4a7935bd1e995;
   const int r = 47;
   uint64_t h = 0x8445d61a4e774912 ^ (8 * m);
   k *= m;
   k ^= k >> r;
   k *= m;
   h ^= k;
   h *= m;
   h ^= h >> r;
   h *= m;
   h ^= h >> r;
   return h;
}



class rand_x { 
    unsigned int seed;
public:
    rand_x(int init) : seed(init) {}

    int operator()(int limit) {
        int divisor = RAND_MAX/(limit+1);
        int retval;

        do { 
            retval = rand_r(&seed) / divisor;
        } while (retval > limit);

        return retval;
    }        
};

constexpr uint64_t MAGIC_START_VALUE = 99; // start value for increments such that page is filled;


int main(int argc, char** argv)
{
   gflags::SetUsageMessage("ScaleStore Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   ScaleStore scalestore;


   uint64_t numberPages = (FLAGS_dramGB * 1024 * 1024 * 1024) / sizeof(storage::Page);
   uint64_t workingSet = numberPages * 0.1;
   std::vector<PID> pageIDs(workingSet);

   
   const uint64_t blockSize = workingSet / FLAGS_worker;
   ensure(blockSize > 0);
   scalestore.getWorkerPool().scheduleJobAsync(0, [&]() {
      for (size_t b_i = 0; b_i < workingSet; ++b_i) {
         // auto& frame = scalestore.getBuffermanager().newPage();
         storage::ExclusiveBFGuard guard;
         pageIDs[b_i] = guard.getFrame().pid;
         ensure(guard.getState() == STATE::INITIALIZED);
         ensure(guard.g.latchState == LATCH_STATE::EXCLUSIVE);
         ensure(!guard.retry());
         guard.getFrame().page->getTuple<uint64_t>(0) = MAGIC_START_VALUE;
      }
   });
   scalestore.getWorkerPool().joinAll();

   
   // fill other nodes page Ids to same vector
   std::vector<PID> completePIDs(pageIDs);
   for (auto p : pageIDs) {
      for (NodeID n_i = 0; n_i < FLAGS_nodes; n_i++) {
         if (n_i == scalestore.getNodeID())
            continue;
         completePIDs.push_back(PID(n_i, p.plainPID()));
      }
   }

   // -------------------------------------------------------------------------------------   
   do 
   {
      std::cout << '\n' << "Press a key to continue...";
   } while (std::cin.get() != '\n');
   // -------------------------------------------------------------------------------------

   
   // scalestore.startProfiler();
   if (scalestore.getNodeID() == 1) {
   for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
      scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
         std::vector<PID> localPid(completePIDs);
         uint64_t seed = time(NULL) ^ ((scalestore.getNodeID() + 1) * 13) ^ t_i;
         std::random_shuffle(std::begin(localPid), std::end(localPid), rand_x(seed));
         for (uint64_t r_i = 0; r_i < FLAGS_RUNS; r_i++) {
            for (size_t b_i = 0; b_i < localPid.size(); ++b_i) {
               PID p = localPid[b_i];

               auto start = utils::getTimePoint();
               storage::SharedBFGuard guard(p);
               auto end = utils::getTimePoint();
               threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
               threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end-start));                  
               ensure(!guard.getFrame().latch.isLatched());
            }
         }
      });
   }
   scalestore.getWorkerPool().joinAll();
   }

   // -------------------------------------------------------------------------------------   
   do 
   {
      std::cout << '\n' << "Press a key to continue...";
   } while (std::cin.get() != '\n');
   // -------------------------------------------------------------------------------------

   if (scalestore.getNodeID() == 1) {
   for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
      scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
         std::vector<PID> localPid(completePIDs);
         uint64_t seed = time(NULL) ^ ((scalestore.getNodeID() + 1) * 13) ^ t_i;
         std::random_shuffle(std::begin(localPid), std::end(localPid), rand_x(seed));
         for (uint64_t r_i = 0; r_i < FLAGS_RUNS; r_i++) {
            for (size_t b_i = 0; b_i < localPid.size(); ++b_i) {
               PID p = localPid[b_i];

               auto start = utils::getTimePoint();
               storage::ExclusiveBFGuard guard(p);
               auto end = utils::getTimePoint();
               threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
               threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end-start));                  
               ensure(guard.getFrame().latch.isLatched());
            }
         }
      });
   }
   scalestore.getWorkerPool().joinAll();
   scalestore.stopProfiler();
   }

      // -------------------------------------------------------------------------------------   
   do 
   {
      std::cout << '\n' << "Press a key to continue...";
   } while (std::cin.get() != '\n');
   // -------------------------------------------------------------------------------------
   

   std::cout << "VALIDATION DONE"
             << "\n";
   return 0;
}
