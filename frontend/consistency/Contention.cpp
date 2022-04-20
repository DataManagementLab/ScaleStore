// -------------------------------------------------------------------------------------
// Consistency Check for ScaleStore
// 1. partition keys and build the tree multi threaded
// 2. partition keys and update keys by 1 for every access
// 3. shuffle keys and iterate once randomly over all keys and add thread id
// 4. run consistency checks
// -------------------------------------------------------------------------------------
#include "PerfEvent.hpp"
#include "scalestore/Config.hpp"
#include "scalestore/ScaleStore.hpp"
#include "scalestore/rdma/CommunicationManager.hpp"
#include "scalestore/storage/datastructures/BTree.hpp"
#include "scalestore/utils/RandomGenerator.hpp"
#include "scalestore/utils/ScrambledZipfGenerator.hpp"
#include "scalestore/utils/Time.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
DEFINE_double(run_for_seconds, 10.0, "");
// -------------------------------------------------------------------------------------
static constexpr uint64_t BTREE_ID = 0;
static constexpr uint64_t BARRIER_ID = 1;
// -------------------------------------------------------------------------------------
std::atomic<uint64_t> threads_active = 0;
// -------------------------------------------------------------------------------------
struct Consistency_workloadInfo : public scalestore::profiling::WorkloadInfo {
   std::string experiment;
   uint64_t elements;

   Consistency_workloadInfo(std::string experiment, uint64_t elements) : experiment(experiment), elements(elements) {}

   virtual std::vector<std::string> getRow() { return {experiment, std::to_string(elements), std::to_string(threads_active.load())}; }

   virtual std::vector<std::string> getHeader() { return {"workload", "elements","thread_active"}; }

   virtual void csv(std::ofstream& file) override {
      file << experiment << " , ";
      file << elements << " , ";
   }

   virtual void csvHeader(std::ofstream& file) override {
      file << "Workload"
           << " , ";
      file << "Elements"
           << " , ";
   }
};
class Barrier {
  private:
   const std::size_t threadCount;
   alignas(64) std::atomic<std::size_t> cntr;
   alignas(64) std::atomic<uint8_t> round;

  public:
   explicit Barrier(std::size_t threadCount) : threadCount(threadCount), cntr(threadCount), round(0) {}

   template <typename F>
   bool wait(F finalizer) {
      auto prevRound = round.load();  // Must happen before fetch_sub
      auto prev = cntr.fetch_sub(1);
      if (prev == 1) {
         // last thread arrived
         cntr = threadCount;
         auto r = finalizer();
         round++;
         return r;
      } else {
         while (round == prevRound) {
            // wait until barrier is ready for re-use
            asm("pause");
            asm("pause");
            asm("pause");
         }
         return false;
      }
   }
   inline bool wait() {
      return wait([]() { return true; });
   }
};

// -------------------------------------------------------------------------------------
using namespace scalestore;
int main(int argc, char* argv[]) {
   using K = uint64_t;
   using V = uint64_t;
   // -------------------------------------------------------------------------------------
   struct Record {
      K key;
      V value;
   };
   // -------------------------------------------------------------------------------------
   gflags::SetUsageMessage("Catalog Test");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   std::cout << "FLAGS_messageHandlerThreads " << FLAGS_messageHandlerThreads << std::endl;
   ScaleStore scalestore;
   auto& catalog = scalestore.getCatalog();
   // -------------------------------------------------------------------------------------
   constexpr uint64_t numberTuples = 100;  // fits on a single page
   // -------------------------------------------------------------------------------------
   // LAMBDAS to reduce boilerplate code

   auto barrier_wait = [&]() {
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            storage::DistributedBarrier barrier(catalog.getCatalogEntry(BARRIER_ID).pid);
            barrier.wait();
         });
      }
      scalestore.getWorkerPool().joinAll();
   };

   
   Barrier local_barrier(FLAGS_worker);
   std::atomic<bool> keep_running = true;
   std::atomic<u64> running_threads_counter = 0;
   auto execute_on_tree = [&](std::function<void(uint64_t t_i, storage::BTree<K, V> & tree)> callback) {
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            running_threads_counter++;
            threads_active++;
            // -------------------------------------------------------------------------------------
            storage::BTree<K, V> tree(catalog.getCatalogEntry(BTREE_ID).pid);
            // -------------------------------------------------------------------------------------
            storage::DistributedBarrier barrier(catalog.getCatalogEntry(BARRIER_ID).pid);
            barrier.wait();
            // -------------------------------------------------------------------------------------
            callback(t_i, tree);
            threads_active--;
            local_barrier.wait();
            running_threads_counter--;
         });
      }

      sleep(FLAGS_run_for_seconds);
      keep_running = false;
      while (running_threads_counter) {
         _mm_pause();
      }
      scalestore.getWorkerPool().joinAll();
   };

   // -------------------------------------------------------------------------------------
   // create Btree (0), Barrier(1)
   // -------------------------------------------------------------------------------------
   if (scalestore.getNodeID() == 0) {
      scalestore.getWorkerPool().scheduleJobSync(0, [&]() {
         scalestore.createBTree<K, V>();
         scalestore.createBarrier(FLAGS_worker * FLAGS_nodes);
      });
   }
   // -------------------------------------------------------------------------------------
   // Contention build and update
   // -------------------------------------------------------------------------------------
   // 1. build tree mutlithreaded
   {
      Consistency_workloadInfo builtInfo{"Build B-Tree", numberTuples};
      scalestore.startProfiler(builtInfo);
      // -------------------------------------------------------------------------------------
      execute_on_tree([&](uint64_t t_i, storage::BTree<K, V>& tree) {
         V values_seen = 0;
         K key = 0;
         while (keep_running) {
            key = (key+1) % numberTuples;
            V value = utils::RandomGenerator::getRandU64(0, 100000000);
            tree.insert(key, value);
            V lkp_value = 99;
            auto found = tree.lookup(key, lkp_value);
            // auto found = tree.lookup_opt(key, lkp_value);            
            values_seen += lkp_value;
            ensure(found);
         }
         std::cout << "t_i " << t_i << " values " << values_seen << std::endl;
      });
      scalestore.stopProfiler();
   }
   {
      Consistency_workloadInfo builtInfo{"Barrier", numberTuples};
      scalestore.startProfiler(builtInfo);
      barrier_wait();
      scalestore.stopProfiler();
      
   }
   // -------------------------------------------------------------------------------------

   return 0;
}
