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
DEFINE_string(mode, "in-memory", "options are {in-memory,out-of-memory}");
DEFINE_uint64(rounds, 10, "Update rounds per thread");
// -------------------------------------------------------------------------------------
static constexpr uint64_t BTREE_ID = 0;
static constexpr uint64_t BARRIER_ID = 1;
// -------------------------------------------------------------------------------------
struct Consistency_workloadInfo : public scalestore::profiling::WorkloadInfo {
   std::string experiment;
   uint64_t elements;

   Consistency_workloadInfo(std::string experiment, uint64_t elements) : experiment(experiment), elements(elements) {}

   virtual std::vector<std::string> getRow() { return {experiment, std::to_string(elements)}; }

   virtual std::vector<std::string> getHeader() { return {"workload", "elements"}; }

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
   // check glfags and determine number of tuplese needed
   auto estimate_tuples = [&](uint64_t sizeInGB, uint64_t sizeOfTuple) {
      std::cout << sizeInGB << "\n";
      std::cout << sizeOfTuple << "\n";
      std::cout << (sizeInGB * 1024 * 1024 * 1024) << "\n";
      std::cout << (sizeInGB * 1024 * 1024 * 1024) / sizeOfTuple << "\n";
      return (((sizeInGB * 1024 * 1024 * 1024) / sizeOfTuple)) / 2;
   };
   // -------------------------------------------------------------------------------------
   uint64_t numberTuples = 0;
   if (FLAGS_mode.compare("in-memory") == 0) {
      // ensure that it fits in memory even if replicted
      numberTuples = estimate_tuples((FLAGS_dramGB / FLAGS_nodes) * 0.8, sizeof(Record));
   } else if (FLAGS_mode.compare("out-of-memory") == 0) {
      // avoid that it fits in-memory
      numberTuples = estimate_tuples((FLAGS_ssd_gib * 0.5 * FLAGS_nodes), sizeof(Record));
   } else {
      throw std::runtime_error("Invalid gflag mode");
   }
   // -------------------------------------------------------------------------------------
   // LAMBDAS to reduce boilerplate code
   Barrier local_barrier(FLAGS_worker);
   auto execute_on_tree = [&](std::function<void(uint64_t t_i, storage::BTree<K, V> & tree)> callback) {
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            // -------------------------------------------------------------------------------------
            storage::BTree<K, V> tree(catalog.getCatalogEntry(BTREE_ID).pid);
            // -------------------------------------------------------------------------------------
            storage::DistributedBarrier barrier(catalog.getCatalogEntry(BARRIER_ID).pid);
            barrier.wait();
            // -------------------------------------------------------------------------------------
            callback(t_i, tree);
            local_barrier.wait();
         });
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
   // CONSISTENCY CHECKS
   // -------------------------------------------------------------------------------------
   // 1. build tree mutlithreaded
   {
      Consistency_workloadInfo builtInfo{"Build B-Tree", numberTuples};
      scalestore.startProfiler(builtInfo);
      // -------------------------------------------------------------------------------------
      execute_on_tree([&](uint64_t t_i, storage::BTree<K, V>& tree) {
         uint64_t start = (scalestore.getNodeID() * FLAGS_worker) + t_i;
         const uint64_t inc = FLAGS_worker * FLAGS_nodes;
         for (; start < numberTuples; start = start + inc) {
            tree.insert(start, 1);
            V value = 99;
            auto found = tree.lookup(start, value);
            ensure(found);
            ensure(value == 1);
            threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
         }
      });
      scalestore.stopProfiler();
   }

   {
      // -------------------------------------------------------------------------------------
      // consistency check for build
      Consistency_workloadInfo builtInfo{"Build Verify", numberTuples};
      scalestore.startProfiler(builtInfo);
      std::atomic<uint64_t> global_result{0};
      execute_on_tree([&](uint64_t t_i, storage::BTree<K, V>& tree) {
         uint64_t start = t_i;  // scans all pages
         const uint64_t inc = FLAGS_worker;
         uint64_t local_result = 0;
         for (; start < numberTuples; start = start + inc) {
            V value = 99;
            auto found = tree.lookup(start, value);
            local_result += value;
            ensure(found);
            ensure(value == 1);
            threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
         }
         global_result += local_result;
      });
      scalestore.stopProfiler();
      ensure(global_result == numberTuples);
   }
   // -------------------------------------------------------------------------------------
   // 2. MT increase own keys by 1 for multiple rounds
   {
      Consistency_workloadInfo builtInfo{"Update by 1 B-Tree", numberTuples};
      scalestore.startProfiler(builtInfo);
      // -------------------------------------------------------------------------------------
      execute_on_tree([&](uint64_t t_i, storage::BTree<K, V>& tree) {
         for (uint64_t rnd = 0; rnd < FLAGS_rounds; rnd++) {
            uint64_t start = (scalestore.getNodeID() * FLAGS_worker) + t_i;
            const uint64_t inc = FLAGS_worker * FLAGS_nodes;
            for (; start < numberTuples; start = start + inc) {
               auto updated = tree.lookupAndUpdate(start, [](V& value) { value++; });
               ensure(updated);
               threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
            }
         }
      });
      scalestore.stopProfiler();
   }
   {
      Consistency_workloadInfo builtInfo{"Verify Update", numberTuples};
      scalestore.startProfiler(builtInfo);
      // -------------------------------------------------------------------------------------
      // consistency check for updates
      execute_on_tree([&](uint64_t t_i, storage::BTree<K, V>& tree) {
         uint64_t start = t_i;
         const uint64_t inc = FLAGS_worker;
         for (; start < numberTuples; start = start + inc) {
            V value = 99;
            auto found = tree.lookup(start, value);
            ensure(found);
            ensure(value == (1 + FLAGS_rounds));
            threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
         }
      });
      scalestore.stopProfiler();
   }
   // -------------------------------------------------------------------------------------
   // 3. increase all keys by own "thread/client id"
   {
      Consistency_workloadInfo builtInfo{"Update by thread id B-Tree", numberTuples};
      scalestore.startProfiler(builtInfo);
      // -------------------------------------------------------------------------------------
      execute_on_tree([&](uint64_t t_i, storage::BTree<K, V>& tree) {
         const uint64_t thread_id = (scalestore.getNodeID() * FLAGS_worker) + t_i;
         for (uint64_t start = 0; start < numberTuples; start++) {
            auto updated = tree.lookupAndUpdate(start, [&](V& value) { value = value + thread_id; });
            ensure(updated);
            threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
         }
      });
      // -------------------------------------------------------------------------------------
      auto before = (1 + FLAGS_rounds);
      auto expected = ((FLAGS_worker * FLAGS_nodes) * ((FLAGS_worker * FLAGS_nodes) - 1) / 2);
      expected = expected + before;
      // -------------------------------------------------------------------------------------
      // consistency check for updates
      execute_on_tree([&]([[maybe_unused]] uint64_t t_i, storage::BTree<K, V>& tree) {
         for (uint64_t start = 0; start < numberTuples; start++) {
            V value = 99;
            auto found = tree.lookup(start, value);
            ensure(found);
            ensure(value == expected);
            threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
         }
      });
      scalestore.stopProfiler();
   }
   std::cout << "Starting hash table report "
             << "\n";
   scalestore.getBuffermanager().reportHashTableStats();

   // -------------------------------------------------------------------------------------
   return 0;
}
