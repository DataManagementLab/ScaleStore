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
DEFINE_uint64(OLAP_factor_mem_ssd, 10, " factor of in-memory data to ssd");
DEFINE_uint64(OLAP_partition_batch, 1, " pages co-located");
DEFINE_double(OLAP_run_for_seconds, 10.0, "");
DEFINE_bool(OLAP_partitioned, true, "");

// -------------------------------------------------------------------------------------
using u64 = uint64_t;
using u8 = uint8_t;
// -------------------------------------------------------------------------------------
static constexpr uint64_t BARRIER_ID = 0;
static constexpr uint64_t PREFETCH_MAX_BATCH = 6;
// -------------------------------------------------------------------------------------
struct OLAP_workloadInfo : public scalestore::profiling::WorkloadInfo {
   std::string experiment;
   uint64_t elements;
   uint64_t readRatio;
   double zipfFactor;

   OLAP_workloadInfo(std::string experiment, uint64_t elements)
       : experiment(experiment), elements(elements)
   {
   }
   
   virtual std::vector<std::string> getRow(){
      return {experiment,std::to_string(elements)};
   }

   virtual std::vector<std::string> getHeader(){
      return {"workload","SSD data GB"};
   }

   virtual void csv(std::ofstream& file) override
   {
      file << experiment << " , ";
      file << elements << " , ";

   }
   virtual void csvHeader(std::ofstream& file) override
   {
      file << "Workload"
           << " , ";
      file << "SSDdataGB"
           << " , ";

   }
};
// -------------------------------------------------------------------------------------
using namespace scalestore;
int main(int argc, char* argv[])
{
   struct debug_row{
      uint64_t t_i = 0;
      uint64_t nodeId = 0;
      uint64_t pid = 0;
      uint64_t element =0;

   };
   using value = debug_row;
   gflags::SetUsageMessage("OLAP Experimetn");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   uint64_t KEYS = (FLAGS_OLAP_factor_mem_ssd * ((FLAGS_dramGB *1024*1024*1024)/ sizeof(value))) / FLAGS_worker;
   std::cout << "Createing Keys: " << KEYS << "\n";
   // -------------------------------------------------------------------------------------
   ScaleStore scalestore;
   auto& catalog = scalestore.getCatalog();
   // -------------------------------------------------------------------------------------
   // WAIT FUNCTION
   auto barrier_wait = [&]() {
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         scalestore.getWorkerPool().scheduleJobAsync(t_i, [&]() {
            storage::DistributedBarrier barrier(catalog.getCatalogEntry(BARRIER_ID).pid);
            barrier.wait();
         });
      }
      scalestore.getWorkerPool().joinAll();
   };

   // -------------------------------------------------------------------------------------
   // BARRIER
   if (scalestore.getNodeID() == 0) {
      scalestore.getWorkerPool().scheduleJobSync(0, [&]() {
         scalestore.createBarrier(FLAGS_worker * FLAGS_nodes);
      });
   }
   std::cout << "before barrier" << "\n";

   barrier_wait();
   // -------------------------------------------------------------------------------------
   std::cout << "After first barrier"
             << "\n";
   std::vector<uint64_t> linkedListIds(FLAGS_worker);
   std::vector<std::vector<PID>> workerPids(FLAGS_worker);
   {
      OLAP_workloadInfo builtInfo{"Build", (uint64_t)(FLAGS_OLAP_factor_mem_ssd * FLAGS_dramGB)};
      scalestore.startProfiler(builtInfo);
      // every node creates a Linked List
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         scalestore.getWorkerPool().scheduleJobAsync(
             t_i, [&, t_i]() { [[maybe_unused]] auto list = scalestore.createLinkedList<value>(linkedListIds[t_i]); });
      }
      scalestore.getWorkerPool().joinAll();
      // fill numbers with allocation per thread a private list
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            auto entry = catalog.getCatalogEntry(linkedListIds[t_i]);
            ensure(entry.type == storage::DS_TYPE::LLIST);
            storage::LinkedList<value> list(entry.pid);
            workerPids[t_i].push_back(entry.pid);
            uint64_t partition = 0;
            uint64_t nid = scalestore.getNodeID();
            if (FLAGS_OLAP_partitioned)
               list.fillNumbers([&](uint64_t element, PID pid){
                                   return debug_row{.t_i=t_i,.nodeId=nid,.pid = pid,.element=element};
                                },
                  KEYS, [&]() { return storage::ExclusiveBFGuard(); }, workerPids[t_i]);
            else
               list.fillNumbers(
                   [&](uint64_t element, PID pid) {
                      return debug_row{.t_i = t_i, .nodeId = nid, .pid = pid, .element = element};
                   },
                   KEYS,
                   [&]() {
                      partition++;
                      auto p = partition % FLAGS_nodes;
                      if (p == nid)
                         return storage::ExclusiveBFGuard();
                      else
                         return storage::ExclusiveBFGuard(p);
                   }, workerPids[t_i]);
         });
      }
      scalestore.getWorkerPool().joinAll();
      // -------------------------------------------------------------------------------------
      barrier_wait();
      scalestore.stopProfiler();
   }
   std::cout << "Flushing all pages to SSD:";
   sleep(5);
   scalestore.getBuffermanager().writeAllPages();
   sleep(20);
   barrier_wait();
   std::cout << " [Done] " << "\n";

   // -------------------------------------------------------------------------------------
   // execute OLAP either from partitioned or random
   for (auto TYPE : {"OLAP_warm_up", "OLAP_scan"}) {
      barrier_wait();
      OLAP_workloadInfo experimentInfo{TYPE, (uint64_t) (FLAGS_OLAP_factor_mem_ssd * FLAGS_dramGB)};
      scalestore.startProfiler(experimentInfo);
      std::atomic<bool> keep_running = true;
      std::atomic<u64> running_threads_counter = 0;
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            uint64_t result = 0;
            running_threads_counter++;
            while (keep_running) {
               for (uint64_t i = 0; i < workerPids[t_i].size(); i++) {
                  auto start = utils::getTimePoint();
                  auto pid = workerPids[t_i][i];
                  storage::SharedBFGuard sg_current(pid);
                  auto& node = sg_current.as<storage::LinkedList<debug_row>::LinkedListNode>(0);
                  auto count = node.count;
                  for (uint64_t c_i = 0; c_i < count; c_i++) {
                     result += node.entries[c_i].element;
                  }
                  auto end = utils::getTimePoint();
                  threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
                  threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
               }
            }
            running_threads_counter--;
         });
      }
      // -------------------------------------------------------------------------------------
      // join threads
      sleep(FLAGS_OLAP_run_for_seconds);
      keep_running = false;
      while (running_threads_counter) {
         _mm_pause();
      }
      scalestore.getWorkerPool().joinAll();
      // -------------------------------------------------------------------------------------
      scalestore.stopProfiler();
   }

   return 0;
}
