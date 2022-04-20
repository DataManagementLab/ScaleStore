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
static constexpr uint64_t BARRIER_ID = 0;
// -------------------------------------------------------------------------------------
DEFINE_uint64(run_for_operations, 1e6, "Total number of operations executed per thread");
DEFINE_bool(attach_perf, false, "break for perf");
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
   // -------------------------------------------------------------------------------------
   gflags::SetUsageMessage("Internal hash table benchmark");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   ScaleStore scalestore;
   auto& catalog = scalestore.getCatalog();
   auto bufferSize = scalestore.getBuffermanager().getDramPoolSize();
   // -------------------------------------------------------------------------------------
   // WAIT FUNCTION
   auto barrier_wait = [&]() {
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            storage::DistributedBarrier barrier(catalog.getCatalogEntry(BARRIER_ID).pid);
            barrier.wait();
         });
      }
      scalestore.getWorkerPool().joinAll();
   };

   auto wait_for_key_press = [&](){
                                if(FLAGS_attach_perf){
                                   std::cout << "Press enter" << "\n";

                                   char temp;
                                   std::cin.get(temp);
                                }
                             };
   // -------------------------------------------------------------------------------------
   // BARRIER
   if (scalestore.getNodeID() == 0) {
      scalestore.getWorkerPool().scheduleJobSync(0, [&]() { scalestore.createBarrier(FLAGS_worker * FLAGS_nodes); });
   }
   barrier_wait();
   // -------------------------------------------------------------------------------------
   // Benchmark
   // -------------------------------------------------------------------------------------
   std::vector<double> ops(FLAGS_worker);
   std::vector<PID> pids(FLAGS_worker*FLAGS_run_for_operations);
   // -------------------------------------------------------------------------------------
   // fill until full
   scalestore.getWorkerPool().scheduleJobSync(0, [&]() {
      for (uint64_t op_i = 0; op_i < bufferSize; op_i++) {
         scalestore::storage::ExclusiveBFGuard new_page_guard;
      }
   });
   wait_for_key_press();
   OLAP_workloadInfo builtInfo{"Build", FLAGS_run_for_operations};
   scalestore.startProfiler(builtInfo);
   for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
      scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
         uint64_t pages{0};
         auto start_time_microseconds = utils::getTimePoint();
         for (uint64_t op_i = 0; op_i < FLAGS_run_for_operations; op_i++) {
            scalestore::storage::ExclusiveBFGuard new_page_guard;
            pages++;
            auto idx = (FLAGS_run_for_operations * t_i) + pages;
            pids[idx] = new_page_guard.getFrame().pid;
            threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
         }
         auto end_time_microseconds = utils::getTimePoint();
         ops[t_i] =
             ((double)(FLAGS_run_for_operations)) / (((double)end_time_microseconds - start_time_microseconds) / (1e6));
      });
   }
   scalestore.getWorkerPool().joinAll();
   scalestore.stopProfiler();
   std::cout << std::fixed;
   std::cout << std::reduce(ops.begin(), ops.end()) / FLAGS_worker << " insert operations with PP under pressure \n";
   // -------------------------------------------------------------------------------------
   std::cout << "Starting hash table report "
             << "\n";
   scalestore.getBuffermanager().reportHashTableStats();
   
   return 0;
};
