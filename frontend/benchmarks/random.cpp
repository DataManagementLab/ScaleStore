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
#include <numeric>
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
      return {experiment,std::to_string(elements), std::to_string(readRatio), std::to_string(zipfFactor)};
   }

   virtual std::vector<std::string> getHeader(){
      return {"workload","elements","read ratio", "zipfFactor"};
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
   auto wait_for_key_press = [&]() {
      if (FLAGS_attach_perf) {
         std::cout << "Press enter"
                   << "\n";

         char temp;
         std::cin.get(temp);
      }
   };
   // -------------------------------------------------------------------------------------
   std::vector<uint64_t> counters(FLAGS_worker);
   wait_for_key_press();
   uint64_t counter = 0;
   for (u64 op_i = 0; op_i < FLAGS_run_for_operations; op_i++)
      counter += utils::RandomGenerator::getRandU64(0, 100);

   std::cout << counter << "\n";

   return 0;
};
