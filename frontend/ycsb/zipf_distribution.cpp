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
DEFINE_uint32(YCSB_read_ratio, 100, "");
DEFINE_bool(YCSB_all_workloads, false , "Execute all workloads i.e. 50 95 100 ReadRatio on same tree");
DEFINE_uint64(YCSB_tuple_count, 1, " Tuple count in");
DEFINE_double(YCSB_zipf_factor, 0.0, "Default value according to spec");
DEFINE_double(YCSB_run_for_seconds, 10.0, "");
DEFINE_bool(YCSB_partitioned, false, "");
DEFINE_bool(YCSB_warm_up, false, "");
DEFINE_bool(YCSB_record_latency, false, "");
DEFINE_bool(YCSB_all_zipf, false, "");
DEFINE_bool(YCSB_local_zipf, false, "");
DEFINE_bool(YCSB_flush_pages, false, "");
// -------------------------------------------------------------------------------------
using u64 = uint64_t;
using u8 = uint8_t;
// -------------------------------------------------------------------------------------
static constexpr uint64_t BTREE_ID = 0;
static constexpr uint64_t BARRIER_ID = 1;
// -------------------------------------------------------------------------------------
template <u64 size>
struct BytesPayload {
   u8 value[size];
   BytesPayload() = default;
   bool operator==(BytesPayload& other) { return (std::memcmp(value, other.value, sizeof(value)) == 0); }
   bool operator!=(BytesPayload& other) { return !(operator==(other)); }
   // BytesPayload(const BytesPayload& other) { std::memcpy(value, other.value, sizeof(value)); }
   // BytesPayload& operator=(const BytesPayload& other)
   // {
      // std::memcpy(value, other.value, sizeof(value));
      // return *this;
   // }
};
// -------------------------------------------------------------------------------------
struct Partition {
   uint64_t begin;
   uint64_t end;
};
// -------------------------------------------------------------------------------------
struct YCSB_workloadInfo : public scalestore::profiling::WorkloadInfo {
   std::string experiment;
   uint64_t elements;
   uint64_t readRatio;
   double zipfFactor;
   std::string zipfOffset;
   uint64_t timestamp = 0;

   YCSB_workloadInfo(std::string experiment, uint64_t elements, uint64_t readRatio, double zipfFactor, std::string zipfOffset)
      : experiment(experiment), elements(elements), readRatio(readRatio), zipfFactor(zipfFactor), zipfOffset(zipfOffset)
   {
   }

   
   virtual std::vector<std::string> getRow(){
      return {
          experiment, std::to_string(elements),    std::to_string(readRatio), std::to_string(zipfFactor),
          zipfOffset, std::to_string(timestamp++),
      };
   }

   virtual std::vector<std::string> getHeader(){
      return {"workload","elements","read ratio", "zipfFactor", "zipfOffset", "timestamp"};
   }
   

   virtual void csv(std::ofstream& file) override
   {
      file << experiment << " , ";
      file << elements << " , ";
      file << readRatio << " , ";
      file << zipfFactor << " , ";
      file << zipfOffset << " , ";
      file << timestamp << " , ";
   }
   virtual void csvHeader(std::ofstream& file) override
   {
      file << "Workload"
           << " , ";
      file << "Elements"
           << " , ";
      file << "ReadRatio"
           << " , ";
      file << "ZipfFactor"
           << " , ";
      file << "ZipfOffset"
           << " , ";
      file << "Timestamp"
           << " , ";
   }
};
// -------------------------------------------------------------------------------------
using namespace scalestore;
int main(int argc, char* argv[])
{
   gflags::SetUsageMessage("Catalog Test");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   // prepare workload
   std::vector<std::string> workload_type; // warm up or benchmark
   std::vector<uint32_t> workloads;
   std::vector<double> zipfs;
   if(FLAGS_YCSB_all_workloads){
      workloads.push_back(5); 
      workloads.push_back(50); 
      workloads.push_back(95);
      workloads.push_back(100);
   }else{
      workloads.push_back(FLAGS_YCSB_read_ratio);
   }

   
   if(FLAGS_YCSB_warm_up){
      workload_type.push_back("YCSB_warm_up");
      workload_type.push_back("YCSB_txn");
   }else{
      workload_type.push_back("YCSB_txn");
   }

   if (FLAGS_YCSB_all_zipf) {
      // zipfs.insert(zipfs.end(), {0.0,1.0,1.25,1.5,1.75,2.0});
      // zipfs.insert(zipfs.end(), {1.05,1.1,1.15,1.20,1.3,1.35,1.4,1.45});
      zipfs.insert(zipfs.end(), {0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0});
   } else {
      zipfs.push_back(FLAGS_YCSB_zipf_factor);
   }
   // -------------------------------------------------------------------------------------
   ScaleStore scalestore;
   // ----------------------------------->--------------------------------------------------
   u64 YCSB_tuple_count = FLAGS_YCSB_tuple_count;
   // -------------------------------------------------------------------------------------

   struct ZipfEntry {
      std::atomic<uint64_t> counter{0};
      uint64_t id = 0;
   };
   std::vector<ZipfEntry> histogram(YCSB_tuple_count);

   for (auto ZIPF : zipfs) {
      uint64_t idx = 0;
      for (auto& h : histogram) {
         h.counter = 0;
         h.id = idx++;
      }

      // -------------------------------------------------------------------------------------
      // YCSB Transaction
      // -------------------------------------------------------------------------------------
      std::unique_ptr<utils::ScrambledZipfGenerator> zipf_random;
      zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(0, YCSB_tuple_count, ZIPF);

      std::atomic<bool> keep_running = true;
      std::atomic<u64> running_threads_counter = 0;
      std::atomic<u64> transactions = 0;
      
      YCSB_workloadInfo experimentInfo{"Zipf", YCSB_tuple_count, 0, ZIPF, (FLAGS_YCSB_local_zipf ? "local_zipf" : "global_zipf")};
      scalestore.startProfiler(experimentInfo);
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            running_threads_counter++;
            uint64_t tl_txns =0;
            while (keep_running) {
               uint64_t idx = zipf_random->rand();
               // uint64_t idx = scalestore::utils::RandomGenerator::getRandU64(0,YCSB_tuple_count);
               histogram[idx].counter++;
               threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
               tl_txns++;
            }
            transactions += tl_txns;
            running_threads_counter--;
         });
      }
      // -------------------------------------------------------------------------------------
      // Join Threads
      // -------------------------------------------------------------------------------------
      sleep(FLAGS_YCSB_run_for_seconds);
      keep_running = false;
      while (running_threads_counter) {
         _mm_pause();
      }
      scalestore.getWorkerPool().joinAll();
      // -------------------------------------------------------------------------------------
      scalestore.stopProfiler();

      std::vector<std::pair<uint64_t,uint64_t>> tmp_hist(histogram.size());
      
      for(uint64_t i =0; i < histogram.size(); i++){
         tmp_hist[i].first = histogram[i].counter.load();
         tmp_hist[i].second = histogram[i].id;
      }
      
      std::sort(tmp_hist.begin(), tmp_hist.end(),
                [](const std::pair<uint64_t,uint64_t>& a, const std::pair<uint64_t,uint64_t>& b) -> bool{
                   return a.first > b.first;
                });

      double combined = 0;
      uint64_t combined_txn = 0;
      for(uint64_t i =0; i < tmp_hist.size(); i++){
         std::cout << "Entry: [" << tmp_hist[i].second << " " << tmp_hist[i].first << " " << tmp_hist[i].first/(double)transactions  << "]\n";
         combined += tmp_hist[i].first/(double)transactions;
         combined_txn += tmp_hist[i].first;
      }
      std::cout << "combined " << combined << "\n";
      std::cout << "txn " << transactions << "\n";
      std::cout << "txn check " << combined_txn << "\n";
   }

   return 0;
}
