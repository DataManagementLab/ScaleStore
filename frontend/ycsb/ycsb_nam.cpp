#include "scalestore/storage/datastructures/BTree.hpp"
#include "PerfEvent.hpp"
#include "scalestore/Config.hpp"
#include "scalestore/ScaleStore.hpp"
#include "scalestore/rdma/CommunicationManager.hpp"
#include "scalestore/utils/RandomGenerator.hpp"
#include "scalestore/utils/ScrambledZipfGenerator.hpp"
#include "scalestore/utils/Time.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
DEFINE_uint32(YCSB_read_ratio, 100, "");
DEFINE_uint64(YCSB_tuple_count, 1, " Tuple count in"); 
DEFINE_double(YCSB_zipf_factor, 0.0, "Default value according to spec");
DEFINE_double(YCSB_run_for_seconds, 10.0, "");
DEFINE_uint32(NAM_storage_nodes, 1, "");
// -------------------------------------------------------------------------------------
using u64 = uint64_t;
using u8 = uint8_t;
// -------------------------------------------------------------------------------------
static constexpr uint64_t BTREE_ID =0; 
static constexpr uint64_t BARRIER_ID =1;
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
struct YCSB_workloadInfo : public scalestore::profiling::WorkloadInfo{
   std::string experiment;
   uint64_t elements;
   uint64_t readRatio;
   double zipfFactor;

   YCSB_workloadInfo(std::string experiment, uint64_t elements, uint64_t readRatio, double zipfFactor)
      : experiment(experiment), elements(elements), readRatio(readRatio), zipfFactor(zipfFactor)
   {
   }

   
   virtual std::vector<std::string> getRow(){
      return {experiment,std::to_string(elements), std::to_string(readRatio), std::to_string(zipfFactor)};
   }

   virtual std::vector<std::string> getHeader(){
      return {"workload","elements","read ratio", "zipfFactor"};
   }
   
   virtual void csv(std::ofstream& file) override {
      file << experiment << " , ";
      file << elements << " , ";
      file << readRatio << " , ";
      file << zipfFactor << " , ";
   }
   virtual void csvHeader(std::ofstream& file) override {
      file << "Workload" << " , ";
      file << "Elements" << " , ";
      file << "ReadRatio" << " , ";
      file << "ZipfFactor" << " , ";
   }
};
// -------------------------------------------------------------------------------------
using namespace scalestore;
int main(int argc, char* argv[])
{
   using K = uint64_t;
   using V = BytesPayload<128>;

   gflags::SetUsageMessage("Catalog Test");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   ScaleStore scalestore;
   auto& catalog = scalestore.getCatalog();
   // -------------------------------------------------------------------------------------
   auto barrier_wait = [&]() {
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            storage::DistributedBarrier barrier(catalog.getCatalogEntry(BARRIER_ID).pid);
            barrier.wait();
         });
      }
      scalestore.getWorkerPool().joinAll();
   };
   // -------------------------------------------------------------------------------------
   auto partition = [&](uint64_t id, uint64_t participants, uint64_t N) -> Partition {
      const uint64_t blockSize = N / participants;
      auto begin = id * blockSize;
      auto end = begin + blockSize;
      if (id == participants - 1)
         end = N;
      return {.begin = begin, .end = end};
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
   u64 YCSB_tuple_count = FLAGS_YCSB_tuple_count;
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   auto nodePartition = partition(scalestore.getNodeID(), FLAGS_NAM_storage_nodes, YCSB_tuple_count);
   // -------------------------------------------------------------------------------------
   // Build YCSB Table / Tree
   // -------------------------------------------------------------------------------------
   if (scalestore.getNodeID() < FLAGS_NAM_storage_nodes) {
      YCSB_workloadInfo builtInfo{"Build", YCSB_tuple_count, FLAGS_YCSB_read_ratio, FLAGS_YCSB_zipf_factor};
      scalestore.startProfiler(builtInfo);
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            // -------------------------------------------------------------------------------------
            // partition
            auto nodeKeys = nodePartition.end - nodePartition.begin;
            auto threadPartition = partition(t_i, FLAGS_worker, nodeKeys);
            auto begin = nodePartition.begin + threadPartition.begin;
            auto end = nodePartition.begin + threadPartition.end;
            storage::BTree<K, V> tree(catalog.getCatalogEntry(BTREE_ID).pid);
            // -------------------------------------------------------------------------------------
            storage::DistributedBarrier barrier(catalog.getCatalogEntry(BARRIER_ID).pid);
            // -------------------------------------------------------------------------------------
            V value;
            for (K k_i = begin; k_i < end; k_i++) {
               utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&value), sizeof(V));
               tree.insert(k_i, value);
               threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
            }
            // -------------------------------------------------------------------------------------
         });
      }
      scalestore.getWorkerPool().joinAll();
      scalestore.stopProfiler();

      std::cout << "Flushing all pages"
                << "\n";
      scalestore.getBuffermanager().writeAllPages();

      std::cout << "Done"
                << "\n";
   }

   barrier_wait();

   // -------------------------------------------------------------------------------------
   // YCSB Transaction Partitioned
   // -------------------------------------------------------------------------------------      
   if (scalestore.getNodeID() >= FLAGS_NAM_storage_nodes) {
      auto zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(0, YCSB_tuple_count,
                                                                         FLAGS_YCSB_zipf_factor);
      // -------------------------------------------------------------------------------------
      // zipf creation can take some time due to floating point loop therefore wait with barrier
      std::atomic<bool> keep_running = true;
      std::atomic<u64> running_threads_counter = 0;
      YCSB_workloadInfo experimentInfo{"YCSB-NAM-Compute", YCSB_tuple_count, FLAGS_YCSB_read_ratio, FLAGS_YCSB_zipf_factor};
      scalestore.startProfiler(experimentInfo);

      auto sleepTime = FLAGS_YCSB_run_for_seconds*(scalestore.getNodeID() - FLAGS_NAM_storage_nodes);
      std::cout << "Sleeping for " << sleepTime << "\n";

      if(sleepTime !=0){
         sleep(sleepTime);
      }
      
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            running_threads_counter++;
            storage::BTree<K, V> tree(catalog.getCatalogEntry(BTREE_ID).pid);
            while (keep_running) {
               K key = zipf_random->rand();
               ensure(key < YCSB_tuple_count);
               V result;
               if (FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio) {
                  auto start = utils::getTimePoint();
                  auto success = tree.lookup_opt(key, result);
                  ensure(success);
                  auto end = utils::getTimePoint();
                  threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
               } else {
                  V payload;
                  utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(V));
                  auto start = utils::getTimePoint();
                  tree.insert(key, payload);
                  auto end = utils::getTimePoint();
                  threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
               }
               threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
            }
            running_threads_counter--;
         });
      }
      // -------------------------------------------------------------------------------------
      // Join Threads
      // -------------------------------------------------------------------------------------
      sleep(FLAGS_YCSB_run_for_seconds * (FLAGS_nodes - FLAGS_NAM_storage_nodes));
      keep_running = false;
      while (running_threads_counter) {
         _mm_pause();
      }
      scalestore.getWorkerPool().joinAll();
      scalestore.stopProfiler();
      barrier_wait();
   }else{
      YCSB_workloadInfo experimentInfo{"YCSB-NAM-Storage", YCSB_tuple_count, FLAGS_YCSB_read_ratio, FLAGS_YCSB_zipf_factor};
      scalestore.startProfiler(experimentInfo);
      sleep(FLAGS_YCSB_run_for_seconds * (FLAGS_nodes - FLAGS_NAM_storage_nodes));
      // sleep(FLAGS_YCSB_run_for_seconds); // run as long as all clients
      barrier_wait();
      scalestore.stopProfiler();
   }
   // -------------------------------------------------------------------------------------
   return 0;
}
