#include "PerfEvent.hpp"
#include "scalestore/Config.hpp"
#include "scalestore/utils/RandomGenerator.hpp"
#include "scalestore/threads/Concurrency.hpp"
#include "scalestore/utils/ScrambledZipfGenerator.hpp"
#include "scalestore/utils/Time.hpp"
#include "btree.hpp"
// -------------------------------------------------------------------------------------
#include <thread>
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
int main(int argc, char* argv[])
{
   using namespace scalestore;
   using K = uint64_t;
   using V = BytesPayload<120>;

   gflags::SetUsageMessage("Catalog Test");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
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
   // prepare workload
   std::vector<std::string> workload_type; // warm up or benchmark
   std::vector<uint32_t> workloads;
   std::vector<double> zipfs;
   std::vector<std::thread> threads;
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

   if(FLAGS_YCSB_all_zipf){
      // zipfs.insert(zipfs.end(), {0.0,1.0,1.25,1.5,1.75,2.0});
      zipfs.insert(zipfs.end(), {1.05,1.1,1.15,1.20,1.3,1.35,1.4,1.45});
   }else{
      zipfs.push_back(FLAGS_YCSB_zipf_factor);
   }
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   // create in-memory Btree
   // -------------------------------------------------------------------------------------
   btreeolc::BTree<K,V> tree;
   concurrency::Barrier barrier(FLAGS_worker);
   // -------------------------------------------------------------------------------------
   u64 YCSB_tuple_count = FLAGS_YCSB_tuple_count;
   // -------------------------------------------------------------------------------------
   // Build YCSB Table / Tree
   // -------------------------------------------------------------------------------------
   std::cout << "Building B-Tree ";

   for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
      threads.emplace_back([&, t_i]() {
         // -------------------------------------------------------------------------------------
         // partition
         auto threadPartition = partition(t_i, FLAGS_worker, YCSB_tuple_count);
         auto begin = threadPartition.begin;
         auto end =  threadPartition.end;
         // -------------------------------------------------------------------------------------
         barrier.wait();
         // -------------------------------------------------------------------------------------
         V value;
         for (K k_i = begin; k_i < end; k_i++) {
            utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&value), sizeof(V));
            tree.insert(k_i, value);
         }
         // -------------------------------------------------------------------------------------
         barrier.wait();
      });
   }   
   
   for (auto& thread : threads) {
      thread.join();
   }
   threads.clear();
   std::cout << "[OK] " << "\n";

   struct tx_counter{
      alignas(64) std::atomic<uint64_t> txn;
   };
   
   for (auto ZIPF : zipfs) {
      // -------------------------------------------------------------------------------------
      // YCSB Transaction
      // -------------------------------------------------------------------------------------
      std::unique_ptr<utils::ScrambledZipfGenerator> zipf_random =
          std::make_unique<utils::ScrambledZipfGenerator>(0, YCSB_tuple_count, ZIPF);

      
      for (auto READ_RATIO : workloads) {
         for (auto TYPE : workload_type) {
            std::atomic<bool> keep_running = true;
            std::atomic<u64> running_threads_counter = 0;
            // std::atomic<uint64_t> thread_tx[FLAGS_worker];            
            tx_counter thread_tx[FLAGS_worker];
            for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
               threads.emplace_back([&, t_i]() {
                                       
                  running_threads_counter++;
                  thread_tx[t_i].txn = 0;
                  barrier.wait();
                  while (keep_running) {
                     K key = zipf_random->rand();
                     ensure(key < YCSB_tuple_count);
                     V result;

                     if (READ_RATIO == 100 || utils::RandomGenerator::getRandU64(0, 100) < READ_RATIO) {
                        // auto start = utils::getTimePoint();
                        auto success = tree.lookup(key, result);
                        ensure(success);
                        // auto end = utils::getTimePoint();
                        // threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
                     } else {
                        V payload;
                        utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(V));
                        // auto start = utils::getTimePoint();
                        tree.insert(key, payload);
                        // auto end = utils::getTimePoint();
                        // threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
                     }
                     // threads::Worker::my().counters.incr(profiling::WorkerCounters::opsPerSec);
                     thread_tx[t_i].txn++;
                  }
                  running_threads_counter--;
               });
            }
            // -------------------------------------------------------------------------------------
            // profiler
            threads.emplace_back([&]() {
               running_threads_counter++;

               u64 time = 0;
               std::cout << "t,workload,read-ratio,tag,tx_committed" << std::endl;
               while (keep_running) {
                  std::cout << time++ << "," << TYPE << ","  << READ_RATIO << "," << FLAGS_tag << ",";
                  u64 total_committed = 0;
                  for (u64 t_i = 0; t_i < FLAGS_worker; t_i++) {
                     total_committed += thread_tx[t_i].txn.exchange(0);
                  }
                  std::cout << total_committed << std::endl;
                  sleep(1);
               }
               running_threads_counter--;
            });

            // -------------------------------------------------------------------------------------
            // Join Threads
            // -------------------------------------------------------------------------------------
            sleep(FLAGS_YCSB_run_for_seconds);
            keep_running = false;
            while (running_threads_counter) {
               _mm_pause();
            }

            for (auto& thread : threads) {
               thread.join();
            }
            threads.clear();
         }
      }
   }

   return 0;
}
