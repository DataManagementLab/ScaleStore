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
DEFINE_bool(YCSB_all_workloads, false, "Execute all workloads i.e. 50 95 100 ReadRatio on same tree");
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
   uint64_t targetThroughput;
   double zipfFactor;
   std::string zipfOffset;
   uint64_t timestamp = 0;

   YCSB_workloadInfo(std::string experiment,
                     uint64_t elements,
                     uint64_t readRatio,
                     uint64_t targetThroughput,
                     double zipfFactor,
                     std::string zipfOffset)
       : experiment(experiment),
         elements(elements),
         readRatio(readRatio),
         targetThroughput(targetThroughput),
         zipfFactor(zipfFactor),
         zipfOffset(zipfOffset) {}

   virtual std::vector<std::string> getRow() {
      return {
          experiment, std::to_string(elements),    std::to_string(readRatio), std::to_string(targetThroughput), std::to_string(zipfFactor),
          zipfOffset, std::to_string(timestamp++),
      };
   }

   virtual std::vector<std::string> getHeader() {
      return {"workload", "elements", "read ratio", "targetThroughput", "zipfFactor", "zipfOffset", "timestamp"};
   }

   virtual void csv(std::ofstream& file) override {
      file << experiment << " , ";
      file << elements << " , ";
      file << readRatio << " , ";
      file << targetThroughput << " , ";
      file << zipfFactor << " , ";
      file << zipfOffset << " , ";
      file << timestamp << " , ";
   }
   virtual void csvHeader(std::ofstream& file) override {
      file << "Workload"
           << " , ";
      file << "Elements"
           << " , ";
      file << "ReadRatio"
           << " , ";
      file << "TargetThroughput"
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

// coordinated omission
// https://www.scylladb.com/2021/04/22/on-coordinated-omission/
std::vector<std::chrono::nanoseconds> poisson_process(double estimated_events_per_second, uint64_t runtime) {
   uint64_t total_number_events_needed = estimated_events_per_second * runtime;
   assert(estimated_events_per_second < 1e9);         // otherwise we need higher precision i.e. nanoseconds
   std::random_device rd;                             // uniformly-distributed integer random number generator
   std::mt19937 rng(rd());                            // mt19937: Pseudo-random number generation
   double lamda = estimated_events_per_second / 1e9;  // how many arrivals per unit
   std::exponential_distribution<double> exp(lamda);
   double sum_arrival_time = 0;
   double new_arrival_time = 0;
   // -------------------------------------------------------------------------------------
   std::vector<std::chrono::nanoseconds> sum_arrival_times;
   sum_arrival_times.reserve(total_number_events_needed);
   // -------------------------------------------------------------------------------------
   for (uint64_t i = 0; i < total_number_events_needed; ++i) {
      new_arrival_time = exp(rng);  // generates the next random number in the distribution
      sum_arrival_time = sum_arrival_time + new_arrival_time;
      sum_arrival_times.emplace_back(
          (uint64_t)(sum_arrival_time));  // looses a bit precision but therefore we use nanosecond and not microseconds
   }
   return sum_arrival_times;
}

inline void wait_until_next(decltype(std::chrono::high_resolution_clock::now()) next) {
   using namespace std::chrono;
   high_resolution_clock::time_point current = high_resolution_clock::now();
   duration<double> time_span = duration_cast<duration<double>>(next - current);
   while (time_span.count() > 0) {
      current = high_resolution_clock::now();
      time_span = duration_cast<duration<double>>(next - current);
   }
}

struct ReadRatioLimits{
   uint64_t read_ratio;
   uint64_t max_throughput;
   uint64_t increment;
   uint64_t start_value;
};

struct LatencyWorkload{
   double zipf {0};
   std::vector<ReadRatioLimits> wl_desc;
};


// -------------------------------------------------------------------------------------
using namespace scalestore;
int main(int argc, char* argv[]) {
   using K = uint64_t;
   using V = BytesPayload<128>;

   gflags::SetUsageMessage("Catalog Test");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   // prepare workload
   std::vector<std::string> workload_type;  // warm up or benchmark
   std::vector<LatencyWorkload> workloads = {
       {.zipf = 0,
        {
            {.read_ratio = 5, .max_throughput = 1600000, .increment = 100000, .start_value = 100000},
            {.read_ratio = 50, .max_throughput = 1800000, .increment = 100000, .start_value = 100000},
            {.read_ratio = 95, .max_throughput = 2700000, .increment = 100000, .start_value = 100000},
            {.read_ratio = 100, .max_throughput = 2900000, .increment = 100000, .start_value = 100000},
        }},
       {.zipf = 1.25,
        {
            {.read_ratio = 5, .max_throughput = 700000, .increment = 25000, .start_value = 100},
            {.read_ratio = 50, .max_throughput = 700000, .increment = 25000, .start_value = 100},
            {.read_ratio = 95, .max_throughput = 2000000, .increment = 50000, .start_value = 100},
            {.read_ratio = 100, .max_throughput = 20000000, .increment = 500000, .start_value = 100},
        }}};

   if (FLAGS_YCSB_warm_up) {
      workload_type.push_back("YCSB_warm_up");
      workload_type.push_back("YCSB_txn");
   } else {
      workload_type.push_back("YCSB_txn");
   }


   ScaleStore scalestore;
   auto& catalog = scalestore.getCatalog();
   // -------------------------------------------------------------------------------------
   auto partition = [&](uint64_t id, uint64_t participants, uint64_t N) -> Partition {
      const uint64_t blockSize = N / participants;
      auto begin = id * blockSize;
      auto end = begin + blockSize;
      if (id == participants - 1) end = N;
      return {.begin = begin, .end = end};
   };

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
   auto nodePartition = partition(scalestore.getNodeID(), FLAGS_nodes, YCSB_tuple_count);
   // -------------------------------------------------------------------------------------
   // Build YCSB Table / Tree
   // -------------------------------------------------------------------------------------
   YCSB_workloadInfo builtInfo{"Build",     YCSB_tuple_count,       FLAGS_YCSB_read_ratio,
                               uint64_t(0), FLAGS_YCSB_zipf_factor, (FLAGS_YCSB_local_zipf ? "local_zipf" : "global_zipf")};
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
         barrier.wait();
         // -------------------------------------------------------------------------------------
         V value;
         for (K k_i = begin; k_i < end; k_i++) {
            utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&value), sizeof(V));
            tree.insert(k_i, value);
            threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
         }
         // -------------------------------------------------------------------------------------
         barrier.wait();
      });
   }
   scalestore.getWorkerPool().joinAll();
   scalestore.stopProfiler();

   if (FLAGS_YCSB_flush_pages) {
      std::cout << "Flushing all pages"
                << "\n";
      scalestore.getBuffermanager().writeAllPages();

      std::cout << "Done"
                << "\n";
   }

   for (auto& workload : workloads){
      auto ZIPF = workload.zipf;
      // -------------------------------------------------------------------------------------
      // YCSB Transaction
      // -------------------------------------------------------------------------------------
      std::unique_ptr<utils::ScrambledZipfGenerator> zipf_random;
      if (FLAGS_YCSB_partitioned)
         zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(nodePartition.begin, nodePartition.end - 1, ZIPF);
      else
         zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(0, YCSB_tuple_count, ZIPF);

      // -------------------------------------------------------------------------------------
      // zipf creation can take some time due to floating point loop therefore wait with barrier
      barrier_wait();
      // -------------------------------------------------------------------------------------
      for(auto p : workload.wl_desc){
         auto READ_RATIO = p.read_ratio;
         auto MAX_TROUGHPUT = p.max_throughput;
         auto START_TARGET_TROUGHPUT = p.start_value;
         auto TARGET_TROUGHPUT_INCREMENT = p.increment;
         for (uint64_t TARGET_TROUGHPUT = START_TARGET_TROUGHPUT; TARGET_TROUGHPUT <= MAX_TROUGHPUT;
              TARGET_TROUGHPUT += TARGET_TROUGHPUT_INCREMENT) {
            std::cout << "new target throughput = " << TARGET_TROUGHPUT << "\n";

            for (auto TYPE : workload_type) {
               barrier_wait();
               std::atomic<bool> keep_running = true;
               std::atomic<u64> running_threads_counter = 0;
               [[maybe_unused]] uint64_t zipf_offset = 0;
               if (FLAGS_YCSB_local_zipf) zipf_offset = (YCSB_tuple_count / FLAGS_nodes) * scalestore.getNodeID();

               YCSB_workloadInfo experimentInfo{
                   TYPE, YCSB_tuple_count, READ_RATIO, TARGET_TROUGHPUT, ZIPF, (FLAGS_YCSB_local_zipf ? "local_zipf" : "global_zipf")};
               scalestore.startProfiler(experimentInfo);
               std::vector<uint64_t> tl_microsecond_latencies[FLAGS_worker];
               for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
                  scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
                     running_threads_counter++;
                     // reserve tl_microsecond_latencies with lambda
                     uint64_t ops = 0;
                     // -------------------------------------------------------------------------------------
                     // generate events
                     auto ttp_per_sec = TARGET_TROUGHPUT / FLAGS_worker;
                     auto latency_samples = ttp_per_sec * 3;  // sample seconds
                     uint64_t samples = 0;
                     auto sum_arrival_times = poisson_process(ttp_per_sec, 10);
                     // -------------------------------------------------------------------------------------
                     storage::DistributedBarrier barrier(catalog.getCatalogEntry(BARRIER_ID).pid);
                     storage::BTree<K, V> tree(catalog.getCatalogEntry(BTREE_ID).pid);
                     barrier.wait();
                     auto start = std::chrono::high_resolution_clock::now();
                     for (auto& t : sum_arrival_times) {
                        // -------------------------------------------------------------------------------------
                        if (!keep_running) break;
                        // -------------------------------------------------------------------------------------
                        K key = zipf_random->rand(zipf_offset);
                        // K key = utils::RandomGenerator::getRandU64(0, YCSB_tuple_count-1);
                        ensure(key < YCSB_tuple_count);
                        V result;
                        // -------------------------------------------------------------------------------------
                        auto next = start + t;
                        wait_until_next(next);  // wait until we send next request
                        // -------------------------------------------------------------------------------------
                        if (READ_RATIO == 100 || utils::RandomGenerator::getRandU64(0, 100) < READ_RATIO) {
                           auto success = tree.lookup_opt(key, result);
                           ensure(success);
                           auto end = std::chrono::high_resolution_clock::now();
                           auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end - next);
                           // skip first second two seconds
                           if (ops > ttp_per_sec*2 && samples < latency_samples){
                              samples++;
                              tl_microsecond_latencies[t_i].push_back(latency.count());
                           }
                        } else {
                           V payload;
                           utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(V));
                           tree.insert(key, payload);
                           auto end = std::chrono::high_resolution_clock::now();
                           auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end - next);
                           if (ops > ttp_per_sec*2 && samples < latency_samples) {
                              samples++;
                              tl_microsecond_latencies[t_i].push_back(latency.count());
                           }
                        }
                        threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
                        ops++;
                     }
                     running_threads_counter--;
                  });
               }
               sleep(10);
               keep_running = false;
               while (running_threads_counter) {
                  _mm_pause();
               }
               // -------------------------------------------------------------------------------------
               // Join Threads
               // -------------------------------------------------------------------------------------
               scalestore.getWorkerPool().joinAll();
               // -------------------------------------------------------------------------------------
               scalestore.stopProfiler();
               // -------------------------------------------------------------------------------------

               // if warmup continue
               if (TYPE.compare("YCSB_txn") != 0) {
                  std::cout << "Skip writting to file "
                            << "\n";
                  continue;
               }

               // combine vector of threads into one
               std::vector<uint64_t> microsecond_latencies;
               for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
                  microsecond_latencies.insert(microsecond_latencies.end(), tl_microsecond_latencies[t_i].begin(),
                                               tl_microsecond_latencies[t_i].end());
               }

               {
                  std::cout << "Shuffle samples " << microsecond_latencies.size() << std::endl;
                  std::random_device rd;
                  std::mt19937 g(rd());
                  std::shuffle(microsecond_latencies.begin(), microsecond_latencies.end(), g);

                  // write out 1000 samples is possible 
                  std::ofstream latency_file;
                  std::ofstream::openmode open_flags = std::ios::app;
                  std::string filename = "latency_samples_" + FLAGS_csvFile;
                  bool csv_initialized = std::filesystem::exists(filename);
                  latency_file.open(filename, open_flags);
                  if (!csv_initialized) {
                     latency_file << "nodeId,workload,tag,ReadRatio,targetThroughput,YCSB_tuple_count,zipf,latency" << std::endl;
                  }

                  uint64_t samples_persist = (microsecond_latencies.size() > 1000) ? 1000 : microsecond_latencies.size();
                  
                  for (uint64_t s_i = 0; s_i < samples_persist; s_i++) {
                     latency_file << scalestore.getNodeID() << "," << TYPE << "," << FLAGS_tag << "," << READ_RATIO << ","
                                  << TARGET_TROUGHPUT << "," << YCSB_tuple_count << "," << ZIPF << "," << microsecond_latencies[s_i]
                                  << std::endl;
                  }
                  latency_file.close();
               }
               std::cout << "Sorting Latencies"
                         << "\n";

               std::sort(microsecond_latencies.begin(), microsecond_latencies.end());
               std::cout << "Latency (min/median/max/99%): " << (microsecond_latencies[0]) << ","
                         << (microsecond_latencies[microsecond_latencies.size() / 2]) << "," << (microsecond_latencies.back()) << ","
                         << (microsecond_latencies[(int)(microsecond_latencies.size() * 0.99)]) << std::endl;
               // -------------------------------------------------------------------------------------
               // write to csv file
               std::ofstream latency_file;
               std::ofstream::openmode open_flags = std::ios::app;
               std::string filename = "latency_" + FLAGS_csvFile;
               bool csv_initialized = std::filesystem::exists(filename);
               latency_file.open(filename, open_flags);
               if (!csv_initialized) {
                  latency_file << "nodeId,workload,tag,ReadRatio,targetThroughput,YCSB_tuple_count,zipf,min,median,max,95th,99th,999th"
                               << std::endl;
               }
               latency_file << scalestore.getNodeID() << "," << TYPE << "," << FLAGS_tag << "," << READ_RATIO << "," << TARGET_TROUGHPUT
                            << "," << YCSB_tuple_count << "," << ZIPF << "," << (microsecond_latencies[0]) << ","
                            << (microsecond_latencies[microsecond_latencies.size() / 2]) << "," << (microsecond_latencies.back()) << ","
                            << (microsecond_latencies[(int)(microsecond_latencies.size() * 0.95)]) << ","
                            << (microsecond_latencies[(int)(microsecond_latencies.size() * 0.99)]) << ","
                            << (microsecond_latencies[(int)(microsecond_latencies.size() * 0.999)]) << std::endl;
               latency_file.close();
            }
         }
      }
   }
   std::cout << "Starting hash table report "
             << "\n";
   scalestore.getBuffermanager().reportHashTableStats();
   return 0;
}
