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
static constexpr uint64_t BTREE_ID = 0;
static constexpr uint64_t BARRIER_ID = 1;
static constexpr uint64_t CUSTOM_BTREE_ID = 2;
// -------------------------------------------------------------------------------------
struct BTree_workloadInfo : public scalestore::profiling::WorkloadInfo {
   std::string experiment;
   uint64_t elements;
   uint64_t readRatio;
   double zipfFactor;

   BTree_workloadInfo(std::string experiment, uint64_t elements) : experiment(experiment), elements(elements) {}

   virtual std::vector<std::string> getRow() { return {experiment, std::to_string(elements)}; }

   virtual std::vector<std::string> getHeader() { return {"workload", "SSD data GB"}; }

   virtual void csv(std::ofstream& file) override {
      file << experiment << " , ";
      file << elements << " , ";
   }
   virtual void csvHeader(std::ofstream& file) override {
      file << "Workload"
           << " , ";
      file << "SSDdataGB"
           << " , ";
   }
};
// -------------------------------------------------------------------------------------
uint64_t estimateTuples(uint64_t sizeInGB, uint64_t sizeOfTuple) {
   return (((sizeInGB * 1024 * 1024 * 1024) / sizeOfTuple)) / 2;
};

// -------------------------------------------------------------------------------------
void test_integer_btree(scalestore::ScaleStore& scalestore) {
   using namespace scalestore;
   using K = uint64_t;
   using V = uint64_t;
   // -------------------------------------------------------------------------------------
   struct Record {
      K key;
      V value;
   };
   // -------------------------------------------------------------------------------------
   auto& catalog = scalestore.getCatalog();
   // -------------------------------------------------------------------------------------
   auto numberTuples = estimateTuples((FLAGS_dramGB / FLAGS_nodes) * 0.8, sizeof(Record));
   // -------------------------------------------------------------------------------------
   // LAMBDAS to reduce boilerplate code
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
   // 1. build tree mutlithreaded
   // -------------------------------------------------------------------------------------
   {
      BTree_workloadInfo builtInfo{"Build B-Tree", numberTuples};
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
      // Scan ASC
      // -------------------------------------------------------------------------------------
      BTree_workloadInfo builtInfo{"Scan Asc", numberTuples};
      scalestore.startProfiler(builtInfo);
      std::atomic<uint64_t> global_result{0};
      execute_on_tree([&](uint64_t /*t_i*/, storage::BTree<K, V>& tree) {
         uint64_t start = 0;
         uint64_t current =0;
         uint64_t scan_result = 0;
         tree.scan<storage::BTree<K, V>::ASC_SCAN>(start, [&scan_result,&current](K key, V value) {
            scan_result += value;
            ensure(current == key);
            current++;
            threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
            return true;
         });
         ensure (scan_result == numberTuples);
      });
      scalestore.stopProfiler();
   }

   {
      // -------------------------------------------------------------------------------------
      // Scan DESC
      // -------------------------------------------------------------------------------------
      BTree_workloadInfo builtInfo{"Scan Desc", numberTuples};
      scalestore.startProfiler(builtInfo);
      std::atomic<uint64_t> global_result{0};
      execute_on_tree([&](uint64_t /*t_i*/, storage::BTree<K, V>& tree) {
         uint64_t start = numberTuples;
         uint64_t current = numberTuples-1;
         uint64_t scan_result = 0;
         tree.scan<storage::BTree<K, V>::DESC_SCAN>(start, [&scan_result, &current](K key, V value) {
            scan_result += value;
            ensure(current == key);
            current--;
            threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
            return true;
         });
         ensure(scan_result == numberTuples);
      });
      scalestore.stopProfiler();
   }
   {
      // -------------------------------------------------------------------------------------
      // Remove half of the tuples  
      // -------------------------------------------------------------------------------------
      {
         BTree_workloadInfo builtInfo{"Remove range", numberTuples};
         scalestore.startProfiler(builtInfo);
         // -------------------------------------------------------------------------------------
         execute_on_tree([&](uint64_t t_i, storage::BTree<K, V>& tree) {
            uint64_t start = (scalestore.getNodeID() * FLAGS_worker) + t_i;
            const uint64_t inc = FLAGS_worker * FLAGS_nodes;
            for (; start < numberTuples/2; start = start + inc) {
               tree.insert(start, 1);
               auto removed = tree.remove(start);
               ensure(removed);
               threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
            }
         });
         scalestore.stopProfiler();
      }
   }
   {
      // -------------------------------------------------------------------------------------
      // RepeatScan ASC
      // -------------------------------------------------------------------------------------
      BTree_workloadInfo builtInfo{"Scan Asc", numberTuples};
      scalestore.startProfiler(builtInfo);
      std::atomic<uint64_t> global_result{0};
      execute_on_tree([&](uint64_t /*t_i*/, storage::BTree<K, V>& tree) {
         uint64_t start = 0;
         uint64_t scan_result = 0;
         tree.scan<storage::BTree<K, V>::ASC_SCAN>(start, [&](K key, V /*value*/) {
         if (key < numberTuples / 2) {
            scan_result++;
            return true;
         }
         return false;
         });
         ensure (scan_result == 0);
      });
      scalestore.stopProfiler();
   }

   {
      // -------------------------------------------------------------------------------------
      // Scan DESC
      // -------------------------------------------------------------------------------------
      BTree_workloadInfo builtInfo{"Scan Desc", numberTuples};
      scalestore.startProfiler(builtInfo);
      std::atomic<uint64_t> global_result{0};
      execute_on_tree([&](uint64_t /*t_i*/, storage::BTree<K, V>& tree) {
         uint64_t start = (numberTuples / 2) - 1;
         uint64_t scan_result = 0;
         tree.scan<storage::BTree<K, V>::DESC_SCAN>(start, [&](K key, V /*value*/) {
            if (key > 0) {
               scan_result++;
               return true;
            }
            return false;
         });
         ensure(scan_result == 0);
      });
      scalestore.stopProfiler();
   }
   }
// -------------------------------------------------------------------------------------
template <int maxLength>
struct Varchar {
   int16_t length;
   char data[maxLength];

   Varchar() : length(0) {}
   Varchar(const char* str) {
      int l = strlen(str);
      assert(l <= maxLength);
      length = l;
      memcpy(data, str, l);
   }
   template <int otherMaxLength>
   Varchar(const Varchar<otherMaxLength>& other) {
      assert(other.length <= maxLength);
      length = other.length;
      memcpy(data, other.data, length);
   }

   void append(char x) { data[length++] = x; };
   std::string toString() { return std::string(data, length); };

   template <int otherMaxLength>
   Varchar<maxLength> operator||(const Varchar<otherMaxLength>& other) const {
      Varchar<maxLength> tmp;
      assert((static_cast<int32_t>(length) + other.length) <= maxLength);
      tmp.length = length + other.length;
      memcpy(tmp.data, data, length);
      memcpy(tmp.data + length, other.data, other.length);
      return tmp;
   }

   bool operator==(const Varchar<maxLength>& other) const { return (length == other.length) && (memcmp(data, other.data, length) == 0); }

   bool operator>(const Varchar<maxLength>& other) const {
      int cmp = memcmp(data, other.data, (length < other.length) ? length : other.length);
      if (cmp)
         return cmp > 0;
      else
         return length > other.length;
   }

   bool operator<(const Varchar<maxLength>& other) const {
      int cmp = memcmp(data, other.data, (length < other.length) ? length : other.length);
      if (cmp)
         return cmp < 0;
      else
         return length < other.length;
   }
};

struct SecondaryKey {
   Varchar<16> name;
   int id;
   
   bool operator==(const SecondaryKey& other) const { return (id == other.id) && (name == other.name); }

   bool operator<(const SecondaryKey& other) const {
      if (name < other.name) return true;
      if (name > other.name) return false;
      // equal
      return(id < other.id);
   }
   bool operator>(const SecondaryKey& other) const {
      if( name > other.name) return true;
      if( name < other.name) return false;
      return (id > other.id);
   }
   bool operator>=(const SecondaryKey& other) const { return !(*this < other); }
};

std::ostream& operator<<(std::ostream& os, SecondaryKey& c) {
   os << c.name.toString() << " " << c.id;
   return os;
}

// -------------------------------------------------------------------------------------
void test_custom_key_btree(scalestore::ScaleStore& scalestore) {
   std::vector<std::string> names = {"mueller", "el-hindi", "feil"};

   using namespace scalestore;
   using K = SecondaryKey;
   using V = uint64_t;
   // -------------------------------------------------------------------------------------
   struct Record {
      K key;
      V value;
   };
   // -------------------------------------------------------------------------------------
   auto& catalog = scalestore.getCatalog();
   // -------------------------------------------------------------------------------------
   auto numberTuples = estimateTuples((FLAGS_dramGB / FLAGS_nodes) * 0.8, sizeof(Record));
   // -------------------------------------------------------------------------------------
   // LAMBDAS to reduce boilerplate code
   auto execute_on_tree = [&](std::function<void(uint64_t t_i, storage::BTree<K, V> & tree)> callback) {
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            // -------------------------------------------------------------------------------------
            storage::BTree<K, V> tree(catalog.getCatalogEntry(CUSTOM_BTREE_ID).pid);
            // -------------------------------------------------------------------------------------
            storage::DistributedBarrier barrier(catalog.getCatalogEntry(BARRIER_ID).pid);
            barrier.wait();
            // -------------------------------------------------------------------------------------
            callback(t_i, tree);
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
      });
   }
   // -------------------------------------------------------------------------------------
   // 1. build tree mutlithreaded
   // -------------------------------------------------------------------------------------
   {
      BTree_workloadInfo builtInfo{"Build B-Tree", numberTuples};
      scalestore.startProfiler(builtInfo);
      // -------------------------------------------------------------------------------------
      execute_on_tree([&](uint64_t t_i, storage::BTree<K, V>& tree) {
         uint64_t start = (scalestore.getNodeID() * FLAGS_worker) + t_i;
         const uint64_t inc = FLAGS_worker * FLAGS_nodes;
         for (; start < numberTuples; start = start + inc) {
            tree.insert({.name = names[start%names.size()].c_str(), .id=(int)start}, 1);
            threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
         }
      });
      scalestore.stopProfiler();
   }

   {
      // -------------------------------------------------------------------------------------
      // Scan ASC
      // -------------------------------------------------------------------------------------
      BTree_workloadInfo builtInfo{"Scan Asc", numberTuples};
      scalestore.startProfiler(builtInfo);
      std::atomic<uint64_t> global_result{0};
      execute_on_tree([&](uint64_t /*t_i*/, storage::BTree<K, V>& tree) {
         SecondaryKey start = {.name = "", .id = 0};
         uint64_t scan_result = 0;
         tree.scan<storage::BTree<K, V>::ASC_SCAN>(start, [&scan_result]([[maybe_unused]] K key, V value) {
                                                             
            scan_result += value;
            threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
            return true;
         });
         ensure (scan_result == numberTuples);
      });
      scalestore.stopProfiler();
   }
}

// -------------------------------------------------------------------------------------
using namespace scalestore;
int main(int argc, char* argv[]) {
   // -------------------------------------------------------------------------------------
   gflags::SetUsageMessage("BTree tests");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   ensure(FLAGS_nodes == 1);
   ScaleStore scalestore;
   // -------------------------------------------------------------------------------------
   test_integer_btree(scalestore);
   // -------------------------------------------------------------------------------------
   test_custom_key_btree(scalestore);
   return 0;
};
