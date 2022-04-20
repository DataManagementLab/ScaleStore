#include "adapter.hpp"
#include "scalestore/Config.hpp"
#include "scalestore/ScaleStore.hpp"
#include "scalestore/rdma/CommunicationManager.hpp"
#include "scalestore/storage/datastructures/BTree.hpp"
#include "scalestore/utils/RandomGenerator.hpp"
#include "scalestore/utils/ScrambledZipfGenerator.hpp"
#include "scalestore/utils/Time.hpp"
#include "schema.hpp"
#include "types.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <unistd.h>
#include <iostream>
#include <set>
#include <string>
#include <vector>
// -------------------------------------------------------------------------------------
DEFINE_uint32(tpcc_warehouse_count, 1, "");
DEFINE_int32(tpcc_abort_pct, 0, "");
DEFINE_uint64(run_until_tx, 0, "");
DEFINE_bool(tpcc_warehouse_affinity, false, "");  // maps every thread to a dedicated warehouse
DEFINE_bool(tpcc_warehouse_locality, false, "");  // maps threads to local warehouses
DEFINE_bool(tpcc_fast_load, false, "");
DEFINE_bool(tpcc_remove, true, "");
DEFINE_double(TPCC_run_for_seconds, 10.0, "");
// -------------------------------------------------------------------------------------
using namespace scalestore;
// adapter and ids
ScaleStoreAdapter<warehouse_t> warehouse;       // 0
ScaleStoreAdapter<district_t> district;         // 1
ScaleStoreAdapter<customer_t> customer;         // 2
ScaleStoreAdapter<customer_wdl_t> customerwdl;  // 3
ScaleStoreAdapter<history_t> history;           // 4
ScaleStoreAdapter<neworder_t> neworder;         // 5
ScaleStoreAdapter<order_t> order;               // 6
ScaleStoreAdapter<order_wdc_t> order_wdc;       // 7
ScaleStoreAdapter<orderline_t> orderline;       // 8
ScaleStoreAdapter<item_t> item;                 // 9
ScaleStoreAdapter<stock_t> stock;               // 10
static constexpr uint64_t barrier_id = 11;

// -------------------------------------------------------------------------------------
// yeah, dirty include...
#include "tpcc_workload.hpp"
// -------------------------------------------------------------------------------------
double calculateMTPS(std::chrono::high_resolution_clock::time_point begin, std::chrono::high_resolution_clock::time_point end, u64 factor) {
   double tps = ((factor * 1.0 / (std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0)));
   return (tps / 1000000.0);
}
// -------------------------------------------------------------------------------------
struct TPCC_workloadInfo : public scalestore::profiling::WorkloadInfo {
   std::string experiment;
   uint64_t warehouses;
   std::string configuration;


   TPCC_workloadInfo(std::string experiment, uint64_t warehouses, std::string configuration)
       : experiment(experiment), warehouses(warehouses), configuration(configuration)
   {
   }

   
   virtual std::vector<std::string> getRow(){
      return {experiment,std::to_string(warehouses), configuration};
   }

   virtual std::vector<std::string> getHeader(){
      return {"experiment","warehouses","configuration"};
   }
   

   virtual void csv(std::ofstream& file) override
   {
      file << experiment << " , ";
      file << warehouses << " , ";
      file << configuration << " , ";
   }
   virtual void csvHeader(std::ofstream& file) override
   {
      file << "Experiment"
           << " , ";
      file << "Warehouses"
           << " , ";
      file << "Configuration"
           << " , ";
   }
};
// -------------------------------------------------------------------------------------
// MAIN
int main(int argc, char* argv[]) {
   gflags::SetUsageMessage("Leanstore TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   ScaleStore db;
   auto& catalog = db.getCatalog();
   // -------------------------------------------------------------------------------------
   auto barrier_wait = [&]() {
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         db.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            storage::DistributedBarrier barrier(catalog.getCatalogEntry(barrier_id).pid);
            barrier.wait();
         });
      }
      db.getWorkerPool().joinAll();
   };
   // -------------------------------------------------------------------------------------

   auto partition = [&](uint64_t id, uint64_t participants, uint64_t N) -> Partition {
      const uint64_t blockSize = N / participants;
      auto begin = id * blockSize;
      auto end = begin + blockSize;
      if (id == participants - 1) end = N;
      return {.begin = begin, .end = end};
   };
   // -------------------------------------------------------------------------------------
   warehouseCount = FLAGS_tpcc_warehouse_count;  // xxx remove as it is defined in workload
   // -------------------------------------------------------------------------------------
   // verify configuration
   ensure((uint64_t)warehouseCount >= FLAGS_nodes);
   if (FLAGS_tpcc_warehouse_affinity) ensure((uint64_t)warehouseCount >= (FLAGS_worker * FLAGS_nodes));
   // -------------------------------------------------------------------------------------
   // generate tables
   db.getWorkerPool().scheduleJobSync(0, [&]() {
      warehouse = ScaleStoreAdapter<warehouse_t>(db, "warehouse");
      district = ScaleStoreAdapter<district_t>(db, "district");
      customer = ScaleStoreAdapter<customer_t>(db, "customer");
      customerwdl = ScaleStoreAdapter<customer_wdl_t>(db, "customerwdl");
      history = ScaleStoreAdapter<history_t>(db, "history");
      neworder = ScaleStoreAdapter<neworder_t>(db, "neworder");
      order = ScaleStoreAdapter<order_t>(db, "order");
      order_wdc = ScaleStoreAdapter<order_wdc_t>(db, "order_wdc");
      orderline = ScaleStoreAdapter<orderline_t>(db, "orderline");
      item = ScaleStoreAdapter<item_t>(db, "item");
      stock = ScaleStoreAdapter<stock_t>(db, "stock");
      // -------------------------------------------------------------------------------------
      if (db.getNodeID() == 0) db.createBarrier(FLAGS_worker * FLAGS_nodes);  // distributed barrier
   });
   // -------------------------------------------------------------------------------------
   // load data
   warehouse_range_node = partition(db.getNodeID(), FLAGS_nodes, warehouseCount);

   // load items on node 0 as it is read-only and will be replicated anyway
   if (db.getNodeID() == 0) {
      db.getWorkerPool().scheduleJobSync(0, [&]() { loadItem(); });
   }

   // generate warehouses
   db.getWorkerPool().scheduleJobSync(0, [&]() { loadWarehouse(warehouse_range_node.begin, warehouse_range_node.end); });

   {  // populate rest of the tables from
      std::atomic<uint32_t> g_w_id = warehouse_range_node.begin + 1;
      for (uint32_t t_i = 0; t_i < FLAGS_worker; t_i++) {
         db.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            thread_id = t_i + (db.getNodeID() * FLAGS_worker);
            ensure((uint64_t)thread_id < MAX_THREADS);
            while (true) {
               uint32_t w_id = g_w_id++;
               if (w_id > warehouse_range_node.end) { return; }
               // start tx
               loadStock(w_id);
               loadDistrinct(w_id);
               for (Integer d_id = 1; d_id <= 10; d_id++) {
                  loadCustomer(w_id, d_id);
                  loadOrders(w_id, d_id);
               }
               // commit tx
            }
         });
      }
      db.getWorkerPool().joinAll();
   }

   auto consistencyCheck = [&]() {
      db.getWorkerPool().scheduleJobSync(0, [&]() {
         // -------------------------------------------------------------------------------------
         // consistency check
         uint64_t warehouse_tbl = 0;
         uint64_t district_tbl = 0;
         uint64_t customer_tbl = 0;
         uint64_t customer_wdl_tbl = 0;
         uint64_t history_tbl = 0;
         uint64_t neworder_tbl = 0;
         uint64_t order_tbl = 0;
         uint64_t order_wdc_tbl = 0;
         uint64_t orderline_tbl = 0;
         uint64_t item_tbl = 0;
         uint64_t stock_tbl = 0;

         warehouse.scan({.w_id = 0}, [&]([[maybe_unused]] warehouse_t::Key key, [[maybe_unused]] warehouse_t record) {
            warehouse_tbl++;
            return true;
         });
         district.scan({.d_w_id = 0, .d_id = 0}, [&]([[maybe_unused]] district_t::Key key, [[maybe_unused]] district_t record) {
            district_tbl++;
            return true;
         });
         customer.scan({.c_w_id = 0, .c_d_id = 0, .c_id = 0},
                       [&]([[maybe_unused]] customer_t::Key key, [[maybe_unused]] customer_t record) {
                          customer_tbl++;
                          return true;
                       });
         customerwdl.scan({.c_w_id = 0, .c_d_id = 0, .c_last = "", .c_first = ""},
                          [&]([[maybe_unused]] customer_wdl_t::Key key, [[maybe_unused]] customer_wdl_t record) {
                             customer_wdl_tbl++;
                             return true;
                          });
         history.scan({.thread_id = 0, .h_pk = 0}, [&]([[maybe_unused]] history_t::Key key, [[maybe_unused]] history_t record) {
            history_tbl++;
            return true;
         });
         neworder.scan({.no_w_id = 0, .no_d_id = 0, .no_o_id = 0},
                       [&]([[maybe_unused]] neworder_t::Key key, [[maybe_unused]] neworder_t record) {
                          neworder_tbl++;
                          return true;
                       });

         order.scan({.o_w_id = 0, .o_d_id = 0, .o_id = 0}, [&]([[maybe_unused]] order_t::Key key, [[maybe_unused]] order_t record) {
            order_tbl++;
            return true;
         });

         order_wdc.scan({.o_w_id = 0, .o_d_id = 0, .o_c_id = 0, .o_id = 0},
                        [&]([[maybe_unused]] order_wdc_t::Key key, [[maybe_unused]] order_wdc_t record) {
                           order_wdc_tbl++;
                           return true;
                        });

         orderline.scan({.ol_w_id = 0, .ol_d_id = 0, .ol_o_id = 0, .ol_number = 0},
                        [&]([[maybe_unused]] orderline_t::Key key, [[maybe_unused]] orderline_t record) {
                           orderline_tbl++;
                           return true;
                        });

         item.scan({.i_id = 0}, [&]([[maybe_unused]] item_t::Key key, [[maybe_unused]] item_t record) {
            item_tbl++;
            return true;
         });

         stock.scan({.s_w_id = 0, .s_i_id = 0}, [&]([[maybe_unused]] stock_t::Key key, [[maybe_unused]] stock_t record) {
            stock_tbl++;
            return true;
         });

         std::cout << "#Tuples in tables:" << std::endl;
         std::cout << "#Warehouse " << warehouse_tbl << "\n";
         std::cout << "#district " << district_tbl << "\n";
         std::cout << "#customer " << customer_tbl << "\n";
         std::cout << "#customer wdl " << customer_wdl_tbl << "\n";
         std::cout << "#history " << history_tbl << "\n";
         std::cout << "#neworder " << neworder_tbl << "\n";
         std::cout << "#order " << order_tbl << "\n";
         std::cout << "#order wdl " << order_wdc_tbl << "\n";
         std::cout << "#orderline " << orderline_tbl << "\n";
         std::cout << "#item " << item_tbl << "\n";
         std::cout << "#stock " << stock_tbl << "\n";
      });
   };

   consistencyCheck();
   sleep(2);
   // -------------------------------------------------------------------------------------
   double gib = (db.getBuffermanager().getConsumedPages() * storage::EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   std::cout << "data loaded - consumed space in GiB = " << gib << std::endl;

   // -------------------------------------------------------------------------------------
   barrier_wait();
   // -------------------------------------------------------------------------------------
   {
      std::atomic<bool> keep_running = true;
      std::atomic<u64> running_threads_counter = 0;
      u64 tx_per_thread[FLAGS_worker];
      u64 remote_new_order_per_thread[FLAGS_worker];  // per specification
      u64 remote_tx_per_thread[FLAGS_worker];
      u64 delivery_aborts_per_thread[FLAGS_worker];
      u64 txn_profile[FLAGS_worker][transaction_types::MAX];
      u64 txn_lat[FLAGS_worker][transaction_types::MAX];
      u64 txn_pay_lat[FLAGS_worker][10];
      std::string configuration;
      if (FLAGS_tpcc_warehouse_affinity) {
         configuration = "warehouse_affinity";
      } else if (FLAGS_tpcc_warehouse_locality) {
         configuration = "warehouse_locality";
      } else {
         configuration = "warehouse_no_locality";
      }
      TPCC_workloadInfo experimentInfo{"TPCC", (uint64_t)warehouseCount, configuration};
      db.startProfiler(experimentInfo);
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         db.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            running_threads_counter++;
            thread_id = t_i + (db.getNodeID() * FLAGS_worker);
            volatile u64 tx_acc = 0;
            storage::DistributedBarrier barrier(catalog.getCatalogEntry(barrier_id).pid);
            barrier.wait();
            while (keep_running) {
               uint32_t w_id;
               if (FLAGS_tpcc_warehouse_affinity) {
                  w_id = t_i + 1 + warehouse_range_node.begin;
               } else if (FLAGS_tpcc_warehouse_locality) {
                  w_id = urand(warehouse_range_node.begin + 1, warehouse_range_node.end);
               } else {
                  w_id = urand(1, FLAGS_tpcc_warehouse_count);
                  if (w_id <= (uint32_t)warehouse_range_node.begin || (w_id > (uint32_t)warehouse_range_node.end)) remote_node_new_order++;
               }
               tx(w_id);
               /*
               if (FLAGS_tpcc_abort_pct && urand(0, 100) <= FLAGS_tpcc_abort_pct) {
                  // abort
               } else {
                  // commit
               }
               */

               tx_acc++;
               threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
            }
            tx_per_thread[t_i] = tx_acc;
            remote_new_order_per_thread[t_i] = remote_new_order;
            remote_tx_per_thread[t_i] = remote_node_new_order;
            delivery_aborts_per_thread[t_i] = delivery_aborts;
            int idx = 0;
            for (auto& tx_count : txns)
               txn_profile[t_i][idx++] = tx_count;
            idx = 0;
            for (auto& tx_l : txn_latencies) {
               txn_lat[t_i][idx] = (tx_l / (double)txn_profile[t_i][idx]);
               idx++;
            }

            idx = 0;
            for (auto& tx_l : txn_paymentbyname_latencies) {
               txn_pay_lat[t_i][idx] = ((tx_l) / (double)txn_profile[t_i][transaction_types::STOCK_LEVEL]);
               idx++;
            }
            running_threads_counter--;
         });
      }
      // -------------------------------------------------------------------------------------
      // Join Threads
      // -------------------------------------------------------------------------------------
      sleep(FLAGS_TPCC_run_for_seconds);
      keep_running = false;
      while (running_threads_counter) {
         _mm_pause();
      }
      db.getWorkerPool().joinAll();
      // -------------------------------------------------------------------------------------
      db.stopProfiler();

      std::cout << "tx per thread " << std::endl;
      for (u64 t_i = 0; t_i < FLAGS_worker; t_i++) {
         std::cout << tx_per_thread[t_i] << ",";
      }
      std::cout << "\n";

      std::cout << "remote node txn " << std::endl;

      for (u64 t_i = 0; t_i < FLAGS_worker; t_i++) {
         std::cout << remote_tx_per_thread[t_i] << ",";
      }
      std::cout << "\n";
      std::cout << "remote new order per specification " << std::endl;
      for (u64 t_i = 0; t_i < FLAGS_worker; t_i++) {
         std::cout << remote_new_order_per_thread[t_i] << ",";
      }
      std::cout << std::endl;

      std::cout << "aborts " << std::endl;
      for (u64 t_i = 0; t_i < FLAGS_worker; t_i++) {
         std::cout << delivery_aborts_per_thread[t_i] << ",";
      }
      std::cout << std::endl;
      std::cout << "txn profile "
                << "\n";

      for (u64 t_i = 0; t_i < FLAGS_worker; t_i++) {
         for (u64 i = 0; i < transaction_types::MAX; i++) {
            std::cout << txn_profile[t_i][i] << ",";
         }
         std::cout << "\n";
      }
      std::cout << "\n";

      for (u64 t_i = 0; t_i < FLAGS_worker; t_i++) {
         for (u64 i = 0; i < transaction_types::MAX; i++) {
            std::cout << txn_lat[t_i][i] << ",";
         }
         std::cout << "\n";
      }
      std::cout << "\n";

      for (u64 t_i = 0; t_i < FLAGS_worker; t_i++) {
         for (u64 i = 0; i < 10; i++) {
            std::cout << txn_pay_lat[t_i][i] << ",";
         }
         std::cout << "\n";
      }
      std::cout << "\n";
      double gib = (db.getBuffermanager().getConsumedPages() * storage::EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
      std::cout << "data loaded - consumed space in GiB = " << gib << std::endl;
      std::cout << "Starting hash table report "
                << "\n";
      db.getBuffermanager().reportHashTableStats();
   }
   consistencyCheck();

   return 0;
}
