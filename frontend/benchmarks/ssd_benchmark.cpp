#include "PerfEvent.hpp"
#include "scalestore/Config.hpp"
#include "scalestore/ScaleStore.hpp"
#include "scalestore/rdma/CommunicationManager.hpp"
#include "scalestore/storage/datastructures/BTree.hpp"
#include "scalestore/storage/datastructures/SmallMap.hpp"
#include "scalestore/utils/RandomGenerator.hpp"
#include "scalestore/utils/ScrambledZipfGenerator.hpp"
#include "scalestore/utils/Time.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
static constexpr uint64_t BARRIER_ID = 0;
// -------------------------------------------------------------------------------------
DEFINE_uint32(writer_threads, 1,"");
DEFINE_uint32(run_for_seconds, 30, "");

struct SSD_workloadInfo : public scalestore::profiling::WorkloadInfo {
   std::string experiment;
   uint64_t writer_threads = FLAGS_writer_threads;
   uint64_t reader_threads = FLAGS_worker;
   std::atomic<int64_t>& w_pages;
   uint64_t timestamp = 0;

   SSD_workloadInfo(std::string experiment, std::atomic<int64_t>& w_pages)
      : experiment(experiment), w_pages(w_pages)
   {
   }

   
   virtual std::vector<std::string> getRow(){
      return {
          experiment, std::to_string(writer_threads), std::to_string(reader_threads), std::to_string(w_pages), std::to_string(timestamp++),
      };
   }

   virtual std::vector<std::string> getHeader(){
      return {"experiment","writers","readers", "w_pages", "timestamp"};
   }
   

   virtual void csv(std::ofstream& file) override
   {
      file << experiment << " , ";
      file << writer_threads << " , ";
      file << reader_threads << " , ";
      file << w_pages << " , ";
      file << timestamp << " , ";
   }
   virtual void csvHeader(std::ofstream& file) override
   {
      file << "experiment"
           << " , ";
      file << "writers"
           << " , ";
      file << "readers"
           << " , ";
      file << "w_pages"
           << " , ";
      file << "Timestamp"
           << " , ";
   }
};

// -------------------------------------------------------------------------------------
using namespace scalestore;
int main(int argc, char* argv[]) {
   // -------------------------------------------------------------------------------------
   gflags::SetUsageMessage("Internal hash table benchmark");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   ScaleStore scalestore;
   auto bufferSize = scalestore.getBuffermanager().getDramPoolSize();
   // fill until full
   std::vector<PID> pids;
   scalestore.getWorkerPool().scheduleJobSync(0, [&]() {
      for (uint64_t op_i = 0; op_i < bufferSize * 0.95; op_i++) {
         scalestore::storage::ExclusiveBFGuard new_page_guard;
         pids.push_back(new_page_guard.getFrame().pid);
      }
   });
   scalestore.getBuffermanager().reportHashTableStats();
   // -------------------------------------------------------------------------------------
   std::vector<std::thread> writer_threads;
   concurrency::Barrier barrier(FLAGS_worker + FLAGS_writer_threads);
   std::atomic<bool> stopped {false};
   std::atomic<int64_t> w_pages = 0;
      
   for(uint64_t t_i =0; t_i < FLAGS_writer_threads; t_i++)
      writer_threads.emplace_back([&, t_i]() {
         storage::AsyncWriteBuffer async_write_buffer(scalestore.getSSDFD(), storage::PAGE_SIZE, 1024);
         barrier.wait();

         while(!stopped){
            auto idx = utils::RandomGenerator::getRandU64(0, pids.size());
            scalestore::storage::SharedBFGuard sguard(pids[idx]);
            if (async_write_buffer.full()) {
               // poll
               const uint64_t polled_events = async_write_buffer.pollEventsSync();
               if (polled_events > 0) {
                  async_write_buffer.getWrittenBfs(
                     [&](storage::BufferFrame& frame, uint64_t /*epoch*/) {
                        // todo get pid because it could have changed with io_map
                        frame.latch.unlatchShared();
                        w_pages++;
                     },
                     polled_events);
               }
            }
            async_write_buffer.add(sguard.getFrame(), sguard.getFrame().pid, 0);
            
         }
      });

   // pin
   for (auto& t : writer_threads)
      threads::CoreManager::getInstance().pinThreadRoundRobin(t.native_handle());

   // worker threads reading from ssd

   SSD_workloadInfo builtInfo{"SSD Benchmark", w_pages};
   scalestore.startProfiler(builtInfo);
   
   for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
      scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
         barrier.wait();
         auto idx = utils::RandomGenerator::getRandU64(0, pids.size());
         scalestore::storage::SharedBFGuard sguard(pids[idx]);
         scalestore.getBuffermanager().readPageSync(sguard.getFrame().pid, reinterpret_cast<uint8_t*>(sguard.getFrame().page));
      });
   }
   sleep(FLAGS_run_for_seconds);
   stopped = true;
   scalestore.getWorkerPool().joinAll();
   scalestore.stopProfiler();

   for (auto& t : writer_threads){
      t.join();
   }

   return 0;
}
