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
DEFINE_uint64(run_for_operations, 1e6, "Total number of operations executed per thread");
DEFINE_uint32(simulated_write_ratio, 0, "");
DEFINE_uint32(simulated_eviction_window, 10, "");
DEFINE_uint32(buffer_to_ssd_ratio, 1, "");
DEFINE_uint32(frames_to_evicted_frames_ratio, 0, "");
DEFINE_uint32(run_for_seconds, 30, "");
DEFINE_bool(test_max_read_perf, false, "");
DEFINE_bool(io_map, false, "");
DEFINE_bool(new_code, true, "");
DEFINE_bool(linear_model, false, "");
DEFINE_bool(write_to_SSD, true, "");

// -------------------------------------------------------------------------------------
struct OLAP_workloadInfo : public scalestore::profiling::WorkloadInfo {
   std::atomic<int64_t>& pages;
   std::atomic<int64_t>& freed_pages;
   std::atomic<int64_t>& w_pages;
   std::atomic<int64_t>& hit_empty_bf;
   std::atomic<int64_t>& failed_to_upgrade;
   uint64_t& eviction_window;
   std::atomic<int64_t>& sampled;
   std::atomic<int64_t>& found_candidates;
   std::atomic<int64_t>& pp_rounds_active;
   std::atomic<int64_t>& hit_spinlock;
   std::atomic<int64_t>& adjusted_model;

   int64_t tmp_freed_pages;
   int64_t tmp_w_pages;
   int64_t tmp_hit_empty_bf;
   int64_t tmp_failed_to_upgrade;
   int64_t tmp_sampled;
   int64_t tmp_found_candidates;
   int64_t tmp_pp_rounds_active;
   int64_t tmp_hit_spinlock;
   int64_t tmp_adjusted_model;

   uint64_t timestamp = 0;
   uint64_t simulated_write_ratio = FLAGS_simulated_write_ratio;
   uint64_t simulated_eviction_window = FLAGS_simulated_eviction_window;
   uint64_t buffer_to_ssd_ratio = FLAGS_buffer_to_ssd_ratio;
   uint64_t frames_to_evicted_frames_ratio = FLAGS_frames_to_evicted_frames_ratio;
   std::string code = FLAGS_new_code ? "new" : "old";
   std::string io_map = FLAGS_io_map ? "true" : "false";

   OLAP_workloadInfo(std::atomic<int64_t>& pages,
                     std::atomic<int64_t>& freed_pages,
                     std::atomic<int64_t>& w_pages,
                     std::atomic<int64_t>& hit_empty_bf,
                     std::atomic<int64_t>& failed_to_upgrade,
                     uint64_t& eviction_window,
                     std::atomic<int64_t>& sampled,
                     std::atomic<int64_t>& found_candidates,
                     std::atomic<int64_t>& pp_rounds_active,
                     std::atomic<int64_t>& hit_spinlock,
                     std::atomic<int64_t>& adjusted_model)
       : pages(pages),
         freed_pages(freed_pages),
         w_pages(w_pages),
         hit_empty_bf(hit_empty_bf),
         failed_to_upgrade(failed_to_upgrade),
         eviction_window(eviction_window),
         sampled(sampled),
         found_candidates(found_candidates),
         pp_rounds_active(pp_rounds_active),
         hit_spinlock(hit_spinlock),
         adjusted_model(adjusted_model) {}

   virtual std::vector<std::string> getRow() {
      tmp_freed_pages = freed_pages.exchange(0);
      tmp_w_pages = w_pages.exchange(0);
      tmp_hit_empty_bf = hit_empty_bf.exchange(0);
      tmp_failed_to_upgrade = failed_to_upgrade.exchange(0);
      tmp_sampled = sampled.exchange(0);
      tmp_found_candidates = found_candidates.exchange(0);
      tmp_pp_rounds_active = pp_rounds_active.exchange(0);
      tmp_hit_spinlock = hit_spinlock.exchange(0);
      tmp_adjusted_model = adjusted_model.exchange(0);

      return {std::to_string(pages.load()),
              std::to_string(tmp_freed_pages),
              std::to_string(tmp_w_pages),
              std::to_string(tmp_hit_empty_bf),
              std::to_string(timestamp++),
              std::to_string(simulated_write_ratio),
              std::to_string(simulated_eviction_window),
              std::to_string(buffer_to_ssd_ratio),
              std::to_string(frames_to_evicted_frames_ratio),
              code,
              io_map,
              std::to_string(tmp_failed_to_upgrade),
              std::to_string(eviction_window),
              std::to_string(tmp_sampled),
              std::to_string(tmp_found_candidates),
              std::to_string(tmp_pp_rounds_active),
              std::to_string(tmp_hit_spinlock),
              std::to_string(tmp_adjusted_model)};
   }

   virtual std::vector<std::string> getHeader() {
      return {"exp_pages",
              "exp_freed_pages",
              "w_pages",
              "h_empty_bf",
              "timestamp",
              "simulated_write_ratio",
              "simulated_eviction_window",
              "buffer_to_ssd_ratio",
              "frames_to_evicted_frames_ratio",
              "code",
              "io_map",
              "failed_to_upgrade",
              "eviction_window",
              "sampled",
              "found_candidates",
              "pp_rounds_active",
              "hit_spinlock",
              "adjusted_model"};
   }

   virtual void csv(std::ofstream& file) override {
      file << pages << " , ";
      file << tmp_freed_pages << " , ";
      file << tmp_w_pages << " , ";
      file << tmp_hit_empty_bf << " , ";
      file << timestamp << " , ";
      file << simulated_write_ratio << " , ";
      file << simulated_eviction_window << " , ";
      file << buffer_to_ssd_ratio << " , ";
      file << frames_to_evicted_frames_ratio << " , ";
      file << code << " , ";
      file << io_map << " , ";
      file << tmp_failed_to_upgrade << " , ";
      file << eviction_window << " , ";
      file << tmp_sampled << " , ";
      file << tmp_found_candidates << " , ";
      file << tmp_pp_rounds_active << " , ";
      file << tmp_hit_spinlock << " , ";
      file << tmp_adjusted_model << " , ";
   }
   virtual void csvHeader(std::ofstream& file) override {
      file << "exp_pages, exp_freed_pages, w_pages, h_empty_bf, timestamp, simulated_write_ratio, "
              "simulated_eviction_window,buffer_to_ssd_ratio, frames_to_evicted_frames_ratio, code, io_map, failed_to_upgrade, "
              "eviction_window,sampled,found_candidates,pp_rounds_active,hit_spinlock,adjusted_model,";
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
   auto number_bfs = scalestore.getBuffermanager().getNumberBufferframes();
   // fill until full
   std::vector<PID> pids;
   scalestore.getWorkerPool().scheduleJobSync(0, [&]() {
      for (uint64_t op_i = 0; op_i < bufferSize * 0.95; op_i++) {
         scalestore::storage::ExclusiveBFGuard new_page_guard;
         pids.push_back(new_page_guard.getFrame().pid);
         // to simulate steady state set dirty flags
         // ensures that percentag of dirty is according to workload
         if (utils::RandomGenerator::getRandU64(0, 100) <= FLAGS_simulated_write_ratio) {
            new_page_guard.getFrame().dirty = true;
         } else {
            new_page_guard.getFrame().dirty = false;
         }
      }
   });
   scalestore.getBuffermanager().reportHashTableStats();

   // simulates evicted frames i.e. increases the collision and work in the hash table
   scalestore.getWorkerPool().scheduleJobSync(0, [&]() {
      const uint64_t ratio = FLAGS_frames_to_evicted_frames_ratio;
      const uint64_t target_frame = (bufferSize * 0.95);
      uint64_t page_slot = 0;

      for (uint64_t r_i = 0; r_i < ratio; r_i++) {
         page_slot = 0;
         NodeID nid = r_i + 1;  // not owner
         for (uint64_t p_i = 0; p_i < target_frame; p_i++) {
            PID randomPID(nid, page_slot);
            scalestore.getBuffermanager().insertFrame(randomPID, [&](storage::BufferFrame& frame) {
               frame.latch.latchExclusive();
               frame.page = nullptr;
               frame.pid = randomPID;
               frame.setPossession(storage::POSSESSION::EXCLUSIVE);
               frame.setPossessor(scalestore.getNodeID() + 1);
               frame.state = storage::BF_STATE::EVICTED;
               frame.pVersion = 0;
            });
            page_slot++;
         }
      }
   });
   scalestore.getBuffermanager().reportHashTableStats();

   std::cout << "BufferFrames in HT " << (bufferSize * 0.95) * (1 + FLAGS_frames_to_evicted_frames_ratio) << std::endl;

   std::atomic<bool> stopped = false;
   std::atomic<bool> pp_stopped = false;
   std::atomic<uint64_t> count = 0;
   std::atomic<uint64_t> filled = 0;
   std::atomic<int64_t> pages = 0;
   std::atomic<int64_t> freed_pages = 0;
   std::atomic<int64_t> hit_empty_bf = 0;
   std::atomic<int64_t> w_pages = 0;
   std::atomic<int64_t> failed_to_upgrade = 0;
   std::atomic<int64_t> spinlock = 0;
   std::atomic<int64_t> total_read_pages = 0;
   std::atomic<int64_t> total_written_pages = 0;
   std::atomic<int64_t> sampled = 0;
   std::atomic<int64_t> found_candidates = 0;
   std::atomic<int64_t> pp_rounds_active = 0;
   std::atomic<int64_t> adjusted_model = 0;
   uint64_t eviction_window = 0;

   constexpr uint64_t SAMPLES = 50;

   int64_t freeBFLimit = (std::ceil(((FLAGS_freePercentage * 1.0 * (bufferSize * 0.95)) / 100.0)));
   std::thread pp([&]() {
      using namespace storage;

      uint64_t old_page_count = pages;
      uint64_t evict_counter = 0;
      // -------------------------------------------------------------------------------------
      auto pp_increment_epoch_condition = [&](uint64_t tmpPages, uint64_t evicted) {
         return ((pages + (freeBFLimit * 0.1)) < (tmpPages + evicted));
      };
      // -------------------------------------------------------------------------------------
      std::vector<std::pair<uint64_t, BufferFrame*>> orderedSample(SAMPLES);
      std::cout << "Buffer frames " << number_bfs << std::endl;
      auto& bfs = scalestore.getBuffermanager().getBufferframes();
      AsyncWriteBuffer async_write_buffer(scalestore.getSSDFD(), PAGE_SIZE, 1024);
      SmallMap<PID, uint64_t> io_map(8196, 2000);
      // -------------------------------------------------------------------------------------
      constexpr uint64_t BATCH_SIZE = 1024;
      const uint64_t htSize = Helper::nextPowerTwo(bufferSize);
      std::vector<storage::BufferFrame*> batch(BATCH_SIZE);
      std::vector<std::pair<uint64_t, BufferFrame*>> candidate_batch(BATCH_SIZE);
      std::vector<uint64_t> sample_epochs(BATCH_SIZE);
      uint64_t current_batch = 0;
      bool epoch_changed = true;
      bool adjust_linear_model = true;
      uint64_t private_pages = 0;
      uint64_t active_pp = 0;
      // -------------------------------------------------------------------------------------
      eviction_window = scalestore.getBuffermanager().globalEpoch;
      // -------------------------------------------------------------------------------------
      auto poll_write_buffer = [&]() {
         const uint64_t polled_events = async_write_buffer.pollEventsSync();
         if (polled_events > 0) {
            async_write_buffer.getWrittenBfs(
               [&](BufferFrame& frame, uint64_t /*epoch*/) {
                   // todo get pid because it could have changed with io_map
                   if (FLAGS_io_map) {
                      auto version = io_map.find(frame.pid)->second;
                      if (frame.latch.optimisticUpgradeToExclusive(version)) {
                         frame.dirty = false;
                         frame.latch.unlatchExclusive();
                         // free writes and track stats for epoch increment
                         private_pages++;
                         // pages++;
                         freed_pages++;
                         evict_counter++;
                      }
                      io_map.erase(frame.pid);
                   } else {
                      ensure(frame.dirty);
                      frame.dirty = false;
                      // free writes and track stats for epoch increment
                      // pages++;
                      private_pages++;
                      freed_pages++;
                      evict_counter++;
                      frame.latch.unlatchShared();
                   }
                   // coolingBuffer.add(&frame);
                   w_pages++;
                   total_written_pages++;
                },
                polled_events);
         }
      };

      auto start = utils::getTimePoint();
      while (!pp_stopped) {
         if (FLAGS_write_to_SSD) { poll_write_buffer(); }
         ensure(pages >= 0);
         if (pp_increment_epoch_condition(old_page_count,
                                          evict_counter)) {  // condition for epoch increase
            evict_counter = 0;
            scalestore.getBuffermanager().globalEpoch++;
            old_page_count = pages;
            epoch_changed = true;
            if (FLAGS_linear_model) { eviction_window++; }
         }

         if (pages.load() < freeBFLimit) {
            auto start_active = utils::getTimePoint();
            pp_rounds_active++;
            // start eviction
            // how long are we in that phase ? time base or page based?
            [[maybe_unused]] uint64_t samples = 0;
            [[maybe_unused]] uint64_t picks = 0;
            // uint64_t deviationFactor = 0;
            // uint64_t threshold = 0;
            // fill vector with ptrs
            if (FLAGS_new_code) {
               uint64_t begin = current_batch % htSize;
               uint64_t end = current_batch + BATCH_SIZE;
               if (end > htSize) end = htSize;
               current_batch = end % htSize;
               ensure(end - begin <= BATCH_SIZE);

               uint64_t entries = end - begin;
               // fill vector
               uint64_t idx = 0;
               // -------------------------------------------------------------------------------------
               for (uint64_t pos_bf = begin; pos_bf < end; pos_bf++) {
                  batch[idx++] = &bfs[pos_bf];
               }
               // sample here based on candidate batch

               if (FLAGS_linear_model) {
                  if (adjust_linear_model) {
                     uint64_t samples = 0;
                     for (uint64_t b_i = 0; b_i < entries; b_i++) {
                        auto& frame = *batch[b_i];
                        if ((frame.state == BF_STATE::FREE) | (frame.state == BF_STATE::EVICTED)) { continue; }
                        sample_epochs[samples] = frame.epoch;
                        samples++;
                     }
                     auto it_end = sample_epochs.begin() + samples;
                     std::sort(sample_epochs.begin(), it_end);
                     uint64_t eviction_idx = samples * (FLAGS_simulated_eviction_window / 100.0);
                     eviction_window = sample_epochs[eviction_idx];
                     adjust_linear_model = false;
                  }
               } else {
                  if (epoch_changed) {
                     uint64_t samples = 0;
                     for (uint64_t b_i = 0; b_i < entries; b_i++) {
                        auto& frame = *batch[b_i];
                        if ((frame.state == BF_STATE::FREE) | (frame.state == BF_STATE::EVICTED)) { continue; }
                        sample_epochs[samples] = frame.epoch;
                        samples++;
                     }
                     auto it_end = sample_epochs.begin() + samples;
                     std::sort(sample_epochs.begin(), it_end);
                     uint64_t eviction_idx = samples * (FLAGS_simulated_eviction_window / 100.0);
                     eviction_window = sample_epochs[eviction_idx];
                     epoch_changed = false;
                  }
               }

               // traverse the hash table
               uint64_t write_idx = 0;
               uint64_t candidate_max = 0;
               uint64_t sampled_round = 0;
               uint64_t found_candidates_round = 0;
               while (entries > 0) {
                  // traverse current level
                  for (uint64_t b_i = 0; b_i < entries; b_i++) {
                     auto* tmp_ptr = batch[b_i];
                     auto& frame = *tmp_ptr;

                     if ((frame.state == BF_STATE::FREE) | (frame.state == BF_STATE::EVICTED)) {
                        hit_empty_bf++;
                        continue;
                     }
                     if (frame.pid != EMPTY_PID && !frame.latch.isLatched()) {
                        // if (utils::RandomGenerator::getRandU64(0, 100) <= FLAGS_simulated_eviction_window) {
                        sampled++;
                        sampled_round++;
                        if (frame.epoch <= eviction_window) {
                           candidate_batch[candidate_max++] = {frame.epoch, tmp_ptr};
                           found_candidates++;
                           found_candidates_round++;
                        }
                     } else {
                        hit_empty_bf++;
                     }
                     // XXX optimistic latching? otherwise pointer might be nulltpr
                     if (frame.next) batch[write_idx++] = frame.next;
                  }
                  
                  // iterate over candidates
                  for (uint64_t c_i = 0; c_i < candidate_max; c_i++) {
                     // for (uint64_t o_s = 0; o_s < (samples * 0.1); o_s++) {
                     auto& frame = *candidate_batch[c_i].second;
                     auto& epoch = candidate_batch[c_i].first;

                     auto version = frame.latch.optimisticLatchOrRestart();
                     if (!version.has_value()) continue;

                     if (epoch != frame.epoch.load()) { continue; }
                     if ((frame.pid == EMPTY_PID) || (frame.state == BF_STATE::FREE) || (frame.state == BF_STATE::EVICTED)) { continue; }
                     if (!frame.latch.optimisticUpgradeToExclusive(version.value())) {
                        failed_to_upgrade++;
                        continue;
                     }

                     ensure(frame.state != BF_STATE::FREE);
                     ensure(frame.state != BF_STATE::EVICTED);

                     if ((frame.pid.getOwner() == scalestore.getNodeID())) {
                        // -------------------------------------------------------------------------------------
                        // no interfeering writes
                        if (FLAGS_test_max_read_perf) {
                           frame.latch.unlatchExclusive();
                           private_pages++;
                           // pages++;
                           continue;
                        }
                        // Dirty add to async write buffer
                        // simulate read ratio because all pages are dirty
                        if (frame.dirty) {
                           if (FLAGS_io_map && io_map.find(frame.pid) != io_map.end()) {  // prevent double writes
                              frame.latch.unlatchExclusive();
                              continue;  // pick next sample
                           }

                           if (async_write_buffer.full()) {
                              ensure(frame.latch.isLatched());
                              frame.latch.unlatchExclusive();
                              // poll here
                              std::cout << "write buffer full"
                                        << "\n";
                              break;
                           }
                           if (frame.pid == 0) {
                              ensure(frame.latch.isLatched());
                              frame.latch.unlatchExclusive();
                              continue;
                           }
                           if (FLAGS_io_map) {
                              io_map.insert({frame.pid, frame.latch.version + 0b10});
                              frame.latch.unlatchExclusive();
                           } else {
                              if (!frame.latch.tryDowngradeExclusiveToShared()) {
                                 hit_empty_bf++;
                                 continue;
                              }
                              ensure(!frame.latch.isLatched());
                           }

                           if (!FLAGS_write_to_SSD) {
                              frame.dirty = false;
                              private_pages++;
                              // pages++;
                              freed_pages++;
                              evict_counter++;
                              frame.latch.unlatchShared();
                              continue;
                           }
                           async_write_buffer.add(frame, frame.pid, epoch);
                           continue;
                        }
                        ensure(!frame.dirty);
                        frame.latch.unlatchExclusive();
                        private_pages++;
                        // pages++;
                        freed_pages++;
                        evict_counter++;
                     }
                  }
                  if (FLAGS_write_to_SSD) {
                     [[maybe_unused]] auto nsubmit = async_write_buffer.submit();
                     poll_write_buffer();
                  }

                  // -------------------------------------------------------------------------------------
                  // next iteration
                  entries = write_idx;
                  write_idx = 0;
                  candidate_max = 0;
               }

               pages += private_pages;
               private_pages = 0;
               
               if (FLAGS_linear_model) {
                  double percentage = (found_candidates_round / (double)sampled_round);
                  double target_percentage = (FLAGS_simulated_eviction_window / 100.0);
                  if (percentage > target_percentage * 2 || percentage < target_percentage / 2) {
                     adjust_linear_model = true;
                     adjusted_model++;
                  }
               }

            } else {
               // old code
               while (samples < SAMPLES) {
                  uint64_t found = 0;
                  uint64_t tried = 0;

                  // uint64_t pickIdx = utils::RandomGenerator::getRandU64(0, htSize);
                  uint64_t pickIdx = utils::RandomGenerator::getRandU64(0, number_bfs - 64);
                  // SSD code
                  for (uint64_t p_i = 0; p_i < 64; ++p_i) {
                     // -------------------------------------------------------------------------------------
                     if (samples == SAMPLES) break;  // break loop if we have enough samples
                     // -------------------------------------------------------------------------------------
                     auto& frame = bfs[pickIdx + p_i];
                     if ((frame.state == BF_STATE::FREE) | (frame.state == BF_STATE::EVICTED)) {
                        hit_empty_bf++;
                        continue;
                     }

                     if (frame.pid != EMPTY_PID && !frame.latch.isLatched()) {
                        if (utils::RandomGenerator::getRandU64(0, 100) <= FLAGS_simulated_eviction_window) {
                           orderedSample[samples].first = frame.epoch;
                           orderedSample[samples].second = &frame;
                           ensure(orderedSample[samples].second != nullptr);
                           samples++;
                           found++;
                        }
                        tried++;
                     } else {
                        hit_empty_bf++;
                     }
                  }
                  picks++;
               }

               if (samples == 0) goto escape;
               for (uint64_t o_s = 0; o_s < (samples); o_s++) {
                  // for (uint64_t o_s = 0; o_s < (samples * 0.1); o_s++) {
                  auto& frame = *orderedSample[o_s].second;
                  auto& epoch = orderedSample[o_s].first;

                  auto version = frame.latch.optimisticLatchOrRestart();
                  if (!version.has_value()) continue;

                  if (epoch != frame.epoch.load()) { continue; }
                  if ((frame.pid == EMPTY_PID) || (frame.state == BF_STATE::FREE) || (frame.state == BF_STATE::EVICTED)) { continue; }
                  if (!frame.latch.optimisticUpgradeToExclusive(version.value())) {
                     hit_empty_bf++;
                     continue;
                  }

                  ensure(frame.state != BF_STATE::FREE);
                  ensure(frame.state != BF_STATE::EVICTED);

                  if ((frame.pid.getOwner() == scalestore.getNodeID())) {
                     // -------------------------------------------------------------------------------------
                     // no interfeering writes
                     if (FLAGS_test_max_read_perf) {
                        frame.latch.unlatchExclusive();
                        pages++;
                        continue;
                     }
                     // Dirty add to async write buffer
                     // simulate read ratio because all pages are dirty
                     if (frame.dirty) {
                        if (FLAGS_io_map && io_map.find(frame.pid) != io_map.end()) {  // prevent double writes
                           frame.latch.unlatchExclusive();
                           continue;  // pick next sample
                        }

                        if (async_write_buffer.full()) {
                           ensure(frame.latch.isLatched());
                           frame.latch.unlatchExclusive();
                           goto escape;
                        }
                        if (frame.pid == 0) {
                           ensure(frame.latch.isLatched());
                           frame.latch.unlatchExclusive();
                           continue;
                        }
                        if (FLAGS_io_map) {
                           io_map.insert({frame.pid, frame.latch.version + 0b10});
                           frame.latch.unlatchExclusive();
                        } else {
                           if (!frame.latch.tryDowngradeExclusiveToShared()) continue;
                           ensure(!frame.latch.isLatched());
                        }

                        if (!FLAGS_write_to_SSD) {
                           pages++;
                           freed_pages++;
                           evict_counter++;
                           frame.latch.unlatchShared();
                           continue;
                        }
                        async_write_buffer.add(frame, frame.pid, epoch);
                        continue;
                     }
                     ensure(!frame.dirty);
                     frame.latch.unlatchExclusive();
                     pages++;
                     freed_pages++;
                     evict_counter++;
                  }
               }
            escape:
               filled++;
               if (!FLAGS_write_to_SSD) { [[maybe_unused]] auto nsubmit = async_write_buffer.submit(); }
            }
            auto end_active = utils::getTimePoint();
            active_pp += end_active -start_active;
         }
      }
      auto end = utils::getTimePoint();
      std::cout << "PP was running " << (end-start)  << "\n";
      std::cout << "PP was busy " <<  active_pp  << "\n";
      std::cout << "PP was idle " << (end-start)-active_pp  << "\n";

   });
   // -------------------------------------------------------------------------------------
   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(17, &cpuset);
   if (pthread_setaffinity_np(pp.native_handle(), sizeof(cpu_set_t), &cpuset) != 0) {
      throw std::runtime_error("Could not pin thread " + std::to_string(17) + " to thread " + std::to_string(17));
   }
   // -------------------------------------------------------------------------------------

   // -------------------------------------------------------------------------------------
   // sample buffer frame single threaded
   OLAP_workloadInfo builtInfo{pages,   freed_pages,      w_pages,          hit_empty_bf, failed_to_upgrade, eviction_window,
                               sampled, found_candidates, pp_rounds_active, spinlock,     adjusted_model};
   scalestore.startProfiler(builtInfo);
   for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
      scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
         const uint64_t blockSize = pids.size() / FLAGS_worker;
         auto begin = t_i * blockSize;
         auto end = begin + blockSize;
         if (t_i == FLAGS_worker - 1) end = pids.size();

         while (pages.load() < (freeBFLimit * 0.9)) {}  // spin to reach steady state
         while (!stopped) {
            auto idx = utils::RandomGenerator::getRandU64(begin, end);
            if (utils::RandomGenerator::getRandU64(0, 100) <= FLAGS_simulated_write_ratio) {
               scalestore::storage::ExclusiveBFGuard xguard(pids[idx]);
               xguard.getFrame().page->data[0]++;
               if (utils::RandomGenerator::getRandU64(0, FLAGS_buffer_to_ssd_ratio) != 0) {
                  auto start = utils::getTimePoint();
                  if (!FLAGS_test_max_read_perf) {
                     while (pages <= 100) {
                        spinlock++;
                        _mm_pause();
                     }
                     pages--;
                  }
                  if (FLAGS_write_to_SSD) {
                     scalestore.getBuffermanager().readPageSync(xguard.getFrame().pid, reinterpret_cast<uint8_t*>(xguard.getFrame().page));
                  }else{
                     for(uint64_t i=0; i< 600; i++){
                         _mm_pause();
                     }
                  }
                  total_read_pages++;
                  ensure(xguard.getFrame().dirty);
                  auto end = utils::getTimePoint();
                  threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
               }
            } else {
               scalestore::storage::SharedBFGuard xguard(pids[idx]);
               xguard.getFrame().page->data[0]++;
               if (utils::RandomGenerator::getRandU64(0, FLAGS_buffer_to_ssd_ratio) != 0) {
                  auto start = utils::getTimePoint();
                  if (!FLAGS_test_max_read_perf) {
                     while (pages <= 100) {
                        spinlock++;
                        _mm_pause();
                        
                     }
                     pages--;
                  }
                  if (FLAGS_write_to_SSD) {
                     scalestore.getBuffermanager().readPageSync(xguard.getFrame().pid, reinterpret_cast<uint8_t*>(xguard.getFrame().page));
                  }else{
                     for(uint64_t i=0; i< 600; i++){
                        _mm_pause();
                     }
                  }
                  total_read_pages++;
                  auto end = utils::getTimePoint();
                  threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
               }
            }
            threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
         }
      });
   }
   sleep(FLAGS_run_for_seconds);
   stopped = true;
   scalestore.getWorkerPool().joinAll();
   scalestore.stopProfiler();
   pp_stopped = true;
   pp.join();
   std::cout << "found samples " << count << std::endl;
   std::cout << "free pages " << pages << std::endl;
   std::cout << "total written pages " << total_written_pages << std::endl;
   std::cout << "total read pages " << total_read_pages << std::endl;
   std::cout << "hit spinlock " << spinlock << std::endl;
   std::cout << filled << std::endl;
   return 0;
};
