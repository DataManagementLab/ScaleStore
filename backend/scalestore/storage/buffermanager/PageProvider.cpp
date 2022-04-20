#include "PageProvider.hpp"
#include "scalestore/storage/datastructures/RingBuffer.hpp"
#include "scalestore/threads/CoreManager.hpp"
#include "scalestore/threads/Worker.hpp"
#include "scalestore/utils/Time.hpp"

// -------------------------------------------------------------------------------------
#include <fstream>

namespace scalestore {
namespace storage {
PageProvider::PageProvider(CM<rdma::InitMessage>& cm,
                           storage::Buffermanager& bm,
                           std::vector<MessageHandler::MailboxPartition>& mhPartitions,
                           s32 ssd_fd)
    : cm(cm),
      bm(bm),
      mhPartitions(mhPartitions),
      freeBFLimit(std::ceil((FLAGS_freePercentage * 1.0 * bm.dramPoolNumberPages / 100.0))),
      coolingBFLimit(std::ceil((FLAGS_coolingPercentage * 1.0 * bm.dramPoolNumberPages / 100.0))),
      ssd_fd(ssd_fd) {

   ensure(FLAGS_evictCoolestEpochs < 1); // must be less than one 
   size_t n = bm.pTable.size;  // size of the hash table 
   if (n > 0) {
      const uint64_t blockSize = n / FLAGS_pageProviderThreads;
      ensure(blockSize > 0);
      for (uint64_t t_i = 0; t_i < FLAGS_pageProviderThreads; t_i++) {
         auto begin = t_i * blockSize;
         auto end = begin + blockSize;
         if (t_i == FLAGS_pageProviderThreads - 1) end = n;
         // parititon mailboxes
         uint8_t* mb = (uint8_t*)cm.getGlobalBuffer().allocate(FLAGS_nodes, CACHE_LINE);  // CL aligned
         EvictionRequest* incoming =
             (EvictionRequest*)cm.getGlobalBuffer().allocate(sizeof(EvictionRequest) * FLAGS_nodes, CACHE_LINE);  // CL aligned
         ensure(((uintptr_t)mb) % CACHE_LINE == 0);
         // -------------------------------------------------------------------------------------
         uint8_t* rmb = (uint8_t*)cm.getGlobalBuffer().allocate(FLAGS_nodes, CACHE_LINE);  // CL aligned
         EvictionResponse* rincoming =
             (EvictionResponse*)cm.getGlobalBuffer().allocate(sizeof(EvictionResponse) * FLAGS_nodes, CACHE_LINE);  // CL aligned
         ensure(((uintptr_t)mb) % CACHE_LINE == 0);
         partitions.emplace_back(mb, incoming, rmb, rincoming, FLAGS_nodes, begin, end);
      }
      std::cout << partitions.size() << "\n";
      ensure(partitions.size() == FLAGS_pageProviderThreads);
   }
   // -------------------------------------------------------------------------------------
   startThread();
   // -------------------------------------------------------------------------------------
   init();
   finishedInit = true;
   // -------------------------------------------------------------------------------------
   while (threadCount != FLAGS_pageProviderThreads)
      ;
}
// -------------------------------------------------------------------------------------
PageProvider::~PageProvider() {
   stopThread();
}
// -------------------------------------------------------------------------------------

void PageProvider::startThread() {
   // -------------------------------------------------------------------------------------
   // helper lambdas
   // -------------------------------------------------------------------------------------
   auto connect = [&](uint64_t t_i) {
      // First initiate connection
      ensure(t_i < FLAGS_pageProviderThreads);

      for (uint64_t n_i = 0; n_i < FLAGS_nodes; n_i++) {
         // -------------------------------------------------------------------------------------
         if (n_i <= bm.nodeId)  // always connect to higher nodes but never to lower ids
            continue;
         // -------------------------------------------------------------------------------------
         auto& ip = NODES[FLAGS_nodes][n_i];
         ensure(partitions.size() > t_i);
         partitions[t_i].cctxs[n_i].rctx = &(cm.initiateConnection(ip, Type::PAGE_PROVIDER, t_i, bm.nodeId));
         partitions[t_i].cctxs[n_i].outgoing.current =
             (EvictionRequest*)cm.getGlobalBuffer().allocate(sizeof(EvictionRequest), CACHE_LINE);  // CL aligned a
         partitions[t_i].cctxs[n_i].outgoing.prev =
             (EvictionRequest*)cm.getGlobalBuffer().allocate(sizeof(EvictionRequest), CACHE_LINE);  // CL aligned a

         partitions[t_i].cctxs[n_i].outgoingResp.current =
             (EvictionResponse*)cm.getGlobalBuffer().allocate(sizeof(EvictionResponse), CACHE_LINE);  // CL aligned a
         partitions[t_i].cctxs[n_i].outgoingResp.prev =
             (EvictionResponse*)cm.getGlobalBuffer().allocate(sizeof(EvictionResponse), CACHE_LINE);  // CL aligned a

         partitions[t_i].cctxs[n_i].inflightFrames.current = new std::vector<BufferFrame*>();
         partitions[t_i].cctxs[n_i].inflightFrames.prev = new std::vector<BufferFrame*>();

         partitions[t_i].cctxs[n_i].inflightFrames.current->reserve(batchSize);
         partitions[t_i].cctxs[n_i].inflightFrames.prev->reserve(batchSize);
         // -------------------------------------------------------------------------------------
      }
      // Second finish connection
      rdma::InitMessage* init = (rdma::InitMessage*)cm.getGlobalBuffer().allocate(sizeof(rdma::InitMessage));
      for (uint64_t n_i = 0; n_i < FLAGS_nodes; n_i++) {
         // -------------------------------------------------------------------------------------
         if (n_i <= bm.nodeId) continue;
         // -------------------------------------------------------------------------------------
         // fill init messages
         init->mbOffset = (uintptr_t)&partitions[t_i].mailboxes[n_i];  // No MB offset
         init->plOffset = (uintptr_t)&partitions[t_i].incoming[n_i];
         init->mbResponseOffset = (uintptr_t)&partitions[t_i].respMailboxes[n_i];  // No MB offset
         init->plResponseOffset = (uintptr_t)&partitions[t_i].respIncoming[n_i];
         init->bmId = bm.nodeId;
         init->type = rdma::MESSAGE_TYPE::Init;
         // -------------------------------------------------------------------------------------
         cm.exchangeInitialMesssage(*(partitions[t_i].cctxs[n_i].rctx), init);
         // -------------------------------------------------------------------------------------
         partitions[t_i].cctxs[n_i].plOffset =
             (reinterpret_cast<rdma::InitMessage*>((partitions[t_i].cctxs[n_i].rctx->applicationData)))->plOffset;
         partitions[t_i].cctxs[n_i].mbOffset =
             (reinterpret_cast<rdma::InitMessage*>((partitions[t_i].cctxs[n_i].rctx->applicationData)))->mbOffset;
         partitions[t_i].cctxs[n_i].bmId = n_i;
         partitions[t_i].cctxs[n_i].respPlOffset =
             (reinterpret_cast<rdma::InitMessage*>((partitions[t_i].cctxs[n_i].rctx->applicationData)))->plResponseOffset;
         partitions[t_i].cctxs[n_i].respMbOffset =
             (reinterpret_cast<rdma::InitMessage*>((partitions[t_i].cctxs[n_i].rctx->applicationData)))->mbResponseOffset;
      }
   };

   // -------------------------------------------------------------------------------------
   // PP Threads
   // -------------------------------------------------------------------------------------
   using namespace std::chrono_literals;
   for (uint64_t t_i = 0; t_i < FLAGS_pageProviderThreads; t_i++) {
      pp_threads.emplace_back([&, t_i, connect]() {     // ATTENTION: connect needs to be copied other wise leaves scope
         profiling::WorkerCounters counters;  // create counters
         // -------------------------------------------------------------------------------------
         std::unique_ptr<threads::ThreadContext> threadContext = std::make_unique<threads::ThreadContext>();
         threads::ThreadContext::tlsPtr = threadContext.get();  // init tl ptr
         // ------------------------------------------------------------------------------------- 
         // -------------------------------------------------------------------------------------
         pid_t x = syscall(__NR_gettid);
         std::cout << "PageProvider has thread id: " << x << std::endl;
         // -------------------------------------------------------------------------------------
         connect(t_i);
         // -------------------------------------------------------------------------------------
         while (!finishedInit)
            ;  // block until initialized
         // -------------------------------------------------------------------------------------
         // Main Logic
         Partition& partition = partitions[t_i];
         uint8_t* mailboxes = partition.mailboxes;
         uint8_t* responses = partition.respMailboxes;

         std::atomic<size_t> totalEvictions = 0;
         std::atomic<size_t> evictedCounter = 0;
         uint64_t tmpPages = bm.getFreePages();

         // Limit at which PP starts incrementing epochs i.e. 90% full
         auto pp_triggered_condition = [&]() { return (bm.getFreePages()) < (coolingBFLimit); };
         // epoch condtion
         auto pp_increment_epoch_condition = [&](uint64_t tmpPages, uint64_t evicted) {
            return ((bm.getFreePages() + (freeBFLimit * 0.1)) < (tmpPages + evicted));
         };
         // start real eviction
         auto pp_start_eviction_condition = [&]() { return (bm.getFreePages() < freeBFLimit); };
         // -------------------------------------------------------------------------------------
         RingBuffer<BufferFrame*> coolingBuffer(1000);
         RingBuffer<Page*> privatePageBuffer(batchSize * FLAGS_nodes * 4);
         // -------------------------------------------------------------------------------------
         [[maybe_unused]] uint64_t failedAttempts = 0;
         // -------------------------------------------------------------------------------------
         uint64_t clients =
             std::accumulate(mhPartitions.begin(), mhPartitions.end(), 0, [](uint64_t a, auto& p) { return a + p.numberMailboxes; });
         std::vector<PID> inflightCRs(clients);
         // -------------------------------------------------------------------------------------
         while (!privatePageBuffer.full()) {
            privatePageBuffer.add(bm.pageFreeList.pop(threads::ThreadContext::my().page_handle));
         }
         // -------------------------------------------------------------------------------------
         // used wshen we do not evict to SSD but only to remote nodes 
         auto remote_condition = [&](BufferFrame& frame) {
            // local pages shared somewhere else
            if ((frame.pid.getOwner() == bm.nodeId) && (frame.state == BF_STATE::HOT) && (frame.possession == POSSESSION::SHARED) &&
                !frame.latch.isLatched()) {
               if ((frame.isPossessor(bm.nodeId)) && (frame.possessors.shared.count() > 1)) { return true; }
               if ((!frame.isPossessor(bm.nodeId)) && (frame.possessors.shared.count() >= 1)) { return true; }
            }
            // remote pages
            if ((frame.pid.getOwner() != bm.nodeId) && (frame.state == BF_STATE::HOT) && !frame.latch.isLatched()) return true;
            return false;
         };

         threadCount++;
         AsyncWriteBuffer async_write_buffer(ssd_fd, PAGE_SIZE, pp_batch_size);
         uint64_t current_batch_offset = 0;
         const uint64_t partition_size = partition.end - partition.begin;
         std::vector<storage::BufferFrame*> batch(pp_batch_size);
         std::vector<std::pair<uint64_t, BufferFrame*>> candidate_batch(pp_batch_size);
         std::vector<uint64_t> sample_epochs(required_samples);
         bool sampling_needed = true;
         uint64_t eviction_window = 0;
         // -------------------------------------------------------------------------------------
         auto batch_traverse_hashtable = [&](uint64_t begin_bfs, uint64_t end_bfs, std::function<bool(BufferFrame & frame)> condition,
                                             std::function<void()> post_process_level) {
            auto entries = end_bfs - begin_bfs;
            ensure(entries <= pp_batch_size);
            // -------------------------------------------------------------------------------------
            // preprocessing
            for (uint64_t idx = 0; idx < entries; idx++) {
               batch[idx] = &bm.bfs[begin_bfs + idx];  // fill vector with ptrs of hashtable from first level
            }
            // -------------------------------------------------------------------------------------
            uint64_t write_idx = 0;      // replace in-place the next level
            while (entries > 0) {
               // traverse current level
               for (uint64_t b_i = 0; b_i < entries; b_i++) {
                  auto* tmp_ptr = batch[b_i];
                  if (tmp_ptr == nullptr) continue;  // might happen due to OLC

                  auto& frame = *tmp_ptr;
                  // condition when we do sth e.g. write to output buffer
                  if(!condition(frame))
                     break;
                  if (frame.next) batch[write_idx++] = frame.next;
               }
               // todo add here
               post_process_level();
               // -------------------------------------------------------------------------------------
               entries = write_idx;
               write_idx = 0;
            }
         };

         // -------------------------------------------------------------------------------------
         auto evict_owner_page = [&](BufferFrame& frame, uint64_t epoch) -> bool {
            // -------------------------------------------------------------------------------------

            if (FLAGS_evict_to_ssd) {
               // Dirty add to async write buffer
               if (frame.dirty) {
                  if (async_write_buffer.full()) {
                     ensure(frame.latch.isLatched());
                     frame.latch.unlatchExclusive();
                     return false;
                  }
                  if (!frame.latch.tryDowngradeExclusiveToShared()) return true;
                  ensure(!frame.latch.isLatched());
                  async_write_buffer.add(frame, frame.pid, epoch);
                  return true;
               }
               // -------------------------------------------------------------------------------------
               // Not dirty; But we own the page X therefore the PP evicted this page to clean the dirty flag
               if ((frame.state == BF_STATE::HOT) && (frame.possession == POSSESSION::EXCLUSIVE) && ((frame.isPossessor(bm.nodeId)))) {
                  bm.removeFrame(frame, [&](BufferFrame& frame) {
                     if (!privatePageBuffer.full()) {
                        privatePageBuffer.add(frame.page);
                     } else {
                        bm.pageFreeList.push(frame.page, threads::ThreadContext::my().page_handle);
                     }
                  });
                  // -------------------------------------------------------------------------------------
                  totalEvictions++;
                  evictedCounter++;
                  return true;
               }
            }

            // -------------------------------------------------------------------------------------
            // Not Dirty; We own the page in shared mode and nobody else thus we can evict the page to SSD
            if ((frame.state == BF_STATE::HOT) && (frame.possession == POSSESSION::SHARED) &&
                ((frame.isPossessor(bm.nodeId)) && (frame.possessors.shared.count() == 1))) {
               auto rand_evict = utils::RandomGenerator::getRandU64(0, 1000);  // second chance like
               if (FLAGS_evict_to_ssd && rand_evict <= FLAGS_prob_SSD) {
                  bm.removeFrame(frame, [&](BufferFrame& frame) {
                     if (!privatePageBuffer.full()) {
                        privatePageBuffer.add(frame.page);
                     } else {
                        bm.pageFreeList.push(frame.page, threads::ThreadContext::my().page_handle);
                     }
                  });

                  totalEvictions++;
                  evictedCounter++;
               } else {
                  frame.latch.unlatchExclusive();
               }
               return true;
               ;
            }

            // -------------------------------------------------------------------------------------
            // Not Dirty; but some other node also has a copy thus we evict the page but keep the frame
            // -------------------------------------------------------------------------------------
            if ((frame.state == BF_STATE::HOT) && (frame.possession == POSSESSION::SHARED)) {
               // cannot evicti page as we are to only possessor
               ensure(frame.possessors.shared.count() > 0);
               if ((frame.isPossessor(bm.nodeId)) && (frame.possessors.shared.count() == 1)) {
                  throw std::runtime_error("should not be reached remove");
               }
               ensure(frame.latch.isLatched());
               if (!privatePageBuffer.full()) {
                  privatePageBuffer.add(frame.page);
               } else {
                  bm.pageFreeList.push(frame.page, threads::ThreadContext::my().page_handle);
               }
               ensure(frame.state == BF_STATE::HOT);
               // adapt state
               frame.possessors.shared.reset(bm.nodeId);
               ensure(frame.possessors.shared.count() >= 1);
               frame.page = nullptr;
               frame.state = BF_STATE::EVICTED;
               ensure(frame.possessors.shared.count() >= 1);
               // -------------------------------------------------------------------------------------
               totalEvictions++;
               evictedCounter++;
            }
            ensure(frame.latch.isLatched());
            frame.latch.unlatchExclusive();
            return true;
         };

         auto poll_write_buffer = [&]() {
            const uint64_t polled_events = async_write_buffer.pollEventsSync();
            if (polled_events > 0) {
               async_write_buffer.getWrittenBfs(
                  [&](BufferFrame& frame, uint64_t epoch_added) {
                      // remove dirty flag
                      ensure(frame.dirty);
                      frame.dirty = false;
                      auto version = frame.latch.version.load();
                      frame.latch.unlatchShared();
                      counters.incr(profiling::WorkerCounters::ssd_pages_written);

                      if (epoch_added != frame.epoch.load()) { return; }
                      if ((frame.pid == EMPTY_PID) || (frame.state == BF_STATE::FREE) || (frame.state == BF_STATE::EVICTED)) { return; }
                      if (!frame.latch.optimisticUpgradeToExclusive(version)) { return;}
                      ensure(frame.state != BF_STATE::FREE);
                      ensure(frame.state != BF_STATE::EVICTED);
                      ensure(frame.pid.getOwner() == bm.nodeId);
                      // -------------------------------------------------------------------------------------
                      auto rc = evict_owner_page(frame, epoch_added);
                      ensure(rc);

                   },
                   polled_events);
            }
         };

         // -------------------------------------------------------------------------------------
         // PageProvider Main Loop - consists of 6 Phases
         // -------------------------------------------------------------------------------------
         while (threadsRunning) {
            counters.incr(profiling::WorkerCounters::pp_rounds);
            // -------------------------------------------------------------------------------------
            // PHASE 1: Incoming Requests
            // -------------------------------------------------------------------------------------
            auto start_p1 = counters.getTimePoint_for(profiling::WorkerCounters::pp_latency_p1_incoming_requests);
            for (uint64_t m_i = 0; m_i < partition.numberMailboxes; m_i++) {
               // -------------------------------------------------------------------------------------
               // incoming requests
               if (mailboxes[m_i] == 0) continue;
               // -------------------------------------------------------------------------------------
               auto& request = partition.incoming[m_i];
               if (privatePageBuffer.getSize() < request.elements) {
                  // std::cerr << "\n";
                  // std::cerr << "private page buffer is not full enough to process remote messages " << std::endl;
                  break;  // breakts loop because we have not enough pages to retrieve remote pages.
               }
               // -------------------------------------------------------------------------------------
               mailboxes[m_i] = 0;
               partition.cctxs[m_i].responseOutstanding = true;
               requests++;
               ensure(request.bmId == m_i);
               ensure(request.pId == t_i);

               std::vector<std::pair<Guard, uint64_t>> guards_p;  // guard and offet to request entrie
               guards_p.reserve(request.elements);
               // optimization we could early abort in find frame or insert if ee.pVersion is not the same
               for (uint64_t ee_i = 0; ee_i < request.elements; ee_i++) {
                  auto& ee = request.entries[ee_i];
                  auto guard = bm.findFrame<CONTENTION_METHOD::NON_BLOCKING>(ee.pid, Exclusive(), request.bmId);
                  if (guard.state == STATE::RETRY || guard.state == STATE::UNINITIALIZED) continue;
                  ensure(guard.state != STATE::NOT_FOUND);
                  // -------------------------------------------------------------------------------------
                  ensure(guard.latchState == LATCH_STATE::EXCLUSIVE);
                  // -------------------------------------------------------------------------------------
                  if (guard.frame->pVersion != ee.pVersion) {
                     ensure(guard.frame->latch.isLatched());
                     guard.frame->latch.unlatchExclusive();
                     continue;
                  }
                  // save qualifying guards for next steps
                  guards_p.push_back({std::move(guard), ee_i});
               }
               // -------------------------------------------------------------------------------------
               // at that point we can copy the outstanding CRequests;
               // -------------------------------------------------------------------------------------
               inflightCRs.clear();
               for (auto& p : mhPartitions) {
                  std::unique_lock<std::mutex> ulquard(p.inflightCRMutex);
                  for (auto& cr : p.inflightCRs) {
                     inflightCRs.push_back(cr.pid);
                  }
               }
               // sort vector and use valid range for binary search
               // optimization sort both and do a comparision instead of binary search *N times 
               sort(inflightCRs.begin(), inflightCRs.end());
               auto endValidPids = std::find_if(inflightCRs.begin(), inflightCRs.end(), [](const PID& pid) { return pid == EMPTY_PID; });
               bool needCheckAgainstCR = (endValidPids != inflightCRs.begin());
               // -------------------------------------------------------------------------------------
               for (auto& p : guards_p) {
                  auto& guard = p.first;
                  auto& ee = request.entries[p.second];
                  ensure(guard.frame->latch.isLatched());
                  if (needCheckAgainstCR && std::binary_search(inflightCRs.begin(), endValidPids, guard.frame->pid)) {
                     ensure(guard.frame->latch.isLatched());
                     guard.frame->latch.unlatchExclusive();
                     continue;
                  }
                  // -------------------------------------------------------------------------------------
                  auto& ctx = partition.cctxs[m_i];
                  bool needRead = false;
                  if(FLAGS_evict_to_ssd){
                     if (guard.frame->dirty) {
                        needRead = true;
                        if (guard.frame->possession == POSSESSION::EXCLUSIVE) {
                           guard.frame->possessors.exclusive = bm.nodeId;
                        } else if (guard.frame->possession == POSSESSION::SHARED) {
                           // only need to read if we do not have the version or someone else?
                           guard.frame->possessors.shared.reset(request.bmId);
                           guard.frame->possessors.shared.set(bm.nodeId);
                        } else {
                           throw std::runtime_error("Invalid possession state");
                        }
                     }
                  }else{
                     // when we do not evict from SSD we just want to keep the most uptodate version
                     // therefore
                     if (guard.frame->possession == POSSESSION::SHARED) {
                        ensure(guard.frame->possessors.shared.test(request.bmId));
                        guard.frame->possessors.shared.reset(request.bmId);
                        if (guard.frame->possessors.shared.none()) {
                           needRead = true;
                           guard.frame->possessors.shared.set(bm.nodeId);
                        }
                     } else if (guard.frame->possession == POSSESSION::EXCLUSIVE) {
                        ensure(guard.frame->possessors.exclusive == request.bmId);
                        guard.frame->possessors.exclusive = bm.nodeId;
                        needRead = true;
                     } 
                  }
                  auto& response = *ctx.outgoingResp.current;
                  // -------------------------------------------------------------------------------------
                  if (!needRead) {
                     ensure(guard.frame->possession == POSSESSION::SHARED);
                     response.add(guard.frame->pid);
                     ensure(guard.frame->latch.isLatched());
                     // check if we can remove the frame
                     guard.frame->possessors.shared.reset(request.bmId);
                     if (guard.frame->possessors.shared.none()) {
                        // remove frame as we are the only one
                        ensure(guard.frame->state == BF_STATE::EVICTED);
                        ensure(guard.frame->page == nullptr);
                        bm.removeFrame(*guard.frame, [](BufferFrame& /*frame*/) {});

                        totalEvictions++;
                        evictedCounter++;
                     } else {
                        ensure(guard.frame->possessors.shared.count() > 0);
                        guard.frame->latch.unlatchExclusive();
                     }
                     continue;
                  }
                  // -------------------------------------------------------------------------------------
                  ensure(guard.frame->latch.isLatched());
                  if (guard.frame->state == BF_STATE::EVICTED) {
                     ensure(guard.frame->page == nullptr);
                     if (privatePageBuffer.empty()) {
                        throw std::runtime_error("Private page buffer empty");
                        while (!privatePageBuffer.full())
                           privatePageBuffer.add(bm.pageFreeList.pop(threads::ThreadContext::my().page_handle));
                     }
                     guard.frame->page = privatePageBuffer.remove();
                  }
                  guard.frame->state = BF_STATE::HOT;
                  guard.frame->dirty = true;
                  response.add(guard.frame->pid);
                  ctx.latchedFrames.emplace_back(guard.frame);
                  ctx.rdmaReadBuffer.add(ctx, guard.frame->page, ee.offset);
                  counters.incr(profiling::WorkerCounters::pp_rdma_received);
               }
               auto& ctx = partition.cctxs[m_i];
               // post read requests for this (m_i) remote node 
               if (ctx.rdmaReadBuffer.elements > 0) {
                  ctx.rdmaReadBuffer.postRead(ctx);
                  ctx.readScheduled = true;
                  ctx.responseLatency = utils::getTimePoint();
               }
            }
            auto end_p1 = counters.getTimePoint_for(profiling::WorkerCounters::pp_latency_p1_incoming_requests);
            counters.incr_by(profiling::WorkerCounters::pp_latency_p1_incoming_requests, end_p1 - start_p1);

            // -------------------------------------------------------------------------------------
            // SSD async get written bfs and add to too cooling buffer
            // -------------------------------------------------------------------------------------
            poll_write_buffer();
            // -------------------------------------------------------------------------------------
            // PHASE 3: Sample from hash table and evict pages 
            // -------------------------------------------------------------------------------------
            auto start_p3 = counters.getTimePoint_for(profiling::WorkerCounters::pp_latency_p3_select);
            if (pp_triggered_condition()) {
               if (pp_increment_epoch_condition(tmpPages,
                                                evictedCounter.load())) {  // condition for epoch increase
                  evictedCounter.exchange(0);
                  bm.globalEpoch++;
                  tmpPages = bm.getFreePages();
                  sampling_needed = true;
               }
               if (pp_start_eviction_condition()) {  // start eviction

                  uint64_t begin_offset = (current_batch_offset % partition_size);
                  uint64_t end_offset = (current_batch_offset + pp_batch_size);
                  if (end_offset > partition_size) end_offset = partition_size;
                  current_batch_offset = end_offset % partition_size;
                  uint64_t begin = begin_offset + partition.begin;
                  uint64_t end = end_offset + partition.begin;

                  if (FLAGS_evict_to_ssd && sampling_needed) {
                     
                     uint64_t samples = 0;
                     while(samples < required_samples){
                        uint64_t b_sampling_offset = (current_batch_offset % partition_size);
                        uint64_t e_sampling_offset = (current_batch_offset + pp_batch_size);
                        if (e_sampling_offset > partition_size) e_sampling_offset = partition_size;
                        current_batch_offset = e_sampling_offset % partition_size;
                        uint64_t begin_sampling = b_sampling_offset + partition.begin;
                        uint64_t end_sampling = e_sampling_offset + partition.begin;

                        batch_traverse_hashtable(
                           begin_sampling, end_sampling,
                           [&](BufferFrame& frame) {
                              if(samples == required_samples) return false;
                              if ((frame.state == BF_STATE::FREE) | (frame.state == BF_STATE::EVICTED)) { return true; }
                              sample_epochs[samples++] = frame.epoch;
                              return true;
                           },
                           [&](){} // empty function
                           );
                     }
                     auto it_end = sample_epochs.begin() + samples;
                     std::sort(sample_epochs.begin(), it_end);
                     uint64_t eviction_idx = samples * FLAGS_evictCoolestEpochs;
                     eviction_window = sample_epochs[eviction_idx];
                     sampling_needed = false;
                  }
                  
                  // traverse the hash table in batches
                  uint64_t candidate_max = 0;  // remember how many candidates we wrote to the candidate batch needs to be resetted per level
                  batch_traverse_hashtable(
                      begin, end,
                      [&](BufferFrame& frame) {
                         if ((frame.state == BF_STATE::FREE) | (frame.state == BF_STATE::EVICTED)) { return true; }
                         if (frame.pid != EMPTY_PID && !frame.latch.isLatched()) {
                            if(FLAGS_evict_to_ssd){
                               if (frame.epoch <= eviction_window) { candidate_batch[candidate_max++] = {frame.epoch, &frame}; }
                            }else{
                               // we only evict remote pages
                               if(remote_condition(frame)) { candidate_batch[candidate_max++] = {frame.epoch, &frame}; }
                            }
                         }
                         return true;
                      },
                      [&]() {
                         // iterate over candidates
                         for (uint64_t c_i = 0; c_i < candidate_max; c_i++) {
                            // for (uint64_t o_s = 0; o_s < (samples * 0.1); o_s++) {
                            auto& frame = *candidate_batch[c_i].second;
                            auto& epoch = candidate_batch[c_i].first;

                            auto version = frame.latch.optimisticLatchOrRestart();
                            if (!version.has_value()) continue;

                            if (epoch != frame.epoch.load()) { continue; }
                            if ((frame.pid == EMPTY_PID) || (frame.state == BF_STATE::FREE) || (frame.state == BF_STATE::EVICTED)) {
                               continue;
                            }
                            if (!frame.latch.optimisticUpgradeToExclusive(version.value())) { continue; }
                            ensure(frame.state != BF_STATE::FREE);
                            ensure(frame.state != BF_STATE::EVICTED);
                            // -------------------------------------------------------------------------------------
                            if ((frame.pid.getOwner() == bm.nodeId)) {
                               auto rc = evict_owner_page(frame, epoch);
                               if (!rc) break;  // encountered dirty page and we cannot write it to the buffer
                               continue;
                            }
                            // -------------------------------------------------------------------------------------
                            // Remote Page
                            // -------------------------------------------------------------------------------------
                            {
                               auto targetNode = frame.pid.getOwner();
                               if (partition.cctxs[targetNode].outgoing.current->full()) {
                                  ensure(frame.latch.isLatched());
                                  frame.latch.unlatchExclusive();
                                  break;
                               }
                               ensure(frame.state == BF_STATE::HOT);
                               partition.cctxs[targetNode].outgoing.current->add(
                                   {.pid = frame.pid, .offset = (uintptr_t)(frame.page), .pVersion = frame.pVersion});
                               partition.cctxs[targetNode].inflightFrames.current->emplace_back(&frame);
                            }
                         }
                         [[maybe_unused]] auto nsubmit = async_write_buffer.submit();
                         poll_write_buffer();
                         // -------------------------------------------------------------------------------------
                         // next iteration
                         candidate_max = 0;
                      });
               }
            }
            // -------------------------------------------------------------------------------------
            auto end_p3 = counters.getTimePoint_for(profiling::WorkerCounters::pp_latency_p3_select);
            counters.incr_by(profiling::WorkerCounters::pp_latency_p3_select, end_p3 - start_p3);

            while (!privatePageBuffer.full()) {  // ensure that the private page Buffer will get refilled again
               Page* p = nullptr;
               if (!bm.pageFreeList.try_pop(p, threads::ThreadContext::my().page_handle)) {
                  // std::cerr << "\n";
                  // std::cerr << "Could not fill the privatePageBuffer" << std::endl;
                  break;
               }
               privatePageBuffer.add(p);
               }
               // -------------------------------------------------------------------------------------
               // Submit to SSD
               // -------------------------------------------------------------------------------------
               [[maybe_unused]] auto nsubmit = async_write_buffer.submit();
               // ------------------------------------------------------------------------------------
               // -------------------------------------------------------------------------------------
               // PHASE 4: Send Requests
               // -------------------------------------------------------------------------------------
               // send batches to remote if possible?
               auto start_p4 = counters.getTimePoint_for(profiling::WorkerCounters::pp_latency_p4_send_requests);
               for (uint64_t n_i = 0; n_i < FLAGS_nodes; n_i++) {
                  if (n_i == bm.nodeId) continue;
                  // check if response still outstanding otherwise skip
                  if (partition.cctxs[n_i].responseExpected) continue;

                  if ((partition.cctxs[n_i].outgoing.current->elements > minOutgoingElements) ||
                      ((partition.cctxs[n_i].outgoing.current->failedSend > 100) &&
                       (partition.cctxs[n_i].outgoing.current->elements > 0))) {
                     // send away and increment active batch
                     auto& request = *partition.cctxs[n_i].outgoing.current;
                     request.bmId = bm.nodeId;
                     request.pId = t_i;
                     writeRequest(t_i, n_i, request);
                     partition.cctxs[n_i].responseExpected = true;
                     // swap outgoing responses
                     partition.cctxs[n_i].outgoing.current->failedSend = 0;  // reset failed send important here before swap
                     partition.cctxs[n_i].outgoing.swap();
                     partition.cctxs[n_i].outgoing.current->reset();
                     partition.cctxs[n_i].inflightFrames.swap();
                     partition.cctxs[n_i].requestLatency = utils::getTimePoint();
                  } else {
                     partition.cctxs[n_i].outgoing.current->failedSend++;
                  }
            }
            auto end_p4 = counters.getTimePoint_for(profiling::WorkerCounters::pp_latency_p4_send_requests);
            counters.incr_by(profiling::WorkerCounters::pp_latency_p4_send_requests, end_p4 - start_p4);
            // -------------------------------------------------------------------------------------
            // PHASE 5: Incoming Responses
            // -------------------------------------------------------------------------------------
            auto start_p5 = counters.getTimePoint_for(profiling::WorkerCounters::pp_latency_p5_incoming_responses);
            for (uint64_t m_i = 0; m_i < partition.numberMailboxes; m_i++) {
               // -------------------------------------------------------------------------------------
               if (responses[m_i] == 0) continue;
               // -------------------------------------------------------------------------------------
               responses[m_i] = 0;
               ensure(partition.cctxs[m_i].responseExpected);
               partition.cctxs[m_i].responseExpected = false;
               auto& ctx = partition.cctxs[m_i];
               auto& response = partition.respIncoming[m_i];
               uint64_t r_i = 0;
               if (response.elements > 0) {
                  for (auto* frame : *ctx.inflightFrames.prev) {
                     if (frame->pid == response.pids[r_i]) {
                        bm.removeFrame(*frame, [&](BufferFrame& frame) {
                           if (!privatePageBuffer.full()) {
                              privatePageBuffer.add(frame.page);
                           } else {
                              bm.pageFreeList.push(frame.page, threads::ThreadContext::my().page_handle);
                           }
                        });
                        ensure(r_i <= response.elements);
                        totalEvictions++;
                        evictedCounter++;
                        r_i++;
                        counters.incr(profiling::WorkerCounters::pp_rdma_evicted);
                     } else {
                        ensure(frame->latch.isLatched());
                        frame->latch.unlatchExclusive();
                     }
                  }
               } else {
                  for (auto* frame : *ctx.inflightFrames.prev) {
                     ensure(frame->latch.isLatched());
                     frame->latch.unlatchExclusive();
                  }
               }
               ctx.inflightFrames.prev->clear();
            }

            auto end_p5 = counters.getTimePoint_for(profiling::WorkerCounters::pp_latency_p5_incoming_responses);
            counters.incr_by(profiling::WorkerCounters::pp_latency_p5_incoming_responses, end_p5 - start_p5);
            // -------------------------------------------------------------------------------------
            // PHASE 6: Send Responses
            // -------------------------------------------------------------------------------------
            auto start_p6 = counters.getTimePoint_for(profiling::WorkerCounters::pp_latency_p6_send_responses);
            for (uint64_t m_i = 0; m_i < partition.numberMailboxes; m_i++) {
               if (m_i == bm.nodeId) continue;
               auto& ctx = partition.cctxs[m_i];
               // if read scheduled and completion is not yet there continue
               if (ctx.readScheduled && !ctx.rdmaReadBuffer.pollCompletion(ctx)) continue;
               auto& response = *ctx.outgoingResp.current;
               // could be an empty response
               if (!ctx.responseOutstanding) continue;
               // -------------------------------------------------------------------------------------
               ctx.responseOutstanding = false;
               ctx.readScheduled = false;
               ctx.readCompleted = false; 
               response.pId = t_i;
               response.bmId = bm.nodeId;
               writeResponse(t_i, m_i, response);
               // -------------------------------------------------------------------------------------
               for (auto f : ctx.latchedFrames) {
                  ensure(f->latch.isLatched());
                  f->latch.unlatchExclusive();
               }
               // -------------------------------------------------------------------------------------
               ctx.latchedFrames.clear();
               responses_sent++;
               // switch responses
               ctx.outgoingResp.swap();
               ctx.outgoingResp.current->reset();
            }
            auto end_p6 = counters.getTimePoint_for(profiling::WorkerCounters::pp_latency_p6_send_responses);
            counters.incr_by(profiling::WorkerCounters::pp_latency_p6_send_responses, end_p6 - start_p6);
         }
         // -------------------------------------------------------------------------------------
         threadCount--;
      });
      // pin threads
      if ((t_i % 2) == 0)
         threads::CoreManager::getInstance().pinThreadToCore(pp_threads.back().native_handle());
      else
         threads::CoreManager::getInstance().pinThreadToHT(pp_threads.back().native_handle());
   }
   // -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
void PageProvider::stopThread() {
   threadsRunning = false;
   while (threadCount)
      ;  // wait

   for(auto& t : pp_threads)
      t.join();
};
// -------------------------------------------------------------------------------------

void PageProvider::init() {
   InitMessage* init = (InitMessage*)cm.getGlobalBuffer().allocate(sizeof(InitMessage));
   // -------------------------------------------------------------------------------------
   size_t numConnections = (FLAGS_worker) * (FLAGS_nodes - 1);  // only expect lower nodeId nodes
   numConnections += (FLAGS_pageProviderThreads * (bm.nodeId)) + ((FLAGS_messageHandlerThreads) * (FLAGS_nodes - 1));
   // -------------------------------------------------------------------------------------
   std::cout << " expected connections PP int" << numConnections << "\n";
   while (cm.getNumberIncomingConnections() != (numConnections))
      ;  // block until client is connected
   // -------------------------------------------------------------------------------------
   std::vector<RdmaContext*> rdmaCtxs(cm.getIncomingConnections());  // get cm ids of incomming

   for (auto* rContext : rdmaCtxs) {
      if (rContext->type != Type::PAGE_PROVIDER) continue;
      // -------------------------------------------------------------------------------------
      auto t_i = rContext->typeId;  // partition
      auto n_i = rContext->nodeId;
      ensure(n_i < bm.nodeId);
      // -------------------------------------------------------------------------------------
      partitions[t_i].cctxs[n_i].rctx = rContext;
      partitions[t_i].cctxs[n_i].bmId = n_i;
      // -------------------------------------------------------------------------------------
      init->mbOffset = (uintptr_t)&partitions[t_i].mailboxes[n_i];  // No MB offset
      init->plOffset = (uintptr_t)&partitions[t_i].incoming[n_i];
      init->bmId = bm.nodeId;
      init->type = rdma::MESSAGE_TYPE::Init;
      init->mbResponseOffset = (uintptr_t)&partitions[t_i].respMailboxes[n_i];  // No MB offset
      init->plResponseOffset = (uintptr_t)&partitions[t_i].respIncoming[n_i];
      // -------------------------------------------------------------------------------------
      cm.exchangeInitialMesssage(*(partitions[t_i].cctxs[n_i].rctx), init);
      // -------------------------------------------------------------------------------------
      partitions[t_i].cctxs[n_i].plOffset =
          (reinterpret_cast<rdma::InitMessage*>((partitions[t_i].cctxs[n_i].rctx->applicationData)))->plOffset;
      partitions[t_i].cctxs[n_i].mbOffset =
          (reinterpret_cast<rdma::InitMessage*>((partitions[t_i].cctxs[n_i].rctx->applicationData)))->mbOffset;
      partitions[t_i].cctxs[n_i].respPlOffset =
          (reinterpret_cast<rdma::InitMessage*>((partitions[t_i].cctxs[n_i].rctx->applicationData)))->plResponseOffset;
      partitions[t_i].cctxs[n_i].respMbOffset =
          (reinterpret_cast<rdma::InitMessage*>((partitions[t_i].cctxs[n_i].rctx->applicationData)))->mbResponseOffset;
      partitions[t_i].cctxs[n_i].outgoing.current =
          (EvictionRequest*)cm.getGlobalBuffer().allocate(sizeof(EvictionRequest), CACHE_LINE);  // CL aligned a
      partitions[t_i].cctxs[n_i].outgoing.prev =
          (EvictionRequest*)cm.getGlobalBuffer().allocate(sizeof(EvictionRequest), CACHE_LINE);  // CL aligned a

      partitions[t_i].cctxs[n_i].outgoingResp.current =
          (EvictionResponse*)cm.getGlobalBuffer().allocate(sizeof(EvictionResponse), CACHE_LINE);  // CL aligned a
      partitions[t_i].cctxs[n_i].outgoingResp.prev =
          (EvictionResponse*)cm.getGlobalBuffer().allocate(sizeof(EvictionResponse), CACHE_LINE);  // CL aligned a

      partitions[t_i].cctxs[n_i].inflightFrames.current = new std::vector<BufferFrame*>();
      partitions[t_i].cctxs[n_i].inflightFrames.prev = new std::vector<BufferFrame*>();
      partitions[t_i].cctxs[n_i].inflightFrames.current->reserve(batchSize);
      partitions[t_i].cctxs[n_i].inflightFrames.prev->reserve(batchSize);
   }
   // -------------------------------------------------------------------------------------
}

// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace scalestore
