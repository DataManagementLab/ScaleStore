#pragma once
// -------------------------------------------------------------------------------------
#include "CommunicationManager.hpp"
#include "scalestore/storage/buffermanager/Buffermanager.hpp"
#include "scalestore/storage/buffermanager/AsyncReadBuffer.hpp"
// -------------------------------------------------------------------------------------
#include <bitset>
#include <iostream>
#include <arpa/inet.h>
#include <libaio.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace scalestore {
namespace rdma {
using namespace storage;

// -------------------------------------------------------------------------------------
struct MessageHandler {
   // -------------------------------------------------------------------------------------
   struct InflightCopyRequest {
      bool inflight = false;
      PID pid = EMPTY_PID;
      uint64_t pVersion = 0;
   };
   // -------------------------------------------------------------------------------------

   struct InvalidationBatch {
      std::vector<storage::Page*> batch;
      size_t elements{0};
      InvalidationBatch() : batch(FLAGS_pollingInterval + 1, nullptr){};
      bool empty() { return (elements == 0); }
      void add(Page* page)
      {
         ensure(elements <= FLAGS_pollingInterval);
         batch[elements] = page;
         elements++;
      }
      void reset() { elements = 0; }
   };

   struct MHEndpoint {
      rdma::RdmaContext* rctx = nullptr;
      uint64_t wqe = 0;
   };
   // -------------------------------------------------------------------------------------
   struct alignas(CACHE_LINE) ConnectionContext {
      uintptr_t plOffset {0};       // does not have mailbox just payload with flag
      rdma::Message* request {nullptr};   // rdma pointer
      rdma::Message* response {nullptr};  // in current protocol only one message can be outstanding per client
      rdma::RdmaContext* rctx {nullptr};
      uint64_t wqe {0};  // wqe currently outstanding
      NodeID bmId {0};   // id to which incoming client connection belongs
      // 2 invalidation lists
      InvalidationBatch* activeInvalidationBatch = nullptr;   // active is the list in which we park deletions
      InvalidationBatch* passiveInvalidationBatch = nullptr;  // passive will be deleted with next signaled completion
      uint64_t retries = 0;
      // remote mailboxes for each remote MH in order to allow MH to delegate requests
      std::vector<uintptr_t> remoteMbOffsets;
      std::vector<uintptr_t> remotePlOffsets;
   };
   // -------------------------------------------------------------------------------------
   // Mailbox partition per thread
   struct MailboxPartition {
      MailboxPartition() = default;
      MailboxPartition(uint8_t* mailboxes, uint64_t numberMailboxes, uint64_t beginId)
          : mailboxes(mailboxes), numberMailboxes(numberMailboxes), beginId(beginId), inflightCRs(numberMailboxes){};
      uint8_t* mailboxes = nullptr;
      uint64_t numberMailboxes;
      uint64_t beginId;
      std::vector<InflightCopyRequest> inflightCRs;
      std::mutex inflightCRMutex;
   };
   // -------------------------------------------------------------------------------------
   MessageHandler(rdma::CM<InitMessage>& cm, storage::Buffermanager& bm, NodeID nodeId);
   ~MessageHandler();
   // -------------------------------------------------------------------------------------
   void startThread();
   void stopThread();
   void init();
   // -------------------------------------------------------------------------------------
   std::atomic<bool> threadsRunning = true;
   std::atomic<size_t> threadCount = 0;
   rdma::CM<InitMessage>& cm;
   storage::Buffermanager& bm;
   // -------------------------------------------------------------------------------------
   NodeID nodeId;
   std::vector<ConnectionContext> cctxs;
   std::vector<MailboxPartition> mbPartitions;
   std::atomic<uint64_t> connectedClients = 0;
   std::atomic<bool> finishedInit = false;
   // -------------------------------------------------------------------------------------   
   
   // -------------------------------------------------------------------------------------
   // Invalidation Logic
   // -------------------------------------------------------------------------------------
   void reclaimInvalidations(ConnectionContext& ctx, storage::PartitionedQueue<storage::Page*, PARTITIONS, BATCH_SIZE, utils::Stack>::BatchHandle& page_handle)
   {
      if (ctx.passiveInvalidationBatch->empty())
         return;  // nothing to do
      for (uint64_t p_i = 0; p_i < ctx.passiveInvalidationBatch->elements; ++p_i) {
         bm.pageFreeList.push(ctx.passiveInvalidationBatch->batch[p_i],page_handle);
      }
      ctx.passiveInvalidationBatch->reset();
   }
   // -------------------------------------------------------------------------------------
   // Handle methods
   // -------------------------------------------------------------------------------------
   template <POSSESSION DESIRED_MODE>
   void handlePossessionRequest(
       MailboxPartition& partition,
       PossessionRequest& request,
       ConnectionContext& ctx,
       uint64_t clientId,
       uint64_t m_i,
       profiling::WorkerCounters& counters,
       storage::AsyncReadBuffer& async_read_buffer,
       storage::PartitionedQueue<storage::Page*, PARTITIONS, BATCH_SIZE, utils::Stack>::BatchHandle& page_handle)
   {
      uint8_t* mailboxes = partition.mailboxes;
      auto guard = bm.findFrameOrInsert<CONTENTION_METHOD::NON_BLOCKING>(request.pid, Protocol<DESIRED_MODE>(), ctx.bmId);
      // -------------------------------------------------------------------------------------
      if (guard.state == STATE::RETRY) {
         ensure(guard.latchState != LATCH_STATE::EXCLUSIVE);
         mailboxes[m_i] = 1;
         counters.incr(profiling::WorkerCounters::mh_msgs_restarted);
         return;
      }

      if(guard.state == STATE::SSD){
         // -------------------------------------------------------------------------------------
         ensure(guard.frame->latch.isLatched());
         ensure(guard.frame->page != nullptr);
         if(async_read_buffer.full()){
               throw std::runtime_error("read buffer is full ");
            }
         
         guard.frame->state = BF_STATE::HOT;
         guard.frame->epoch = bm.globalEpoch.load();
         guard.frame->possession = POSSESSION::SHARED;
         guard.frame->setPossessor(nodeId);
         guard.frame->dirty = false;
         async_read_buffer.add(*guard.frame, guard.frame->pid, m_i, true);
         counters.incr(profiling::WorkerCounters::mh_msgs_restarted);
         return;         
      }
         
      if (guard.frame->state == BF_STATE::INVALIDATION_EXPECTED) {
         guard.frame->latch.unlatchExclusive();
         mailboxes[m_i] = 1;
         counters.incr(profiling::WorkerCounters::mh_msgs_restarted);
         return;
      }
      // -------------------------------------------------------------------------------------
      ensure((guard.frame->state == BF_STATE::HOT) || (guard.frame->state == BF_STATE::EVICTED));
      ensure(guard.frame->latch.isLatched());
      ensure(guard.latchState == LATCH_STATE::EXCLUSIVE);
      // -------------------------------------------------------------------------------------
      auto& response = *MessageFabric::createMessage<rdma::PossessionResponse>(ctx.response, RESULT::WithPage);
      response.type = (DESIRED_MODE == POSSESSION::SHARED ? MESSAGE_TYPE::PRRS : MESSAGE_TYPE::PRRX);
      //-------------------------------------------------------------------------------------
      ensure(guard.state == STATE::LOCAL_POSSESSION_CHANGE);  // TODO add ssd
      // -------------------------------------------------------------------------------------
      // Shared Possession
      // -------------------------------------------------------------------------------------
      if (DESIRED_MODE == POSSESSION::SHARED) {
         // -------------------------------------------------------------------------------------
         // Shared - Exclusive conflict
         // -------------------------------------------------------------------------------------
         if (guard.frame->possession == POSSESSION::EXCLUSIVE) {
            guard.frame->pVersion++;
            // -------------- -----------------------------------------------------------------------
            if (guard.frame->possessors.exclusive == nodeId) {
               // -------------------------------------------------------------------------------------
               // local -> downgrade
               ensure(guard.frame->state == BF_STATE::HOT);
               guard.frame->possessors.exclusive = 0;
               guard.frame->possession = POSSESSION::SHARED;
               guard.frame->setPossessor(nodeId);  // downgrade
               response.resultType = RESULT::WithPage;
            } else {
               ensure(guard.frame->state == BF_STATE::EVICTED);
               ensure(!guard.frame->isPossessor(nodeId));
               // -------------------------------------------------------------------------------------
               response.resultType = RESULT::NoPageExclusiveConflict;
               response.conflictingNodeId = guard.frame->possessors.exclusive;
               response.pVersion = guard.frame->pVersion;
               writeMsg(clientId, response,page_handle);
               // -------------------------------------------------------------------------------------
               // set new possessor update metadate asynchronously
               guard.frame->possessors.exclusive = 0;
               guard.frame->possession = POSSESSION::SHARED;
               guard.frame->setPossessor(ctx.bmId);
               // -------------------------------------------------------------------------------------
               guard.frame->latch.unlatchExclusive();
               return;
            }
            ensure(guard.frame->state == BF_STATE::HOT);
         } else {
            // -------------------------------------------------------------------------------------
            // No Conflict but page might be evicted which issues and Copy Request to another node which we
            // need to track
            // -------------------------------------------------------------------------------------
            if (guard.frame->state == BF_STATE::EVICTED) {
               ensure(!guard.frame->isPossessor(nodeId));
               // -------------------------------------------------------------------------------------
               response.resultType = RESULT::NoPageEvicted;
               response.conflictingNodeId = guard.frame->possessors.shared;  // not really conflicting here
               response.pVersion = guard.frame->pVersion;
               writeMsg(clientId, response, page_handle);
               // -------------------------------------------------------------------------------------
               // register possible copy request
               // -------------------------------------------------------------------------------------
               // acquire latch
               {
                  std::unique_lock<std::mutex> ulquard(partition.inflightCRMutex);
                  partition.inflightCRs[m_i].inflight = true;
                  partition.inflightCRs[m_i].pid = guard.frame->pid;
                  partition.inflightCRs[m_i].pVersion = guard.frame->pVersion;
               }

               // set new possessor update metadate asynchronously after we notified requester
               // -------------------------------------------------------------------------------------
               guard.frame->setPossessor(ctx.bmId);
               // -------------------------------------------------------------------------------------
               guard.frame->latch.unlatchExclusive();
               return;
            }
         }
      } else if (DESIRED_MODE == POSSESSION::EXCLUSIVE) {
         // -------------------------------------------------------------------------------------
         // Exclusive - Shared conflicts
         // -------------------------------------------------------------------------------------
         // increment pVersion
         guard.frame->pVersion++;
         guard.frame->dirty = true;
         // -------------------------------------------------------------------------------------
         if (guard.frame->possession == POSSESSION::SHARED) {
            guard.frame->possessors.shared.reset(nodeId);  // reset own node id already
            // test if there are more shared possessors
            if (guard.frame->possessors.shared.any()) {
               // -------------------------------------------------------------------------------------
               // if outdated else write page
               if (guard.frame->state == BF_STATE::EVICTED) {
                  response.resultType = RESULT::NoPageSharedConflict;
                  response.conflictingNodeId = guard.frame->possessors.shared;

                  response.pVersion = guard.frame->pVersion;
                  // -------------------------------------------------------------------------------------
                  writeMsg(clientId, response, page_handle);  // response to client as in old code path
                  
                  // -------------------------------------------------------------------------------------
                  guard.frame->possession = POSSESSION::EXCLUSIVE;
                  guard.frame->setPossessor(ctx.bmId);
                  guard.frame->latch.unlatchExclusive();
                  return;
               }
               // -------------------------------------------------------------------------------------
               response.resultType = RESULT::WithPageSharedConflict;
               response.conflictingNodeId = guard.frame->possessors.shared;
               // -------------------------------------------------------------------------------------
            }
            guard.frame->possession = POSSESSION::EXCLUSIVE;  // update at the end
            // -------------------------------------------------------------------------------------
         } else {
            // -------------------------------------------------------------------------------------
            // Already in exclusive mode
            // -------------------------------------------------------------------------------------
            if (guard.frame->possessors.exclusive != nodeId) {  // we are not the exclusive possessor
               ensure(guard.frame->state == BF_STATE::EVICTED);
               ensure(!guard.frame->isPossessor(nodeId));
               // -------------------------------------------------------------------------------------
               response.resultType = RESULT::NoPageExclusiveConflict;
               response.conflictingNodeId = guard.frame->possessors.exclusive;
               response.pVersion = guard.frame->pVersion;
               // -------------------------------------------------------------------------------------
               writeMsg(clientId, response, page_handle);  // response to client as in old code path
               // -------------------------------------------------------------------------------------
               // set new possessor update metadate asynchronously after we notified requester
               // -------------------------------------------------------------------------------------
               guard.frame->setPossessor(ctx.bmId);
               // -------------------------------------------------------------------------------------
               guard.frame->latch.unlatchExclusive();
               return;
            }
            // if we are the possesor we do not need to take action as we oveerwrite it after the if
         }
         ensure(guard.frame->state == BF_STATE::HOT);
         guard.frame->state = BF_STATE::EVICTED;  // we own the page but someone is going to modify it
      }

      response.pVersion = guard.frame->pVersion;
      // -------------------------------------------------------------------------------------
      // set new possessor update metadate asynchronously
      // -------------------------------------------------------------------------------------
      guard.frame->setPossessor(ctx.bmId);
      ensure(guard.frame->isPossessor(ctx.bmId));
      ensure(guard.frame->page);
      // -------------------------------------------------------------------------------------
      // write back page
      // guard.frame->page->magicDebuggingNumber = request.pid;
      ensure((response.resultType == RESULT::WithPageSharedConflict) || (response.resultType == RESULT::WithPage));
      writePageAndMsg(clientId, guard.frame->page, request.pageOffset, response, page_handle);
      // -------------------------------------------------------------------------------------
      counters.incr(profiling::WorkerCounters::rdma_pages_tx);
      // -------------------------------------------------------------------------------------
      if (guard.frame->state == BF_STATE::EVICTED) {
         // frame stays in HT to maintain states page can be reused
         ctx.activeInvalidationBatch->add(guard.frame->page);
         guard.frame->page = nullptr;
      }
      guard.frame->latch.unlatchExclusive();
   }

   template <typename MSG>
   void delegateMsg(MHEndpoint& mhEndpoint, uint64_t clientId, uint64_t delegateToBM, MSG& msg, storage::PartitionedQueue<storage::Page*, PARTITIONS, BATCH_SIZE, utils::Stack>::BatchHandle& page_handle)
   {
      auto& wqe = mhEndpoint.wqe;
      uint64_t SIGNAL_ = FLAGS_pollingInterval - 1;
      rdma::completion signal = ((wqe & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
      uint8_t flag = 1;
      rdma::postWriteBatch(
          *(mhEndpoint.rctx), signal,
          RDMABatchElement{
              .memAddr = &msg, .size = (sizeof(MSG)), .remoteOffset = cctxs[clientId].remotePlOffsets[delegateToBM]},
          RDMABatchElement{.memAddr = &flag,
                           .size = (sizeof(uint8_t)),
                           .remoteOffset = cctxs[clientId].remoteMbOffsets[delegateToBM]});
      if ((wqe & SIGNAL_) == SIGNAL_) {
         int comp{0};
         ibv_wc wcReturn;
         while (comp == 0) {
            comp = rdma::pollCompletion(mhEndpoint.rctx->id->qp->send_cq, 1, &wcReturn);
         }
         if (wcReturn.status != IBV_WC_SUCCESS)
            throw;

         // reclaim invalidation
         reclaimInvalidations(cctxs[clientId], page_handle);
         std::swap(cctxs[clientId].activeInvalidationBatch, cctxs[clientId].passiveInvalidationBatch);
      }
      wqe++;
   }

   // writes page first and signales with another message for completion
   template <typename MSG>
   void writePageAndMsg(uint64_t clientId, Page* page, uintptr_t pageOffset, MSG& msg, storage::PartitionedQueue<storage::Page*, PARTITIONS, BATCH_SIZE, utils::Stack>::BatchHandle& page_handle)
   {
      auto& context = cctxs[clientId];
      writePageAndMsg(clientId, page, pageOffset, msg, context.plOffset, page_handle);
   }

   // writes page first and signales with another message for completion
   template <typename MSG>
   void writePageAndMsg(uint64_t clientId, Page* page, uintptr_t pageOffset, MSG& msg, uintptr_t plOffset, storage::PartitionedQueue<storage::Page*, PARTITIONS, BATCH_SIZE, utils::Stack>::BatchHandle& page_handle)
   {
      auto& context = cctxs[clientId];
      auto& wqe = context.wqe;
      uint64_t SIGNAL_ = FLAGS_pollingInterval - 1;
      rdma::completion signal = ((wqe & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
      // rdma::completion signal = rdma::completion::signaled;

      rdma::postWriteBatch(*(context.rctx), signal,
                           RDMABatchElement{.memAddr = page, .size = (sizeof(Page)), .remoteOffset = pageOffset},
                           RDMABatchElement{.memAddr = &msg, .size = (sizeof(MSG)), .remoteOffset = plOffset});

      if ((wqe & SIGNAL_) == SIGNAL_) {
         int comp{0};
         ibv_wc wcReturn;
         while (comp == 0) {
            comp = rdma::pollCompletion(context.rctx->id->qp->send_cq, 1, &wcReturn);
         }
         if (wcReturn.status != IBV_WC_SUCCESS)
            throw;

         // reclaim invalidation
         reclaimInvalidations(cctxs[clientId], page_handle);
         std::swap(cctxs[clientId].activeInvalidationBatch, cctxs[clientId].passiveInvalidationBatch);
      }
      wqe++;
   }

   template <typename MSG>
   void writeMsg(NodeID clientId, MSG& msg, storage::PartitionedQueue<storage::Page*, PARTITIONS, BATCH_SIZE, utils::Stack>::BatchHandle& page_handle)
   {
      auto& wqe = cctxs[clientId].wqe;
      uint64_t SIGNAL_ = FLAGS_pollingInterval - 1;
      rdma::completion signal = ((wqe & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
      rdma::postWrite(&msg, *(cctxs[clientId].rctx), signal, cctxs[clientId].plOffset);

      if ((wqe & SIGNAL_) == SIGNAL_) {
         int comp{0};
         ibv_wc wcReturn;
         while (comp == 0) {
            comp = rdma::pollCompletion(cctxs[clientId].rctx->id->qp->send_cq, 1, &wcReturn);
         }
         if (wcReturn.status != IBV_WC_SUCCESS)
            throw;

         // reclaim invalidation
         reclaimInvalidations(cctxs[clientId], page_handle);
         std::swap(cctxs[clientId].activeInvalidationBatch, cctxs[clientId].passiveInvalidationBatch);
      }
      wqe++;
   }
   // pointer
   template <typename MSG>
   void writeMsg(NodeID clientId, MSG* msg, storage::PartitionedQueue<storage::Page*, PARTITIONS, BATCH_SIZE, utils::Stack>::BatchHandle& page_handle)
   {
      auto& wqe = cctxs[clientId].wqe;
      uint64_t SIGNAL_ = FLAGS_pollingInterval - 1;
      rdma::completion signal = ((wqe & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
      rdma::postWrite(msg, *(cctxs[clientId].rctx), signal, cctxs[clientId].plOffset);

      if ((wqe & SIGNAL_) == SIGNAL_) {
         int comp{0};
         ibv_wc wcReturn;
         while (comp == 0) {
            comp = rdma::pollCompletion(cctxs[clientId].rctx->id->qp->send_cq, 1, &wcReturn);
         }
         if (wcReturn.status != IBV_WC_SUCCESS)
            throw;

         // reclaim invalidation
         reclaimInvalidations(cctxs[clientId], page_handle);
         std::swap(cctxs[clientId].activeInvalidationBatch, cctxs[clientId].passiveInvalidationBatch);
      }
      wqe++;
   }
   // -------------------------------------------------------------------------------------
   // Protocol functor which is injected to Buffermanager find frame;
   template <POSSESSION DESIRED_MODE>
   struct Protocol {
      void operator()(Guard& g, [[maybe_unused]] NodeID nodeId)
      {
         // -------------------------------------------------------------------------------------
         // Optimistic
         // -------------------------------------------------------------------------------------
         auto version = g.frame->latch.optimisticLatchOrRestart();
         if (!version.has_value()) {
            g.state = STATE::RETRY;
            g.latchState = LATCH_STATE::UNLATCHED;
            return;
         }
         // -------------------------------------------------------------------------------------
         if (g.frame->possession != DESIRED_MODE || !(g.frame->isPossessor(nodeId)))
            g.state = STATE::LOCAL_POSSESSION_CHANGE;
         else
            g.state = STATE::INITIALIZED;
         // -------------------------------------------------------------------------------------
         // Exclusive
         // -------------------------------------------------------------------------------------
         if (g.frame->latch.optimisticUpgradeToExclusive(version.value())) {
            g.vAcquired = g.frame->latch.version;
            g.latchState = LATCH_STATE::EXCLUSIVE;
         } else {
            g.state = STATE::RETRY;
            g.latchState = LATCH_STATE::UNLATCHED;
         }
      }

      void undo(Guard& g)
      {
         if (g.latchState == LATCH_STATE::EXCLUSIVE) {
            ensure(g.state != STATE::RETRY);
            ensure(g.frame->latch.isLatched());
            g.frame->latch.unlatchExclusive();
            g.state = STATE::RETRY;
            g.latchState = LATCH_STATE::UNLATCHED;
         }
      }
   };

   // Protocol functor which is injected to Buffermanager find frame;
   struct Invalidation {
      void operator()(Guard& g, [[maybe_unused]] NodeID nodeId)
      {
         // -------------------------------------------------------------------------------------
         // Exclusive
         // -------------------------------------------------------------------------------------
         if (!g.frame->latch.tryLatchExclusive()) {
            g.latchState = LATCH_STATE::UNLATCHED;
            g.state = STATE::RETRY;
            return;
         }
         g.vAcquired = g.frame->latch.version;
         g.latchState = LATCH_STATE::EXCLUSIVE;
         g.state = STATE::INITIALIZED;
      }

      void undo(Guard& g)
      {
         if (g.latchState == LATCH_STATE::EXCLUSIVE) {
            ensure(g.state != STATE::RETRY);
            ensure(g.frame->latch.isLatched());
            g.frame->latch.unlatchExclusive();
            g.state = STATE::RETRY;
            g.latchState = LATCH_STATE::UNLATCHED;
         }
      }
   };

   // Protocol functor which is injected to Buffermanager find frame;
   struct Copy {
      void operator()(Guard& g, [[maybe_unused]] NodeID nodeId)
      {
         // -------------------------------------------------------------------------------------
         // Exclusive
         // -------------------------------------------------------------------------------------
         if (!g.frame->latch.tryLatchShared()) {
            g.latchState = LATCH_STATE::UNLATCHED;
            g.state = STATE::RETRY;
            return;
         }
         g.vAcquired = g.frame->latch.version;
         g.latchState = LATCH_STATE::SHARED;
         g.state = STATE::INITIALIZED;
      }

      void undo(Guard& g)
      {
         if (g.latchState == LATCH_STATE::SHARED) {
            ensure(g.state != STATE::RETRY);
            ensure(!g.frame->latch.isLatched());
            g.frame->latch.unlatchShared();
            g.state = STATE::RETRY;
            g.latchState = LATCH_STATE::UNLATCHED;
         }
      }
   };

   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
};  // namespace rdma
}  // namespace rdma
}  // namespace scalestore
