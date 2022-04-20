#pragma once
// -------------------------------------------------------------------------------------
#include "scalestore/rdma/CommunicationManager.hpp"
#include "scalestore/storage/buffermanager/Buffermanager.hpp"
#include "scalestore/rdma/MessageHandler.hpp"
#include "scalestore/storage/buffermanager/AsyncWriteBuffer.hpp"
// -------------------------------------------------------------------------------------

namespace scalestore
{
namespace storage
{
using namespace rdma;

constexpr uint64_t batchSize = 32; // keeps rdma latency reasonable low 
constexpr uint64_t pp_batch_size = 128; // most robust and fast due to cache 
constexpr uint64_t required_samples = 600; // reasonable high confidence in sampling 
constexpr uint64_t outgoingBachtes = 2;
constexpr uint64_t minOutgoingElements = 16;
constexpr uint64_t maxOutstandingWrites = 32;
constexpr uint64_t WR_READ_ID = 99;
constexpr uint64_t BITMAP_BATCH = 8 * 2; // two cacheline a 8 uint64_t

struct EvictionEntry {
   PID pid;
   uintptr_t offset;
   uint64_t pVersion;
};

struct EvictionRequest {
   uint64_t bmId;
   uint64_t pId;
   uint64_t failedSend = 0;
   uint64_t elements;
   EvictionEntry entries[batchSize];
   void reset() {
      elements = 0;
      failedSend = 0;
   }
   bool empty(){ return elements == 0;}
   bool full(){ return elements == batchSize;}
   void add(EvictionEntry&& entry){
      ensure(elements < batchSize);
      entries[elements] = std::move(entry);
      elements++;
   }
};
struct EvictionResponse {
   PID pids[batchSize];

   void reset() { elements = 0;}
   bool empty(){ return elements == 0;}
   bool full(){ return elements == batchSize;}
   void add(PID pid){
      ensure(elements < batchSize);
      pids[elements] = pid;
      elements++;
   }

   size_t elements;
   uint64_t bmId;
   uint64_t pId;
};

struct PageProvider {
   std::atomic<bool> finishedInit = false;
   std::atomic<bool> threadsRunning = true;
   std::atomic<size_t> threadCount = 0;
   rdma::CM<InitMessage>& cm;
   storage::Buffermanager& bm;
   std::vector<MessageHandler::MailboxPartition>& mhPartitions;
   const uint64_t freeBFLimit;
   const uint64_t coolingBFLimit;
   s32 ssd_fd;
   std::vector<std::thread> pp_threads;
   void startThread();
   void stopThread();
   void init();
   PageProvider(CM<rdma::InitMessage>& cm,
                storage::Buffermanager& bm,
                std::vector<MessageHandler::MailboxPartition>& mhPartitions,
                s32 ssd_fd);
   ~PageProvider();


   template<typename T>
   struct MsgSlots{
      T* current;
      T* prev;

      void swap(){
         std::swap(current, prev);
      }
   };

   
   struct alignas(CACHE_LINE) ConnectionContext {
      // requests
      uintptr_t mbOffset;
      uintptr_t plOffset;
      // resonses
      bool responseExpected = false;
      bool readScheduled = false;
      bool responseOutstanding = false;
      bool readCompleted = false; // for read 
      bool pollCompleted = false; // for writes
      uintptr_t respMbOffset;
      uintptr_t respPlOffset;
      MsgSlots<EvictionResponse> outgoingResp;
      MsgSlots<EvictionRequest> outgoing;  // from which request will be sent
      std::vector<BufferFrame*> latchedFrames;  // local latched frames to answer requests
      MsgSlots<std::vector<BufferFrame*>> inflightFrames;  // local latched frames to answer requests need also two?
      rdma::RdmaContext* rctx;
      uint64_t wqe;  // wqe currently outstanding will be resetted with reads completions
      NodeID bmId;   // id to which incoming client connection belongs
      uint64_t requestLatency {0};
      uint64_t responseLatency {0};

      struct RdmaReadBuffer {
         struct ibv_send_wr sq_wr[batchSize];
         struct ibv_sge send_sgl[batchSize];
         struct ibv_send_wr* bad_wr;
         size_t elements = 0;

         void add(ConnectionContext& ctx, void* memAddr, uint64_t remoteOffset)
         {
            RdmaContext& rCtx = *(ctx.rctx);
            send_sgl[elements].addr = (uint64_t)(unsigned long)memAddr;
            send_sgl[elements].length = sizeof(Page);
            send_sgl[elements].lkey = rCtx.mr->lkey;
            sq_wr[elements].opcode = IBV_WR_RDMA_READ;
            sq_wr[elements].send_flags = 0;  // unsignaled>
            sq_wr[elements].sg_list = &send_sgl[elements];
            sq_wr[elements].num_sge = 1;
            sq_wr[elements].wr.rdma.rkey = rCtx.rkey;
            sq_wr[elements].wr.rdma.remote_addr = remoteOffset;
            sq_wr[elements].wr_id = WR_READ_ID;
            sq_wr[elements].next = nullptr;
            // modify previous element
            if (elements > 0) {
               sq_wr[elements - 1].next = &sq_wr[elements];
            }
            elements++;
         }
         void postRead(ConnectionContext& ctx)
         {
            RdmaContext& rCtx = *(ctx.rctx);
            // set last eleement signaled
            sq_wr[elements -1].send_flags = IBV_SEND_SIGNALED; 
             
            ensure(elements > 0);
            auto ret = ibv_post_send(rCtx.id->qp, &sq_wr[0], &bad_wr);
            if (ret)
               throw std::runtime_error("Failed to post send request" + std::to_string(ret) + " " +
                                        std::to_string(errno));
         };
         bool pollCompletion(ConnectionContext& ctx)
         {
            RdmaContext& rCtx = *(ctx.rctx);

            if (ctx.readCompleted) {
               elements = 0;
               return true;
            }

            for (uint64_t i = 0; i < 2; i++) {
               int comp{0};
               ibv_wc wcReturn;
               comp = rdma::pollCompletion(rCtx.id->qp->send_cq, 1, &wcReturn);
               if (comp > 0 && wcReturn.wr_id == WR_READ_ID) {
                  elements = 0;
                  ctx.wqe = 0;
                  return true;
               }
               if(comp > 0){
                  // mark pollcompletion
                  ctx.pollCompleted = true;
               }
            }

            return false;
            // reset
         }
      };

      RdmaReadBuffer rdmaReadBuffer;
   };

   
   // -------------------------------------------------------------------------------------
   // Mailbox partition per thread
   struct Partition {
      Partition(uint8_t* mailboxes,
                EvictionRequest* incoming,  // incoming requests
                uint8_t* respMailboxes,
                EvictionResponse* respIncoming,  // incoming requests
                uint64_t numberMailboxes,
                uint64_t begin,
                uint64_t end)
          : mailboxes(mailboxes),
            incoming(incoming),
            respMailboxes(respMailboxes),
            respIncoming(respIncoming),
            numberMailboxes(numberMailboxes),
            cctxs(FLAGS_nodes),
            begin(begin),
            end(end)
      {
      }

      // -------------------------------------------------------------------------------------
      // requests
      uint8_t* mailboxes = nullptr;
      EvictionRequest* incoming = nullptr;
      // -------------------------------------------------------------------------------------
      // responses
      uint8_t* respMailboxes = nullptr;
      EvictionResponse* respIncoming = nullptr;
      // -------------------------------------------------------------------------------------
      uint64_t numberMailboxes;  // remote nodes
      std::vector<ConnectionContext> cctxs;
      uint64_t begin;  // pTabel partitions
      uint64_t end;
   };
   // -------------------------------------------------------------------------------------


   // -------------------------------------------------------------------------------------

   std::vector<Partition> partitions;

   /// TODO: remove debug counter
   uint64_t requests {0};
   uint64_t responses_sent {0};

   
   template <typename MSG>
   void writeRequest(uint64_t partitionId, NodeID nodeId, MSG& msg)
   {
      ConnectionContext& ctx = partitions[partitionId].cctxs[nodeId];
      auto& wqe = ctx.wqe;
      rdma::completion signal = (wqe == maxOutstandingWrites) ? rdma::completion::signaled : rdma::completion::unsignaled;
      uint8_t flag = 1;
      rdma::postWriteBatch(*(ctx.rctx), signal,
                           RDMABatchElement{.memAddr = &msg, .size = (sizeof(MSG)), .remoteOffset = ctx.plOffset},
                           RDMABatchElement{.memAddr = &flag, .size = (sizeof(uint8_t)), .remoteOffset = ctx.mbOffset});

      
      if ((wqe == maxOutstandingWrites && !ctx.pollCompleted)) {

         int comp{0};
         ibv_wc wcReturn;
         while (comp == 0) {
            comp = rdma::pollCompletion(ctx.rctx->id->qp->send_cq, 1, &wcReturn);
            if(wcReturn.wr_id == WR_READ_ID){
               comp = 0;
               ctx.readCompleted = true;
            }
               
         }
         ensure(wcReturn.wr_id != WR_READ_ID);
         wqe = 0;
         ctx.pollCompleted = false;
      }
      wqe++;
   }

   
   template <typename MSG>
   void writeResponse(uint64_t partitionId, NodeID nodeId, MSG& msg)
   {
      ConnectionContext& ctx = partitions[partitionId].cctxs[nodeId];
      auto& wqe = ctx.wqe;
      rdma::completion signal = (wqe == maxOutstandingWrites) ? rdma::completion::signaled : rdma::completion::unsignaled;
      uint8_t flag = 1;
      rdma::postWriteBatch(*(ctx.rctx), signal,
                           RDMABatchElement{.memAddr = &msg, .size = (sizeof(MSG)), .remoteOffset = ctx.respPlOffset},
                           RDMABatchElement{.memAddr = &flag, .size = (sizeof(uint8_t)), .remoteOffset = ctx.respMbOffset});

      if ((wqe == maxOutstandingWrites && !ctx.pollCompleted)) {
         int comp{0};
         ibv_wc wcReturn;
         while (comp == 0) {
            comp = rdma::pollCompletion(ctx.rctx->id->qp->send_cq, 1, &wcReturn);
            if(wcReturn.wr_id == WR_READ_ID){
               comp = 0;
               ctx.readCompleted = true;
            }
         }
         ensure(wcReturn.wr_id != WR_READ_ID);
         //reset
         wqe = 0;
         ctx.pollCompleted = false;
      }
      wqe++;
   }

};
}  // namespace storage
}  // namespace scalestore

