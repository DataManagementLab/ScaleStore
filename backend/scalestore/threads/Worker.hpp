#pragma once
#include "Defs.hpp"
#include "scalestore/profiling/counters/CPUCounters.hpp"
#include "scalestore/profiling/counters/WorkerCounters.hpp"
#include "scalestore/rdma/CommunicationManager.hpp"
#include "scalestore/rdma/messages/Messages.hpp"
#include "scalestore/storage/buffermanager/Page.hpp"
#include "scalestore/storage/buffermanager/BufferFrame.hpp" 
#include "scalestore/storage/buffermanager/PartitionedQueue.hpp"
#include "ThreadContext.hpp"
// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace threads
{
using namespace rdma;
// -------------------------------------------------------------------------------------
struct Worker {

   // -------------------------------------------------------------------------------------
   static thread_local Worker* tlsPtr;
   static inline Worker& my() {
      return *Worker::tlsPtr;
   }
   // -------------------------------------------------------------------------------------
   uint64_t workerId;
   std::string name;
   // -------------------------------------------------------------------------------------
   profiling::CPUCounters cpuCounters;
   // -------------------------------------------------------------------------------------
   profiling::WorkerCounters counters;
   // -------------------------------------------------------------------------------------
   // RDMA
   // -------------------------------------------------------------------------------------
   // context for every connection 
   struct ConnectionContext {
      uintptr_t mbOffset;
      uintptr_t plOffset;
      rdma::Message* incoming;
      rdma::Message* outgoing;
      rdma::RdmaContext* rctx;
      uint64_t wqe; // wqe currently outstanding
   };
   
   // -------------------------------------------------------------------------------------
   rdma::CM<rdma::InitMessage>& cm;
   NodeID nodeId;
   std::vector<ConnectionContext> cctxs;
   std::unique_ptr<ThreadContext> threadContext;

   Worker(uint64_t workerId, std::string name, rdma::CM<rdma::InitMessage>& cm, NodeID nodeId, rdma::Type type);
   ~Worker();
   
   template <typename MSG>
   void writeMsg(NodeID nodeId, MSG& msg)
      {
         auto& wqe = cctxs[nodeId].wqe;
         uint64_t SIGNAL_ = FLAGS_pollingInterval -1;
         rdma::completion  signal = ((wqe & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
         uint8_t flag = 1;
         // rdma::postWriteBatch(*(cctxs[nodeId].rctx), signal,
         //                      RDMABatchElement{.memAddr = &msg, .size = (sizeof(MSG)), .remoteOffset = cctxs[nodeId].plOffset},
         //                      RDMABatchElement{.memAddr = &flag, .size = (sizeof(uint8_t)), .remoteOffset = cctxs[nodeId].mbOffset});
         // -------------------------------------------------------------------------------------
         rdma::postWrite(&msg, *(cctxs[nodeId].rctx), rdma::completion::unsignaled,cctxs[nodeId].plOffset);
         rdma::postWrite(&flag, *(cctxs[nodeId].rctx), signal ,cctxs[nodeId].mbOffset);
         // -------------------------------------------------------------------------------------
         if ((wqe & SIGNAL_) == SIGNAL_) {
            int comp{0};
            ibv_wc wcReturn;
            while (comp == 0) {
               comp = rdma::pollCompletion(cctxs[nodeId].rctx->id->qp->send_cq, 1, &wcReturn);
            }
         }
         wqe++;
   }

   template <typename RESPONSE, typename MSG>
   RESPONSE& writeMsgSync(NodeID nodeId, MSG& msg)
   {
      // -------------------------------------------------------------------------------------
      auto& response = *static_cast<RESPONSE*>(cctxs[nodeId].incoming);
      response.receiveFlag = 0;
      volatile uint8_t& received = response.receiveFlag;
      // -------------------------------------------------------------------------------------
      writeMsg(nodeId, msg);
      // -------------------------------------------------------------------------------------
      while (received == 0) {
         _mm_pause();
      }
      return response;
   }

   
   template <typename RESPONSE, typename MSG>
   void writeMsgASync(NodeID nodeId, MSG& msg)
   {
      auto& response = *static_cast<RESPONSE*>(cctxs[nodeId].incoming);
      response.receiveFlag = 0;
      // -------------------------------------------------------------------------------------
      writeMsg(nodeId, msg);
   }

   template <typename RESPONSE>
   RESPONSE& collectResponseMsgASync(NodeID nodeId)
   {
      // -------------------------------------------------------------------------------------
      auto& response = *static_cast<RESPONSE*>(cctxs[nodeId].incoming);
      volatile uint8_t& received = response.receiveFlag;
      // -------------------------------------------------------------------------------------
      while (received == 0) {
         _mm_pause();
      }
      // -------------------------------------------------------------------------------------
      return response;
   }

   
   
};
// -------------------------------------------------------------------------------------
}  // namespace threads
}  // namespace scalestore

