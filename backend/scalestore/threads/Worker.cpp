#include "Worker.hpp"
// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace threads
{
// -------------------------------------------------------------------------------------
thread_local Worker* Worker::tlsPtr = nullptr;
// -------------------------------------------------------------------------------------
Worker::Worker(uint64_t workerId, std::string name, rdma::CM<rdma::InitMessage>& cm, NodeID nodeId, rdma::Type type) : workerId(workerId), name(name), cpuCounters(name), cm(cm), nodeId(nodeId) , cctxs(FLAGS_nodes), threadContext(std::make_unique<ThreadContext>()){
   ThreadContext::tlsPtr = threadContext.get();
   // -------------------------------------------------------------------------------------
   // Connection to MessageHandler
   // -------------------------------------------------------------------------------------
   // First initiate connection

   for (uint64_t n_i = 0; n_i < FLAGS_nodes; n_i++) {
      // -------------------------------------------------------------------------------------
      if (n_i == nodeId) continue;
      // -------------------------------------------------------------------------------------
      auto& ip = NODES[FLAGS_nodes][n_i];
      cctxs[n_i].rctx = &(cm.initiateConnection(ip, type, workerId, nodeId));
      // -------------------------------------------------------------------------------------
      cctxs[n_i].incoming = (rdma::Message*)cm.getGlobalBuffer().allocate(rdma::LARGEST_MESSAGE, CACHE_LINE);
      cctxs[n_i].outgoing = (rdma::Message*)cm.getGlobalBuffer().allocate(rdma::LARGEST_MESSAGE, CACHE_LINE);
      cctxs[n_i].wqe = 0;
      // -------------------------------------------------------------------------------------
   }
   // -------------------------------------------------------------------------------------
   // Second finish connection
   rdma::InitMessage* init = (rdma::InitMessage*)cm.getGlobalBuffer().allocate(sizeof(rdma::InitMessage));
   for (uint64_t n_i = 0; n_i < FLAGS_nodes; n_i++) {
      // -------------------------------------------------------------------------------------
      if (n_i == nodeId) continue;
      // -------------------------------------------------------------------------------------
      // fill init messages
      init->mbOffset = 0;  // No MB offset
      init->plOffset = (uintptr_t)cctxs[n_i].incoming;
      init->bmId = nodeId;
      init->type = rdma::MESSAGE_TYPE::Init;
      // -------------------------------------------------------------------------------------
      cm.exchangeInitialMesssage(*(cctxs[n_i].rctx), init);
      // -------------------------------------------------------------------------------------
      cctxs[n_i].plOffset = (reinterpret_cast<rdma::InitMessage*>((cctxs[n_i].rctx->applicationData)))->plOffset;
      cctxs[n_i].mbOffset = (reinterpret_cast<rdma::InitMessage*>((cctxs[n_i].rctx->applicationData)))->mbOffset;
      ensure((reinterpret_cast<rdma::InitMessage*>((cctxs[n_i].rctx->applicationData)))->bmId == n_i);
   }

   // -------------------------------------------------------------------------------------
   // "broadcast" remote mailbox information to all message handlers to allow the MH to delegate requests
   // transparently 
   // -------------------------------------------------------------------------------------
   for (uint64_t o_n_i = 0; o_n_i < FLAGS_nodes; o_n_i++) {
      if(o_n_i == nodeId) continue;
      for (uint64_t i_n_i = 0; i_n_i < FLAGS_nodes; i_n_i++) {
         if (o_n_i == i_n_i)
            continue;
         if (nodeId == i_n_i)
            continue;
         auto& request = *MessageFabric::createMessage<DelegationRequest>(cctxs[o_n_i].outgoing, cctxs[i_n_i].mbOffset, cctxs[i_n_i].plOffset, i_n_i);
         assert(request.type == MESSAGE_TYPE::DR);
         [[maybe_unused]] auto& response = writeMsgSync<DelegationResponse>(o_n_i, request);

      }      
   }

}

// -------------------------------------------------------------------------------------
Worker::~Worker()
{
// finish connection
   for (uint64_t n_i = 0; n_i < FLAGS_nodes; n_i++) {
      // -------------------------------------------------------------------------------------
      if (n_i == nodeId)
         continue;
      auto& request = *MessageFabric::createMessage<FinishRequest>(cctxs[n_i].outgoing);
      assert(request.type == MESSAGE_TYPE::Finish);
      writeMsg(n_i, request);
      // -------------------------------------------------------------------------------------
   }
}
// -------------------------------------------------------------------------------------
}  // namespace threads
}  // namespace scalestore
