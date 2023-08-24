#pragma once
// -------------------------------------------------------------------------------------
#include "Defs.hpp"
#include "scalestore/Config.hpp"
#include "scalestore/utils/MemoryManagement.hpp"
// -------------------------------------------------------------------------------------
#include <arpa/inet.h>
#include <atomic>
#include <cassert>
#include <fstream> // std::ifstream
#include <gflags/gflags.h>
#include <infiniband/verbs.h>
#include <iostream>
#include <memory_resource>
#include <mutex>
#include <netdb.h>
#include <netinet/in.h>
#include <rdma/rdma_cma.h>
#include <string>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>

static int debug = 0;
#define DEBUG_LOG(msg)                                                         \
  if (debug)                                                                   \
  std::cout << msg << std::endl
#define DEBUG_VAR(var)                                                         \
  if (debug)                                                                   \
  std::cout << #var << " " << var << std::endl

#define USE_INLINE 1

namespace scalestore {
namespace rdma {

// smaller inline size reduces WQE, max with our cards would be 220
static constexpr uint64_t INLINE_SIZE = 64; // LARGEST MESSAGE

enum completion : bool {
  signaled = true,
  unsignaled = false,
};
// application specific
enum Type {
  RDMA_CM = 0,
  WORKER = 1,
  PAGE_PROVIDER = 2,
  MESSAGE_HANDLER = 3,
};

struct RdmaContext {
  void *applicationData;
  uint32_t rkey;
  ibv_mr *mr;
  rdma_cm_id *id;
  Type type;
  uint64_t typeId;
  NodeID nodeId;
};

// -------------------------------------------------------------------------------------
// Compile time batching
// -------------------------------------------------------------------------------------

struct RDMABatchElement {
  void *memAddr;
  size_t size;
  size_t remoteOffset;
};

template <typename... ARGS>
void postWriteBatch(RdmaContext &context, completion wc, ARGS &&...args) {
  constexpr uint64_t numberElements =
      std::tuple_size<std::tuple<ARGS...>>::value;

  auto *qp = context.id->qp;
  auto *mr = context.mr;
  auto rkey = context.rkey;

  struct ibv_send_wr sq_wr[numberElements];
  struct ibv_sge send_sgl[numberElements];
  struct ibv_send_wr *bad_wr;
  RDMABatchElement element[numberElements] = {args...};

  for (uint64_t b_i = 0; b_i < numberElements; b_i++) {
    send_sgl[b_i].addr = (uint64_t)(unsigned long)element[b_i].memAddr;
    send_sgl[b_i].length = element[b_i].size;
    send_sgl[b_i].lkey = mr->lkey;
    sq_wr[b_i].opcode = IBV_WR_RDMA_WRITE;
    sq_wr[b_i].send_flags =
        ((b_i == numberElements - 1) && wc) ? IBV_SEND_SIGNALED : 0;
#ifdef USE_INLINE
    sq_wr[b_i].send_flags |=
        (element[b_i].size <= INLINE_SIZE) ? IBV_SEND_INLINE : 0;
#endif
    sq_wr[b_i].sg_list = &send_sgl[b_i];
    sq_wr[b_i].num_sge = 1;
    sq_wr[b_i].wr.rdma.rkey = rkey;
    sq_wr[b_i].wr.rdma.remote_addr = element[b_i].remoteOffset;
    sq_wr[b_i].wr_id = 0;
    sq_wr[b_i].next =
        (b_i == numberElements - 1)
            ? nullptr
            : &sq_wr[b_i + 1]; // do not forget to set this otherwise it crashes
  }

  auto ret = ibv_post_send(qp, &sq_wr[0], &bad_wr);
  if (ret)
    throw std::runtime_error("Failed to post send request" +
                             std::to_string(ret) + " " + std::to_string(errno));
}

// -------------------------------------------------------------------------------------

inline void postReceive(void *memAddr, size_t size, ibv_qp *qp, ibv_mr *mr) {
  struct ibv_recv_wr rq_wr; /* recv work request record */
  struct ibv_sge recv_sgl;  /* recv single SGE */
  struct ibv_recv_wr *bad_wr;
  recv_sgl.addr =
      (uintptr_t)memAddr; // important to use address of buffer not of ptr
  recv_sgl.length = size;
  recv_sgl.lkey = mr->lkey;
  rq_wr.sg_list = &recv_sgl;
  rq_wr.num_sge = 1;
  rq_wr.next = nullptr;
  rq_wr.wr_id = 1;
  auto ret = ibv_post_recv(qp, &rq_wr, &bad_wr); // returns 0 on success
  if (ret)
    throw std::runtime_error("Failed to post receive to QP with errno " +
                             std::to_string(ret));
}

template <typename T>
inline void postReceive(T *memAddr, ibv_qp *qp, ibv_mr *mr) {
  static_assert(!std::is_void<T>::value,
                "post receive cannot be called with void use other function");
  postReceive(memAddr, sizeof(T), qp, mr);
}

template <typename T>
inline void postReceive(T *memAddr, RdmaContext &context) {
  static_assert(!std::is_void<T>::value,
                "post receive cannot be called with void use other function");
  postReceive(memAddr, sizeof(T), context.id->qp, context.mr);
}

inline void postSend(void *memAddr, size_t size, ibv_qp *qp, ibv_mr *mr,
                     completion wc) {
  struct ibv_send_wr sq_wr;
  struct ibv_sge send_sgl;
  struct ibv_send_wr *bad_wr;
  send_sgl.addr = (uintptr_t)memAddr;
  send_sgl.length = size;
  send_sgl.lkey = mr->lkey;
  sq_wr.opcode = IBV_WR_SEND;
  sq_wr.send_flags = wc ? IBV_SEND_SIGNALED : 0;
#ifdef USE_INLINE
  sq_wr.send_flags |= (size <= INLINE_SIZE) ? IBV_SEND_INLINE : 0;
#endif
  sq_wr.sg_list = &send_sgl;
  sq_wr.num_sge = 1;
  sq_wr.next = nullptr; // do not forget to set this otherwise it  crashes
  auto ret = ibv_post_send(qp, &sq_wr, &bad_wr);
  if (ret)
    throw std::runtime_error("Failed to post send request");
}

template <typename T>
inline void postSend(T *memAddr, ibv_qp *qp, ibv_mr *mr, completion wc) {
  static_assert(!std::is_void<T>::value,
                " post send cannot be called with void");
  postSend(memAddr, sizeof(T), qp, mr, wc);
}

template <typename T>
inline void postSend(T *memAddr, RdmaContext &context, completion wc) {
  static_assert(!std::is_void<T>::value,
                " post send cannot be called with void");
  postSend(memAddr, sizeof(T), context.id->qp, context.mr, wc);
}

inline void postWrite(void *memAddr, size_t size, ibv_qp *qp, ibv_mr *mr,
                      completion wc, size_t rkey, size_t remoteOffset) {
  struct ibv_send_wr sq_wr;
  struct ibv_sge send_sgl;
  struct ibv_send_wr *bad_wr;
  send_sgl.addr = (uint64_t)(unsigned long)memAddr;
  send_sgl.length = size;
  send_sgl.lkey = mr->lkey;
  sq_wr.opcode = IBV_WR_RDMA_WRITE;
  sq_wr.send_flags = wc ? IBV_SEND_SIGNALED : 0;
#ifdef USE_INLINE
  sq_wr.send_flags |= (size <= INLINE_SIZE) ? IBV_SEND_INLINE : 0;
#endif
  sq_wr.sg_list = &send_sgl;
  sq_wr.num_sge = 1;
  sq_wr.wr.rdma.rkey = rkey;
  sq_wr.wr.rdma.remote_addr = remoteOffset;
  sq_wr.next = nullptr; // do not forget to set this otherwise it  crashes
  auto ret = ibv_post_send(qp, &sq_wr, &bad_wr);
  if (ret)
    throw std::runtime_error("Failed to post send request" +
                             std::to_string(ret) + " " + std::to_string(errno));
}

template <typename T>
inline void postWrite(T *memAddr, ibv_qp *qp, ibv_mr *mr, completion wc,
                      size_t rkey, size_t remoteOffset) {
  static_assert(!std::is_void<T>::value,
                "post write cannot be called with void");
  postWrite(memAddr, sizeof(T), qp, mr, wc, rkey, remoteOffset);
}

template <typename T>
inline void postWrite(T *memAddr, RdmaContext &context, completion wc,
                      size_t remoteOffset) {
  static_assert(!std::is_void<T>::value,
                "post write cannot be called with void");
  postWrite(memAddr, sizeof(T), context.id->qp, context.mr, wc, context.rkey,
            remoteOffset);
}

inline void postRead(void *memAddr, size_t size, ibv_qp *qp, ibv_mr *mr,
                     completion wc, size_t rkey, size_t remoteOffset,
                     size_t wcId = 0) {
  struct ibv_send_wr sq_wr;
  struct ibv_sge send_sgl;
  struct ibv_send_wr *bad_wr;
  send_sgl.addr = (uint64_t)(unsigned long)memAddr;
  send_sgl.length = size;
  send_sgl.lkey = mr->lkey;
  sq_wr.opcode = IBV_WR_RDMA_READ;
  sq_wr.send_flags = wc ? IBV_SEND_SIGNALED : 0;
  sq_wr.sg_list = &send_sgl;
  sq_wr.num_sge = 1;
  sq_wr.wr.rdma.rkey = rkey;
  sq_wr.wr.rdma.remote_addr = remoteOffset;
  sq_wr.wr_id = wcId;
  sq_wr.next = nullptr; // do not forget to set this otherwise it  crashes
  auto ret = ibv_post_send(qp, &sq_wr, &bad_wr);
  if (ret)
    throw std::runtime_error("Failed to post send request" +
                             std::to_string(ret) + " " + std::to_string(errno));
}

template <typename T>
inline void postRead(T *memAddr, ibv_qp *qp, ibv_mr *mr, completion wc,
                     size_t rkey, size_t remoteOffset, size_t wcId = 0) {
  static_assert(!std::is_void<T>::value,
                "post write cannot be called with void");
  postRead(memAddr, sizeof(T), qp, mr, wc, rkey, remoteOffset, wcId);
}

template <typename T>
inline void postRead(T *memAddr, RdmaContext &context, completion wc,
                     size_t remoteOffset, size_t wcId = 0) {
  static_assert(!std::is_void<T>::value,
                "post write cannot be called with void");
  postRead(memAddr, sizeof(T), context.id->qp, context.mr, wc, context.rkey,
           remoteOffset, wcId);
}

// low level wrapper; once returned every wc need to be checked for success
inline int pollCompletion(ibv_cq *cq, size_t expected, ibv_wc *wcReturn) {
  int numCompletions{0};
  numCompletions = ibv_poll_cq(cq, expected, wcReturn);
  if (numCompletions < 0)
    throw std::runtime_error("Poll cq failed");
  return numCompletions;
}

// lambda function can be passed to evaluate status etc.
template <typename F>
inline int pollCompletion(ibv_cq *cq, size_t expected, ibv_wc *wcReturn,
                          F &func) {
  int numCompletions = pollCompletion(cq, expected, wcReturn);
  func(numCompletions);
  return numCompletions;
}

template <typename INITIAL_MSG> class CM {
public:
  //! Default constructor
  CM()
      : port(htons(FLAGS_port)),
        mbr(FLAGS_dramGB * FLAGS_rdmaMemoryFactor * 1024 * 1024 * 1024),
        running(true), handler(&CM::handle, this) {
    // create thread
    incomingChannel = rdma_create_event_channel();
    if (!incomingChannel)
      throw std::runtime_error("Could not create rdma_event_channels");

    int ret =
        rdma_create_id(incomingChannel, &incomingCmId, nullptr, RDMA_PS_TCP);
    if (ret == -1)
      throw std::runtime_error("Could not create id");

    // create rdma ressources
    struct sockaddr_storage sin;
    getAddr(FLAGS_ownIp, (struct sockaddr *)&sin);
    bindHandler(sin);
    createPD(incomingCmId); // single pd shared between clients and server
    createMR();             // single mr shared between clients and server
    // sendCq = createCQ(incomingCmId);  // completion queue for all incoming
    // requests

    if (FLAGS_nodes == 1)
      running = false;
    initialized = true; // sync handler thread
  }

  ~CM() {
    // disconnect and delete application context
    for (auto *context : outgoingIds) {
      [[maybe_unused]] auto ret = rdma_disconnect(context->id);
      assert(ret == 0);
    }
    for (auto *context : incomingIds) {
      [[maybe_unused]] auto ret = rdma_disconnect(context->id);
      assert(ret == 0);
    }
    // drain disconnect events
    for (auto *c : outgoingChannels) {
      struct rdma_cm_event *event;
      [[maybe_unused]] auto ret = rdma_get_cm_event(c, &event);
      rdma_ack_cm_event(event);
      assert(ret == 0);
    }
    for (auto *context : outgoingIds) {
      rdma_destroy_qp(context->id);
    }
    for (auto *cq : outgoingCqs) {
      ibv_destroy_cq(cq);
    }
    for (auto *context : outgoingIds) {
      rdma_destroy_id(context->id);
    }

    std::vector<ibv_cq *> cqs; // order is important
    for (auto *context : incomingIds) {
      cqs.push_back(context->id->qp->send_cq);
    }
    for (auto *context : incomingIds) {
      rdma_destroy_qp(context->id);
    }

    for (auto *cq : cqs) {
      ibv_destroy_cq(cq);
    }

    for (auto *context : incomingIds) {
      rdma_destroy_id(context->id);
    }

    rdma_destroy_id(incomingCmId);
    rdma_destroy_event_channel(incomingChannel);

    for (auto *c : outgoingChannels) {
      rdma_destroy_event_channel(c);
    }
    ibv_dereg_mr(mr);
    ibv_dealloc_pd(pd);
    handler.join(); // handler joins last to drain incoming disconnection events

    for (auto *context : outgoingIds)
      delete context;
    for (auto *context : incomingIds)
      delete context;
  }

  void handle() {
    // wait for initialization
    while (!initialized)
      ; // block
    std::unordered_map<uintptr_t, ComSetupContext *> connections;
    while (running) {
      struct rdma_cm_event *event;
      auto ret = rdma_get_cm_event(incomingChannel, &event);
      if (ret)
        throw;
      struct rdma_cm_id *currentId = event->id;
      ComSetupContext *context;
      if (event->event ==
          RDMA_CM_EVENT_CONNECT_REQUEST) // if new connection create context
        connections[(uintptr_t)currentId] = new ComSetupContext;
      context = connections[(uintptr_t)currentId];
      ensure(context != nullptr);

      switch (event->event) {
      case RDMA_CM_EVENT_ADDR_RESOLVED:
        DEBUG_LOG("RDMA_CM_EVENT_ADDR_RESOLVED");
        break;
      case RDMA_CM_EVENT_ROUTE_RESOLVED:
        DEBUG_LOG("RDMA_CM_EVENT_ROUTE_RESOLVED");
        break;
      case RDMA_CM_EVENT_CONNECT_REQUEST: {
        DEBUG_LOG("RDMA_CM_EVENT_CONNECT_REQUEST received");
        numberConnections++;
        rdma_ack_cm_event(event);
        ibv_cq *incomingCQ = createCQ(currentId);
        createQP(currentId, incomingCQ);
        context->response =
            static_cast<RdmaInfo *>(mbr.allocate(sizeof(RdmaInfo)));
        postReceive(context->response, currentId->qp, mr);
        auto *applicationData =
            static_cast<INITIAL_MSG *>(mbr.allocate(sizeof(INITIAL_MSG)));
        auto *rdmaContext = createRdmaContext(currentId, applicationData);
        currentId->context = rdmaContext;

        struct rdma_conn_param conn_param;
        memset(&conn_param, 0, sizeof conn_param);

        // std::cout << RDMA_MAX_RESP_RES << std::endl;
        // std::cout << RDMA_MAX_INIT_DEPTH << std::endl;
        // conn_param.responder_resources = RDMA_MAX_RESP_RES;
        // conn_param.initiator_depth = RDMA_MAX_INIT_DEPTH;

        conn_param.responder_resources = RDMA_MAX_RESP_RES;
        conn_param.initiator_depth = RDMA_MAX_INIT_DEPTH;

        ret = rdma_accept(currentId, &conn_param);
        if (ret)
          throw std::runtime_error("Rdma accept failed");

        std::unique_lock<std::mutex> l(incomingMut);
        incomingIds.push_back(rdmaContext);
        break;
      }
      case RDMA_CM_EVENT_ESTABLISHED: {
        DEBUG_LOG("RDMA_CM_EVENT_ESTABLISHED");
        exchangeRdmaInfo(currentId, mr, RDMA_CM, 0, 0); // does not know node Id
        ((RdmaContext *)currentId->context)->rkey = context->response->rkey;
        ((RdmaContext *)currentId->context)->type = context->response->type;
        ((RdmaContext *)currentId->context)->typeId = context->response->typeId;
        ((RdmaContext *)currentId->context)->nodeId = context->response->nodeId;
        // std::cout <<"************* received  RKEY << "<<
        // context->response->rkey << std::endl;
        rdma_ack_cm_event(event);
        auto *sock = rdma_get_peer_addr(currentId);
        std::string ip(inet_ntoa(((sockaddr_in *)sock)->sin_addr));
        DEBUG_VAR(ip);
        numberConnectionsEstablished++;
        break;
      }
      case RDMA_CM_EVENT_ADDR_ERROR:
      case RDMA_CM_EVENT_ROUTE_ERROR:
      case RDMA_CM_EVENT_CONNECT_ERROR:
      case RDMA_CM_EVENT_UNREACHABLE:
      case RDMA_CM_EVENT_REJECTED:
        DEBUG_LOG("Unexpected Event");
        break;
      case RDMA_CM_EVENT_DISCONNECTED: {
        DEBUG_LOG("RDMA_CM_EVENT_DISCONNECTED");
        rdma_ack_cm_event(event);
        numberConnections--;
        numberConnectionsEstablished--;
        break;
      }
      case RDMA_CM_EVENT_DEVICE_REMOVAL:
        DEBUG_LOG("RDMA_CM_EVENT_DEVICE_REMOVAL");
        break;
      case RDMA_CM_EVENT_TIMEWAIT_EXIT: {
        DEBUG_LOG("RDMA_CM_EVENT_TIMEWAIT_EXIT");
        rdma_ack_cm_event(event);
        break;
      }
      default:
        throw std::runtime_error("unhandled cm event received");
      }

      if (numberConnections == 0) {
        for (auto &[key, value] : connections) {
          delete value;
        }
        running = false; // no more open connections
        return;
      }
    }
    DEBUG_LOG("Stopped incoming connections handler");
  }

  // should be thread safe
  RdmaContext &initiateConnection(std::string ip, Type type, uint64_t typeId,
                                  NodeID nodeId) {
    auto *response = static_cast<RdmaInfo *>(mbr.allocate(
        sizeof(RdmaInfo))); // to not reallocate every restart and drain memory
    auto *applicationData =
        static_cast<INITIAL_MSG *>(mbr.allocate(sizeof(INITIAL_MSG)));

  RETRY:
    std::unique_lock<std::mutex> l(outgoingMut);
    rdma_event_channel *outgoingChannel = rdma_create_event_channel();
    if (!outgoingChannel)
      throw std::runtime_error("Could not create outgoing event channel");

    struct rdma_cm_id *outgoingCmId;
    auto ret =
        rdma_create_id(outgoingChannel, &outgoingCmId, nullptr, RDMA_PS_TCP);
    if (ret == -1)
      throw std::runtime_error("Could not create id");

    struct sockaddr_storage sin;
    getAddr(ip, (struct sockaddr *)&sin);
    resolveAddr(outgoingChannel, outgoingCmId, sin);
    ibv_cq *clientCq = createCQ(outgoingCmId);
    createQP(outgoingCmId, clientCq);

    postReceive(response, outgoingCmId->qp, mr);
    auto *rdmaContext = createRdmaContext(outgoingCmId, applicationData);
    outgoingCmId->context = rdmaContext;

    struct rdma_cm_event *event;
    struct rdma_conn_param conn_param;
    memset(&conn_param, 0, sizeof conn_param);

    // not yet sure if those have effect if we use plain qp's
    conn_param.responder_resources = RDMA_MAX_RESP_RES;
    conn_param.initiator_depth = RDMA_MAX_INIT_DEPTH;
    if (rdma_connect(outgoingCmId, &conn_param))
      throw std::runtime_error("Could not connect to RDMA endpoint");
    if (rdma_get_cm_event(outgoingChannel, &event))
      throw std::runtime_error("Rdma CM event failed");
    if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
      DEBUG_LOG("Retry with sleep");
      rdma_ack_cm_event(event);
      rdma_destroy_id(outgoingCmId);
      rdma_destroy_event_channel(outgoingChannel);
      delete rdmaContext;
      l.unlock();
      sleep(1);
      goto RETRY;
    };
    DEBUG_LOG("Connection established");
    rdma_ack_cm_event(event);
    exchangeRdmaInfo(outgoingCmId, mr, type, typeId, nodeId);
    rdmaContext->rkey = response->rkey;
    rdmaContext->type = response->type;
    rdmaContext->typeId = response->typeId;
    rdmaContext->nodeId = response->nodeId;
    // std::cout << " ****** client got RKEY "  << response->rkey << std::endl;
    outgoingIds.push_back(rdmaContext);
    outgoingCqs.push_back(clientCq);
    outgoingChannels.push_back(outgoingChannel);
    return *rdmaContext;
  }

  size_t getNumberIncomingConnections() { return numberConnectionsEstablished; }

  // attention no latch/ wait until all connections are finished
  std::vector<RdmaContext *> getIncomingConnections() {
    std::unique_lock<std::mutex> l(incomingMut);
    return incomingIds;
  };

  utils::SynchronizedMonotonicBufferRessource &getGlobalBuffer() { return mbr; }

  void exchangeInitialMesssage(RdmaContext &context,
                               INITIAL_MSG *initialMessage) {
    DEBUG_LOG("Exchanging Experimetn Infos");
    rdma::postSend(initialMessage, context, rdma::completion::signaled);
    int completions{0};
    ibv_wc wcs[2];

    while (completions != 2) {
      auto expected = 2 - completions; // prevents over polling the q and
                                       // draining it from next phase
      auto comp = pollCompletion(
          context.id->qp->send_cq, expected,
          wcs); // assumes that the completion qs are the same for send and recv
      for (int i = 0; i < comp; i++) {
        /* verify the completion status */
        if (wcs[i].status != IBV_WC_SUCCESS) {
          throw;
        }
        ensure(wcs[i].qp_num == context.id->qp->qp_num);
      }
      completions += comp;
    }
  }

private:
  struct RdmaInfo {
    uint32_t rkey; // remote key
    Type type;
    uint64_t typeId;
    NodeID nodeId;
    RdmaInfo(uint32_t rkey_, Type type_, uint64_t typeId_, NodeID nodeId_)
        : rkey(rkey_), type(type_), typeId(typeId_), nodeId(nodeId_) {}
  };

  struct ComSetupContext {
    RdmaInfo *response = nullptr;
    rdma_cm_id *id = nullptr;
  };

  uint16_t port;
  utils::SynchronizedMonotonicBufferRessource
      mbr; // can we chunk that buffer into sub buffers for clients?
  struct ibv_pd *pd;
  struct ibv_mr *mr;
  // handler section
  std::atomic<bool> running{false};
  std::thread handler;
  struct rdma_event_channel *incomingChannel;
  struct rdma_cm_id *incomingCmId;
  std::mutex incomingMut;
  std::atomic<bool> initialized{false};
  std::atomic<size_t> numberConnections{0};
  std::atomic<size_t> numberConnectionsEstablished{0};
  std::mutex outgoingMut;
  std::vector<RdmaContext *> outgoingIds;
  std::vector<ibv_cq *> outgoingCqs;
  std::vector<rdma_event_channel *> outgoingChannels;
  std::vector<RdmaContext *> incomingIds;

  void resolveAddr(rdma_event_channel *outgoingChannel,
                   rdma_cm_id *outgoingCmId, struct sockaddr_storage &sin) {
    if (sin.ss_family == AF_INET)
      ((struct sockaddr_in *)&sin)->sin_port = port;
    else
      ((struct sockaddr_in6 *)&sin)->sin6_port = port;

    auto ret =
        rdma_resolve_addr(outgoingCmId, nullptr, (struct sockaddr *)&sin, 2000);
    if (ret)
      throw;
    struct rdma_cm_event *event;
    ret = rdma_get_cm_event(outgoingChannel, &event);
    if (ret)
      throw;
    if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED)
      throw;
    rdma_ack_cm_event(event);
    DEBUG_LOG("Addr resolved");
    ret = rdma_resolve_route(outgoingCmId, 2000);
    if (ret)
      throw;
    ret = rdma_get_cm_event(outgoingChannel, &event);
    if (ret)
      throw;
    if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED)
      throw;
    rdma_ack_cm_event(event);
    DEBUG_LOG("Route Resolved");
  }

  // Helper Functions
  void bindHandler(struct sockaddr_storage &sin) {
    int ret;
    if (sin.ss_family == AF_INET)
      ((struct sockaddr_in *)&sin)->sin_port = port;
    else
      ((struct sockaddr_in6 *)&sin)->sin6_port = port;

    ret = rdma_bind_addr(incomingCmId, (struct sockaddr *)&sin);
    if (ret) {
      throw std::runtime_error("Could not bind to rdma device");
    }
    DEBUG_LOG("rdma_bind_addr successful");
    DEBUG_LOG("rdma_listen");
    ret = rdma_listen(incomingCmId, 3);
    if (ret) {
      throw std::runtime_error("Could not listen");
    }
  }

  // function to exchange rdma info including rkey
  // must ensure that before calling that an receive has been posted
  void exchangeRdmaInfo(rdma_cm_id *cmId, ibv_mr *mr, Type type,
                        uint64_t typeId, NodeID nodeId) {
    RdmaInfo *ownInfo = new (mbr.allocate(sizeof(RdmaInfo)))
        RdmaInfo(mr->rkey, type, typeId, nodeId);
    postSend(ownInfo, cmId->qp, mr, completion::signaled);
    // poll completion for IBV_WC_RECV and
    int completions{0};
    ibv_wc wcs[2];
    while (completions != 2) {
      auto expected = 2 - completions; // prevents over polling the q and
                                       // draining it from next phase
      auto comp = pollCompletion(
          cmId->qp->send_cq, expected,
          wcs); // assumes that the completion qs are the same for send and recv
      for (int i = 0; i < comp; i++) {
        /* verify the completion status */
        if (wcs[i].status != IBV_WC_SUCCESS) {
          throw;
        }
      }
      completions += comp;
    }
    DEBUG_LOG("Rdma Info Exchange");
  }

  // we only create one CQ for the handler and only send CQ
  struct ibv_cq *createCQ(rdma_cm_id *cmId) {
    struct ibv_cq *cq = ibv_create_cq(cmId->verbs, 16, nullptr, nullptr, 0);
    if (!cq)
      throw std::runtime_error("Could not create cq");
    DEBUG_LOG("CQ created");
    return cq;
  }

  // can we create a single one?
  void createPD(rdma_cm_id *cmId) {
    pd = ibv_alloc_pd(cmId->verbs);
    if (!pd)
      throw std::runtime_error("Could not create PD");
    DEBUG_LOG("PD created");
  }

  RdmaContext *createRdmaContext(rdma_cm_id *cmId,
                                 INITIAL_MSG *applicationData) {
    auto *rdmaContext = new RdmaContext();
    postReceive(applicationData, cmId->qp, mr);
    rdmaContext->applicationData = applicationData;
    rdmaContext->mr = mr;
    rdmaContext->id = cmId;
    return rdmaContext;
  }

  void createMR() {
    // std::cout << "memory created " << std::endl;
    DEBUG_VAR(mbr.getUnderlyingBuffer());
    mr = ibv_reg_mr(pd, mbr.getUnderlyingBuffer(), mbr.getBufferSize(),
                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                        IBV_ACCESS_REMOTE_WRITE);
    DEBUG_VAR(mr);
    if (!mr)
      throw std::runtime_error("Memory region could not be registered");

    DEBUG_LOG("Created Memory region");
    DEBUG_VAR(mr->lkey);
    DEBUG_VAR(mr->rkey);
  }
  // called with the correct cm_id
  void createQP(rdma_cm_id *cm_id, ibv_cq *completionQueue) {
    // std::cout << "Create QP " << std::endl;
    // std::cout << "cm_id " << cm_id << std::endl;
    // std::cout << "cq " << completionQueue << std::endl;
    // std::cout << "pd " << pd << std::endl;
    struct ibv_qp_init_attr init_attr;
    int ret;
    memset(&init_attr, 0, sizeof(init_attr));
    init_attr.cap.max_send_wr = 1024; // 4096;
    init_attr.cap.max_recv_wr = 1024; // 4096;
    init_attr.cap.max_recv_sge = 1;
    init_attr.cap.max_send_sge = 1;
#ifdef USE_INLINE
    init_attr.cap.max_inline_data = INLINE_SIZE;
#endif
    init_attr.qp_type = IBV_QPT_RC;
    init_attr.send_cq = completionQueue;
    init_attr.recv_cq = completionQueue;
    ret = rdma_create_qp(cm_id, pd, &init_attr);
    if (ret)
      throw std::runtime_error("Could not create QP " + std::to_string(ret) +
                               " errno " + std::to_string(errno));
    DEBUG_VAR(init_attr.cap.max_send_wr);
    DEBUG_VAR(init_attr.cap.max_recv_wr);
    DEBUG_VAR(init_attr.cap.max_recv_sge);
    DEBUG_VAR(init_attr.cap.max_send_sge);
    DEBUG_VAR(init_attr.cap.max_inline_data);
    DEBUG_LOG("QP created");
  }
  void getAddr(std::string ip, struct sockaddr *addr) {
    DEBUG_LOG("get_addr");
    struct addrinfo *res;
    auto ret = getaddrinfo(ip.c_str(), NULL, NULL, &res);
    if (ret) {
      throw std::runtime_error("getaddrinfo failed");
    }

    if (res->ai_family == PF_INET)
      memcpy(addr, res->ai_addr, sizeof(struct sockaddr_in));
    else if (res->ai_family == PF_INET6)
      memcpy(addr, res->ai_addr, sizeof(struct sockaddr_in6));
    else
      throw std::runtime_error("Unexpected  ai_family");
    freeaddrinfo(res);
  }
};
} // namespace rdma
} // namespace scalestore
