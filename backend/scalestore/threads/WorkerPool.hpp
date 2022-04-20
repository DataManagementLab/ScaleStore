#pragma once
// -------------------------------------------------------------------------------------
#include "Worker.hpp"
// -------------------------------------------------------------------------------------
#include <thread>
#include <atomic>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <functional>
// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace threads
{
// -------------------------------------------------------------------------------------
class WorkerPool
{
   static constexpr uint64_t MAX_WORKER_THREADS = 70;

   std::atomic<uint64_t> runningThreads = 0;
   std::atomic<bool> keepRunning = true;
   // -------------------------------------------------------------------------------------
   struct WorkerThread {
      std::mutex mutex;
      std::condition_variable cv;
      std::function<void()> job;
      bool wtReady = true;
      bool jobSet = false;
      bool jobDone = false;
   };
   // -------------------------------------------------------------------------------------
   std::vector<std::thread> workerThreads;
   std::vector<Worker*> workers;
   WorkerThread workerThreadsMeta [MAX_WORKER_THREADS];
   uint32_t workersCount;
  public:
   
   // -------------------------------------------------------------------------------------
   WorkerPool(rdma::CM<rdma::InitMessage>& cm, NodeID nodeId);
   ~WorkerPool();
   // -------------------------------------------------------------------------------------
   void scheduleJobAsync(uint64_t t_i, std::function<void()> job);
   void scheduleJobSync(uint64_t t_i, std::function<void()> job);
   void joinAll();
};
// -------------------------------------------------------------------------------------
}  // namespace threads
}  // namespace scalestore
