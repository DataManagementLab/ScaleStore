#include "../Config.hpp"
#include "WorkerPool.hpp"
#include "CoreManager.hpp"
// -------------------------------------------------------------------------------------
#include <cassert>
// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace threads
{
// -------------------------------------------------------------------------------------

WorkerPool::WorkerPool(rdma::CM<rdma::InitMessage>& cm, NodeID nodeId): workers(MAX_WORKER_THREADS,nullptr)
{
   workersCount = FLAGS_worker;
   ensure(workersCount < MAX_WORKER_THREADS);
   workerThreads.reserve(workersCount);
   for (uint64_t t_i = 0; t_i < workersCount; t_i++) {
      workerThreads.emplace_back([&, t_i]() {
         std::string threadName("worker_" + std::to_string(t_i));
         pthread_setname_np(pthread_self(), threadName.c_str());
         // -------------------------------------------------------------------------------------
         workers[t_i] = new Worker(t_i, threadName, cm, nodeId, rdma::Type::WORKER);
         Worker::tlsPtr = workers[t_i];
         // -------------------------------------------------------------------------------------
         runningThreads++;
         auto& meta = workerThreadsMeta[t_i];
         while (keepRunning) {
            std::unique_lock guard(meta.mutex);
            meta.cv.wait(guard, [&]() { return keepRunning == false || meta.jobSet; });
            if (!keepRunning) {
               break;
            }
            meta.wtReady = false;
            meta.job();
            meta.wtReady = true;
            meta.jobDone = true;
            meta.jobSet = false;
            meta.cv.notify_one();
         }
         runningThreads--;
      });
   }


   
   if(FLAGS_pinThreads){
      for (auto& t : workerThreads) {
         threads::CoreManager::getInstance().pinThreadRoundRobin(t.native_handle());
            // threads::CoreManager::getInstance().pinThreadToCore(t.native_handle());
      }
   }

   
   for (auto& t : workerThreads) {
      t.detach();
   }
   // -------------------------------------------------------------------------------------
   // Wait until all worker threads are initialized
   while (runningThreads < workersCount) {
   }
}
// -------------------------------------------------------------------------------------
WorkerPool::~WorkerPool(){
   keepRunning = false;
   
   for (uint64_t t_i = 0; t_i < workersCount; t_i++) {
      workerThreadsMeta[t_i].cv.notify_one();
   }
   while (runningThreads) {
   }

   for(auto& w : workers)
      if(w) delete w;

}
// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------
void WorkerPool::scheduleJobSync(uint64_t t_i, std::function<void()> job)
{
   ensure(t_i < workersCount);
   auto& meta = workerThreadsMeta[t_i];
   std::unique_lock guard(meta.mutex);
   meta.cv.wait(guard, [&]() { return !meta.jobSet && meta.wtReady; });
   meta.jobSet = true;
   meta.jobDone = false;
   meta.job = job;
   guard.unlock();
   meta.cv.notify_one();
   guard.lock();
   meta.cv.wait(guard, [&]() { return meta.jobDone; });
}
// -------------------------------------------------------------------------------------
void WorkerPool::scheduleJobAsync(uint64_t t_i, std::function<void()> job)
{
   ensure(t_i < workersCount);
   auto& meta = workerThreadsMeta[t_i];
   std::unique_lock guard(meta.mutex);
   meta.cv.wait(guard, [&]() { return !meta.jobSet && meta.wtReady; });
   meta.jobSet = true;
   meta.jobDone = false;
   meta.job = job;
   guard.unlock();
   meta.cv.notify_one();
}
// -------------------------------------------------------------------------------------
void WorkerPool::joinAll()
{
   for (uint64_t t_i = 0; t_i < workersCount; t_i++) {
      auto& meta = workerThreadsMeta[t_i];
      std::unique_lock guard(meta.mutex);
      meta.cv.wait(guard, [&]() { return meta.wtReady && !meta.jobSet; });
   }
}

}  // namespace threads
}  // namespace scalestore
