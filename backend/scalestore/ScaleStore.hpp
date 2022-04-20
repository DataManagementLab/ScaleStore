#pragma once
// -------------------------------------------------------------------------------------
#include "profiling/ProfilingThread.hpp"
#include "profiling/counters/BMCounters.hpp"
#include "profiling/counters/RDMACounters.hpp"
#include "rdma/CommunicationManager.hpp"
#include "rdma/MessageHandler.hpp"
#include "storage/buffermanager/BufferFrameGuards.hpp"
#include "storage/buffermanager/Buffermanager.hpp"
#include "storage/buffermanager/Catalog.hpp"
#include "storage/buffermanager/PageProvider.hpp"
#include "storage/datastructures/BTree.hpp"
#include "storage/datastructures/DistributedBarrier.hpp"
#include "storage/datastructures/Vector.hpp"
#include "threads/CoreManager.hpp"
#include "threads/WorkerPool.hpp"
// -------------------------------------------------------------------------------------
#include <memory>

namespace scalestore
{
// -------------------------------------------------------------------------------------
// avoids destruction of objects before remote side finished
struct RemoteGuard{
   std::atomic<uint64_t>& numberRemoteConnected;
   RemoteGuard(std::atomic<uint64_t>& numberRemoteConnected) : numberRemoteConnected(numberRemoteConnected){};
   ~RemoteGuard(){ while(numberRemoteConnected);}
};

class ScaleStore
{
  public:
   //! Default constructor
   ScaleStore();
   //! Destructor
   ~ScaleStore();
   // -------------------------------------------------------------------------------------
   // Deleted constructors
   //! Copy constructor
   ScaleStore(const ScaleStore& other) = delete;
   //! Move constructor
   ScaleStore(ScaleStore&& other) noexcept = delete;
   //! Copy assignment operator
   ScaleStore& operator=(const ScaleStore& other) = delete;
   //! Move assignment operator
   ScaleStore& operator=(ScaleStore&& other) noexcept = delete;
   // -------------------------------------------------------------------------------------
   storage::Buffermanager& getBuffermanager() { return *bm; }
   // -------------------------------------------------------------------------------------
   threads::WorkerPool& getWorkerPool() { return *workerPool; }
   // -------------------------------------------------------------------------------------
   rdma::CM<rdma::InitMessage>& getCM() { return *cm; }
   // -------------------------------------------------------------------------------------
   NodeID getNodeID() { return nodeId; }
   // -------------------------------------------------------------------------------------
   s32 getSSDFD() { return ssd_fd; }
   // -------------------------------------------------------------------------------------
   void startProfiler(profiling::WorkloadInfo& wlInfo) {
      pt.running = true;
      profilingThread.emplace_back(&profiling::ProfilingThread::profile, &pt, nodeId, std::ref(wlInfo), std::ref(*bm));
   }
   // -------------------------------------------------------------------------------------
   void stopProfiler()
   {
      if (pt.running == true) {
         pt.running = false;
         for (auto& p : profilingThread)
            p.join();
         profilingThread.clear();
      }
      std::locale::global(std::locale("C")); // hack to restore locale which is messed up in tabulate package
   };

   // -------------------------------------------------------------------------------------
   // Catalog
   // -------------------------------------------------------------------------------------
   storage::Catalog& getCatalog(){
      return *catalog;      
   }
   // -------------------------------------------------------------------------------------
   template<class Key, class Value>
   storage::BTree<Key,Value> createBTree(){
      storage::BTree<Key,Value> tree;
      // register into catalog
      catalog->insertCatalogEntry(storage::DS_TYPE::BTREE, tree.entryPage);
      return tree;
   }
   // -------------------------------------------------------------------------------------
   template<class T>
   storage::LinkedList<T> createLinkedList(){
      storage::LinkedList<T> list;
      catalog->insertCatalogEntry(storage::DS_TYPE::LLIST, list.head);
      return list;
   }
   // with data structure id 
   template<class T>
   storage::LinkedList<T> createLinkedList(uint64_t& ret_datastructureId){
      storage::LinkedList<T> list;
      ret_datastructureId =catalog->insertCatalogEntry(storage::DS_TYPE::LLIST, list.head);
      return list;
   }
   // -------------------------------------------------------------------------------------
   storage::DistributedBarrier createBarrier(uint64_t threads){
      storage::DistributedBarrier barrier(threads);
      catalog->insertCatalogEntry(storage::DS_TYPE::BARRIER, barrier.pid);
      return barrier;
   }

   
   
   // -------------------------------------------------------------------------------------
  private:
   NodeID nodeId = 0;
   s32 ssd_fd;
   std::unique_ptr<rdma::CM<rdma::InitMessage>> cm;
   std::unique_ptr<storage::Buffermanager> bm;
   std::unique_ptr<rdma::MessageHandler> mh;
   std::unique_ptr<storage::PageProvider> pp;
   std::unique_ptr<RemoteGuard> rGuard;
   std::unique_ptr<threads::WorkerPool> workerPool;
   std::unique_ptr<profiling::BMCounters> bmCounters;
   std::unique_ptr<profiling::RDMACounters> rdmaCounters;
   std::unique_ptr<storage::Catalog> catalog;
   profiling::ProfilingThread pt;
   std::vector<std::thread> profilingThread;
};
// -------------------------------------------------------------------------------------
}  // namespace scalestore
