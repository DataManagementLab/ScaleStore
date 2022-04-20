#pragma once
#include <numeric>
// -------------------------------------------------------------------------------------
namespace scalestore {
namespace threads {
// -------------------------------------------------------------------------------------
// Very Simplistic Core Manager to pin threads on OUR Severs
// with that kind of cpu mappings
// node 0 cpus: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 28 29 30 31 32 33 34 35 36 37 38 39 40 41
// -------------------------------------------------------------------------------------
struct CoreManager{
   // vector of cpuids

   
   std::vector<uint64_t> cores;
   std::vector<uint64_t> hts;

   static CoreManager& getInstance(){
      static CoreManager coremanager;
      return coremanager;
   }

   bool pinThreadToHT(pthread_t thread){
      ensure(hts.size() > 0);
      uint64_t id = *hts.begin();
      hts.erase(hts.begin());
      schedAffinity(id, thread);
      return true;
   }
   bool pinThreadToCore(pthread_t thread){
      ensure(cores.size() > 0);
      uint64_t id = *cores.begin();
      cores.erase(cores.begin());
      schedAffinity(id, thread);
      return true;
   }
   
   bool pinThread(pthread_t thread){
      if(cores.size() != 0)
         return pinThreadToCore(thread);
      else if (hts.size() != 0)
         return pinThreadToHT(thread);
      else
         throw std::runtime_error(" Cannot pin thread");
   }

   bool pinThreadRoundRobin(pthread_t thread){

      if((cores.size() == 0) &&  (hts.size() == 0)){
         throw std::runtime_error(" Cannot pin thread");
      }      
      if(cores.size() >= hts.size())
         return pinThreadToCore(thread);
      else  
         return pinThreadToHT(thread);
   }

   
  private:
   bool schedAffinity(uint64_t id, pthread_t thread) {
      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);
      CPU_SET(id, &cpuset);
      if (pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset) != 0) {
         throw std::runtime_error("Could not pin thread " + std::to_string(id) + " to thread " +
                                  std::to_string(id));
      }
      return true;
   }
   
   CoreManager(){
      uint64_t count = std::thread::hardware_concurrency();
      uint64_t socketCount = count / FLAGS_sockets;
      uint64_t pCores = socketCount / 2;
      uint64_t firstCPUId = pCores * FLAGS_socket;
      uint64_t firstHTId = pCores * (FLAGS_sockets + FLAGS_socket);
      cores.resize(pCores);
      hts.resize(pCores); 
      std::iota(std::begin(cores), std::end(cores), firstCPUId);
      std::iota(std::begin(hts), std::end(hts), firstHTId);
    }
   
};

}  // threads
}  // scalestore
