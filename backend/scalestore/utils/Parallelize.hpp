#pragma once
// -------------------------------------------------------------------------------------
#include "Defs.hpp"
#include "../threads/Concurrency.hpp"
// -------------------------------------------------------------------------------------
namespace scalestore {
namespace utils {

class Parallelize{

  public:
   template<typename F>
   static void parallelRange(uint64_t n, F function){
      parallelRange(std::thread::hardware_concurrency(), n,function);
   }

   template<typename F>
   static void parallelRange(int threads, uint64_t n, F function){
      ensure(threads > 0);
      concurrency::WorkerGroup g(threads);
      const uint64_t blockSize = n / threads;
      ensure(blockSize > 0);
      g.run([&](int workerId){
               auto begin = workerId * blockSize;
               auto end = begin + blockSize;
               if(workerId == threads - 1)
                  end = n;
               function(begin,end);
            });
      g.wait();
   }
};

}  // utils
}  // scalestore
