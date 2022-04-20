#pragma once
#include "../../storage/buffermanager/Buffermanager.hpp"
#include "Defs.hpp"
// -------------------------------------------------------------------------------------
#include <array>
#include <atomic>
// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace profiling
{
// -------------------------------------------------------------------------------------
struct RDMACounters {
   // -------------------------------------------------------------------------------------

   struct RDMAEventFunctor {
      std::ifstream counterFile;
      std::string path;
      size_t value = 0;
      size_t prevValue  = 0; 
      RDMAEventFunctor(RDMAEventFunctor&& ref) = default;
      RDMAEventFunctor(std::string counterFile) : path(counterFile){};
      ~RDMAEventFunctor(){};

      // calculates difference between calls and returns in bytes
      std::size_t operator()()
      {
         
         counterFile.open(path);
         counterFile >> value;
         counterFile.close();

         size_t bytes = value * 4; // 4 lanes
         double diff  = (bytes -prevValue);
         prevValue = bytes;         
         return diff;
      }
   };
   // -------------------------------------------------------------------------------------
   enum {
      sentGB,
      recvGB,
      COUNT,
   };
   // -------------------------------------------------------------------------------------
   static const constexpr inline std::array<std::string_view, COUNT> translation{{"sent (GB)", "received (GB)"}};
   static_assert(translation.size() == COUNT);
   // -------------------------------------------------------------------------------------

   double getSentGB();
   double getRecvGB();

   RDMACounters();
   ~RDMACounters();


   double prevSent = 0;
   double prevRecv = 0;
   
   RDMAEventFunctor rdmaRecv;
   RDMAEventFunctor rdmaSent;
};
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace scalestore
