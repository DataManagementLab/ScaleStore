#pragma once
#include "../../storage/buffermanager/Buffermanager.hpp"
// -------------------------------------------------------------------------------------
#include <array>
#include <atomic>
// -------------------------------------------------------------------------------------

// forward declare

namespace scalestore
{
namespace storage
{
class Buffermanager;
}  // namespace storage
}  // namespace scalestore

// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace profiling
{
// -------------------------------------------------------------------------------------
struct BMCounters {
   // -------------------------------------------------------------------------------------
   enum {
      freePages,
      freeFrames,
      dramPoolSize,
      percUtilized,
      globalEpoch,
      consumedPages,
      COUNT,
   };
   // -------------------------------------------------------------------------------------
   static const constexpr inline std::array<std::string_view, COUNT> translation{{"free pages","free frames", "page pool", "perc. pages used", "global epoch","consumed pages"}};
   static_assert(translation.size() == COUNT);
   // -------------------------------------------------------------------------------------
   struct LOG_ENTRY{
      const std::string_view name;
      const LOG_LEVEL level;
   };

   static const constexpr inline std::array<LOG_ENTRY, COUNT> bmCounterLogLevel{{
       {"freePages", LOG_LEVEL::CSV},
       {"freeFrames", LOG_LEVEL::CSV},
       {"dramPoolSize", LOG_LEVEL::CSV},
       {"percUtilized", LOG_LEVEL::RELEASE},
       {"globalEpoch", LOG_LEVEL::RELEASE},
       {"consumedPages", LOG_LEVEL::RELEASE},
   }};
   // -------------------------------------------------------------------------------------
   BMCounters(storage::Buffermanager& bm);
   ~BMCounters();
   storage::Buffermanager& bm;
};
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace scalestore
