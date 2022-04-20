#pragma once
// -------------------------------------------------------------------------------------
#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <string>
// -------------------------------------------------------------------------------------

namespace scalestore {
namespace profiling {
// -------------------------------------------------------------------------------------
// wrapper to perfevent for nameing convention
struct CPUCounters {
   std::string threadName;
   std::unique_ptr<PerfEvent> e;

   CPUCounters(std::string name);
   ~CPUCounters();
};
// -------------------------------------------------------------------------------------
}  // profiling
}  // scalestore
