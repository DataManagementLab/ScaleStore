#include "CPUCounters.hpp"
#include "../CounterRegistry.hpp"
// -------------------------------------------------------------------------------------
namespace scalestore {
namespace profiling {
// -------------------------------------------------------------------------------------
CPUCounters::CPUCounters(std::string name): threadName(name){
   if(FLAGS_cpuCounters){
      e = std::make_unique<PerfEvent>(false);
      CounterRegistry::getInstance().registerCPUCounter(this);
   }
}
// -------------------------------------------------------------------------------------
CPUCounters::~CPUCounters(){
   if(FLAGS_cpuCounters){
      CounterRegistry::getInstance().deregisterCPUCounter(this);
   }
}
// -------------------------------------------------------------------------------------
}  // profiling
}  // scalestore
