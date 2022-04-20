#include "WorkerCounters.hpp"
#include "../CounterRegistry.hpp"
// -------------------------------------------------------------------------------------
namespace scalestore {
namespace profiling {
// -------------------------------------------------------------------------------------
WorkerCounters::WorkerCounters(){
   CounterRegistry::getInstance().registerWorkerCounter(this);
}
// -------------------------------------------------------------------------------------
WorkerCounters::~WorkerCounters(){
   CounterRegistry::getInstance().deregisterWorkerCounter(this);
}
// -------------------------------------------------------------------------------------
}  // profiling
}  // scalestore
