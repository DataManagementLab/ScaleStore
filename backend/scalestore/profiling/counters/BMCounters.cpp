#include "BMCounters.hpp"
#include "../CounterRegistry.hpp"

namespace scalestore {
namespace profiling {


// -------------------------------------------------------------------------------------
BMCounters::BMCounters(storage::Buffermanager& bm) : bm(bm){
   CounterRegistry::getInstance().registerBMCounter(this);
}
// -------------------------------------------------------------------------------------
BMCounters::~BMCounters(){
   CounterRegistry::getInstance().deregisterBMCounter(this);
}

}  // profiling
}  // scalestore
