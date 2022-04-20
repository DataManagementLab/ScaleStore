#include "RDMACounters.hpp"
#include "../CounterRegistry.hpp"
// -------------------------------------------------------------------------------------

namespace scalestore {
namespace profiling {

RDMACounters::RDMACounters(): rdmaRecv(rdmaPathRecv), rdmaSent(rdmaPathXmit){
   CounterRegistry::getInstance().registerRDMACounter(this);
};
// -------------------------------------------------------------------------------------
RDMACounters::~RDMACounters(){
   CounterRegistry::getInstance().deregisterRDMACounter(this);
}
// -------------------------------------------------------------------------------------
double RDMACounters::getSentGB(){   
   return (rdmaSent() / (double)(1024*1024*1024));   
}
// -------------------------------------------------------------------------------------
double RDMACounters::getRecvGB(){
   return (rdmaRecv() / (double)(1024*1024*1024));
}
}  // profiling
}  // scalestore
