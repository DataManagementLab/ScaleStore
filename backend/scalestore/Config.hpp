#pragma once
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
// Buffermanager Config
// -------------------------------------------------------------------------------------
DECLARE_double(dramGB);
DECLARE_uint64(worker);
DECLARE_uint64(batchSize);
DECLARE_uint64(pageProviderThreads);
DECLARE_double(freePercentage);
DECLARE_uint64(coolingPercentage);
DECLARE_double(evictCoolestEpochs);
DECLARE_bool(csv); 
DECLARE_string(csvFile);
DECLARE_string(tag);
DECLARE_string(ssd_path);
DECLARE_bool(evict_to_ssd);
DECLARE_double(ssd_gib);
DECLARE_uint32(falloc);
DECLARE_uint64(prob_SSD);
DECLARE_uint32(partitionBits);
DECLARE_uint32(page_pool_partitions);
/// -------------------------------------------------------------------------------------
// CONTENTION
// -------------------------------------------------------------------------------------
DECLARE_bool(backoff);
// -------------------------------------------------------------------------------------
// RDMA Config
// -------------------------------------------------------------------------------------
DECLARE_uint64(nodes);
DECLARE_string(ownIp);
DECLARE_double(rdmaMemoryFactor); // factor to be multiplied by dramGB
DECLARE_uint32(port);
DECLARE_uint64(pollingInterval);
DECLARE_bool(read);
DECLARE_bool(random);
DECLARE_uint64(messageHandlerThreads);
DECLARE_uint64(messageHandlerMaxRetries);

// -------------------------------------------------------------------------------------
// Server Specific Part
// -------------------------------------------------------------------------------------
DECLARE_uint32(sockets);
DECLARE_uint32(socket);
DECLARE_bool(pinThreads);
DECLARE_bool(cpuCounters); 
