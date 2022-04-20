#include "Partition.hpp"
namespace scalestore {
namespace storage {
Partition::Partition( u64 freeFramesSize, u64 freePagesSize, u64 freePIDsSize)
    : frameFreeList(freeFramesSize), pageFreeList(freePagesSize), pidFreeList(freePIDsSize)
{
}
}  // storage
}  // scalestore

