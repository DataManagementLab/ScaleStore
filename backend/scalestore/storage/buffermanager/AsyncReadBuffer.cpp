#include "AsyncReadBuffer.hpp"
// -------------------------------------------------------------------------------------
#include <signal.h>
#include <cstring>
// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace storage
{
// -------------------------------------------------------------------------------------
AsyncReadBuffer::AsyncReadBuffer(int fd, u64 page_size, u64 batch_max_size) : fd(fd), page_size(page_size), batch_max_size(batch_max_size), free_slots(batch_max_size)
{
   // write_buffer = std::make_unique<Page[]>(batch_max_size);
   write_buffer_commands = std::make_unique<WriteCommand[]>(batch_max_size);
   iocbs = std::make_unique<struct iocb[]>(batch_max_size);
   iocbs_ptr = std::make_unique<struct iocb*[]>(batch_max_size);
   events = std::make_unique<struct io_event[]>(batch_max_size);
   // -------------------------------------------------------------------------------------
   memset(&aio_context, 0, sizeof(aio_context));
   const int ret = io_setup(batch_max_size, &aio_context);
   if (ret != 0) {
      throw std::runtime_error(("io_setup failed, ret code = " + std::to_string(ret)));
   }
   // init free slots
   for(uint64_t s_i = 0; s_i < batch_max_size; ++s_i)
      free_slots.add(s_i);
}

// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace scalestore
   // -------------------------------------------------------------------------------------
