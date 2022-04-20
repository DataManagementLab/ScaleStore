#include "AsyncWriteBuffer.hpp"
// -------------------------------------------------------------------------------------
#include <signal.h>
#include <cstring>
// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace storage
{
// -------------------------------------------------------------------------------------
AsyncWriteBuffer::AsyncWriteBuffer(int fd, u64 page_size, u64 batch_max_size) : fd(fd), page_size(page_size), batch_max_size(batch_max_size), free_slots(batch_max_size)
{
   write_buffer = std::make_unique<Page[]>(batch_max_size);
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
bool AsyncWriteBuffer::full()
{
   if ((outstanding_ios + ready_to_submit) >= (int64_t)batch_max_size - 2) {
      return true;
   } else {
      return false;
   }
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::add(BufferFrame& bf, PID pid, uint64_t epoch_added)
{
   ensure(!full());
   ensure(u64(bf.page) % 512 == 0);
   ensure((outstanding_ios + ready_to_submit) <= (int64_t)batch_max_size);
   // -------------------------------------------------------------------------------------
   auto slot = free_slots.remove();
   auto slot_ptr = ready_to_submit++;
   write_buffer_commands[slot].bf = &bf;
   write_buffer_commands[slot].epoch_added = epoch_added;
   bf.page->magicDebuggingNumber = epoch_added;
  
   void* write_buffer_slot_ptr = bf.page;
   io_prep_pwrite(&iocbs[slot], fd, write_buffer_slot_ptr, page_size, page_size * pid.plainPID());
   iocbs[slot].data = write_buffer_slot_ptr;
   iocbs_ptr[slot_ptr] = &iocbs[slot];
}
// -------------------------------------------------------------------------------------
u64 AsyncWriteBuffer::submit()
{
   if (ready_to_submit > 0) {
      int ret_code = io_submit(aio_context, ready_to_submit, iocbs_ptr.get());
      ensure(ret_code == s32(ready_to_submit));
      outstanding_ios += ready_to_submit;
      ready_to_submit = 0;
      return ret_code;
   }
   return 0;
}
// -------------------------------------------------------------------------------------
u64 AsyncWriteBuffer::pollEventsSync()
{
   if (outstanding_ios > 0) {
      const int done_requests = io_getevents(aio_context, 1, outstanding_ios, events.get(), NULL);
      if(done_requests<0)
         throw std::runtime_error("io_getevents failed" + std::to_string(done_requests));
      outstanding_ios -= done_requests;
      ensure(outstanding_ios >=0);
      return done_requests;
   }
   return 0;
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::getWrittenBfs(std::function<void(BufferFrame&, uint64_t)> callback, u64 n_events)
{
   for (u64 i = 0; i < n_events; i++) {
      // const auto slot = (u64(events[i].data) - u64(write_buffer.get())) / page_size;
      const auto slot = ((iocb*)(events[i].obj)) - (((iocbs.get())));
      free_slots.add(slot);
      // -------------------------------------------------------------------------------------
      ensure(events[i].res == page_size);
      if(!(events[i].res2 == 0)){
         raise(SIGTRAP);
      }
      callback(*write_buffer_commands[slot].bf, write_buffer_commands[slot].epoch_added);
   }
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace scalestore
   // -------------------------------------------------------------------------------------
