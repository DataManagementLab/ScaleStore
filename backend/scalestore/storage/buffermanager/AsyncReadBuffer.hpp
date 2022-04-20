#pragma once
#include "BufferFrame.hpp"
#include "Defs.hpp"
#include "scalestore/storage/datastructures/RingBuffer.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <libaio.h>

#include <functional>
#include <list>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace storage
{
// -------------------------------------------------------------------------------------
class AsyncReadBuffer
{
  private:
   struct WriteCommand {
      BufferFrame* bf;
      PID pid;
      uint64_t client_slot; // message handler specific
      bool recheck_msg = false;
   };
   io_context_t aio_context;
   int fd;
   u64 page_size, batch_max_size;
   u64 pending_requests = 0;
   u64 ready_to_submit = 0;
   u64 outstanding_ios = 0;


#define AIO_RING_MAGIC 0xa10a10a1
struct aio_ring {
	unsigned id; /** kernel internal index number */
	unsigned nr; /** number of io_events */
	unsigned head;
	unsigned tail;

	unsigned magic;
	unsigned compat_features;
	unsigned incompat_features;
	unsigned header_length; /** size of aio_ring */

	struct io_event events[0];
};

/* Stolen from kernel arch/x86_64.h */
#ifdef __x86_64__
#define read_barrier() __asm__ __volatile__("lfence" ::: "memory")
#else
#ifdef __i386__
#define read_barrier() __asm__ __volatile__("" : : : "memory")
#else
#define read_barrier() __sync_synchronize()
#endif
#endif

   /* Code based on axboe/fio:
    * https://github.com/axboe/fio/blob/702906e9e3e03e9836421d5e5b5eaae3cd99d398/engines/libaio.c#L149-L172
    */
   inline static int own_io_getevents(io_context_t ctx,
                                      long min_nr,
                                      long max_nr,
                                      struct io_event* events,
                                      struct timespec* timeout)
   {
      int i = 0;

      struct aio_ring* ring = (struct aio_ring*)ctx;
      if (ring == NULL || ring->magic != AIO_RING_MAGIC) {
         goto do_syscall;
      }

      while (i < max_nr) {
         unsigned head = ring->head;
         if (head == ring->tail) {
            /* There are no more completions */
            return i;
            // break;
         } else {
            /* There is another completion to reap */
            events[i] = ring->events[head];
            read_barrier();
            ring->head = (head + 1) % ring->nr;
            i++;
         }
      }

      if (i == 0 && timeout != NULL && timeout->tv_sec == 0 && timeout->tv_nsec == 0) {
         /* Requested non blocking operation. */
         return 0;
      }

      if (i >= min_nr) {
         return i;
      }

   do_syscall:
      return syscall(__NR_io_getevents, ctx, min_nr - i, max_nr - i, &events[i], timeout);
   }

   public:
   std::unique_ptr<Page[]> write_buffer;
   std::unique_ptr<WriteCommand[]> write_buffer_commands;
   std::unique_ptr<struct iocb[]> iocbs;
   std::unique_ptr<struct iocb*[]> iocbs_ptr;
   std::unique_ptr<struct io_event[]> events;
   RingBuffer<uint64_t> free_slots;
   // -------------------------------------------------------------------------------------
   // Debug
   // -------------------------------------------------------------------------------------
   AsyncReadBuffer(int fd, u64 page_size, u64 batch_max_size);
   // Caller takes care of sync
   // bool full();
   // void add(BufferFrame& bf, PID pid, uint64_t client_slot, bool recheck_msg);
   // u64 submit();
   // u64 pollEventsSync();
   // void getReadBfs(std::function<void(BufferFrame&, uint64_t, bool)> callback, u64 n_events);

   // -------------------------------------------------------------------------------------
   inline bool full()
   {
      if ((outstanding_ios + ready_to_submit) >= batch_max_size - 2) {
         return true;
      } else {
         return false;
      }
   }
   // -------------------------------------------------------------------------------------
   inline void add(BufferFrame& bf, PID pid, uint64_t client_slot, bool recheck_msg)
   {
      ensure(!full());
      ensure(u64(bf.page) % 512 == 0);
      ensure((outstanding_ios + ready_to_submit) <= batch_max_size);
      // -------------------------------------------------------------------------------------
      auto slot = free_slots.remove();
      auto slot_ptr = ready_to_submit++;
      write_buffer_commands[slot].bf = &bf;
      write_buffer_commands[slot].pid = pid;
      write_buffer_commands[slot].client_slot = client_slot;
      write_buffer_commands[slot].recheck_msg = recheck_msg;
      bf.page->magicDebuggingNumber = pid;

      void* write_buffer_slot_ptr = bf.page;
      io_prep_pread(&iocbs[slot], fd, write_buffer_slot_ptr, page_size, page_size * pid.plainPID());
      iocbs[slot].data = write_buffer_slot_ptr;
      iocbs_ptr[slot_ptr] = &iocbs[slot];
   }
   // -------------------------------------------------------------------------------------
   inline u64 submit()
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
   inline  u64 pollEventsSync()
   {
      if (outstanding_ios > 0) {
         const int done_requests = own_io_getevents(aio_context, outstanding_ios, outstanding_ios, events.get(), NULL);
         if (done_requests < 0)
            throw std::runtime_error("io_getevents failed" + std::to_string(done_requests));
         ensure(outstanding_ios >= (uint64_t)done_requests);
         outstanding_ios -= (uint64_t)done_requests;
         return done_requests;
      }
      return 0;
   }
   // -------------------------------------------------------------------------------------
   inline void getReadBfs(std::function<void(BufferFrame&, uint64_t, bool)> callback, u64 n_events)
   {
      for (u64 i = 0; i < n_events; i++) {
         // const auto slot = (u64(events[i].data) - u64(write_buffer.get())) / page_size;
         const auto slot = ((iocb*)(events[i].obj)) - (((iocbs.get())));
         free_slots.add(slot);
         // -------------------------------------------------------------------------------------
         ensure(events[i].res == page_size);
         if (!(events[i].res2 == 0)) {
            raise(SIGTRAP);
         }
         callback(*write_buffer_commands[slot].bf, write_buffer_commands[slot].client_slot,
                  write_buffer_commands[slot].recheck_msg);
      }
   }
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace scalestore
// -------------------------------------------------------------------------------------
