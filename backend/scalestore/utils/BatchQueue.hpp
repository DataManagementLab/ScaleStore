#pragma once
#include <random>
#include <algorithm>
namespace scalestore
{
namespace utils
{
// -------------------------------------------------------------------------------------
class SpinLock
{
   // TODO For the Exclusive-Spinlock, we use a 64-bit atomic, as we saw 2-3x better throughput in micro-benchmarks
   // compared to a single byte implementation
   alignas(64) std::atomic<bool> lock_ = {0};

  public:
   void lock() noexcept
   {
      for (;;) {
         // Optimistically assume the lock is free on the first try
         if (!lock_.exchange(true, std::memory_order_acquire)) {
            return;
         }
         // Wait for lock to be released without generating cache misses
         while (lock_.load(std::memory_order_relaxed)) {
            _mm_pause();
         }
      }
   }

   bool try_lock() noexcept
   {
      // First do a relaxed load to check if lock is free in order to prevent
      // unnecessary cache misses if someone does while(!try_lock())
      return !lock_.load(std::memory_order_relaxed) && !lock_.exchange(true, std::memory_order_acquire);
   }

   void unlock() noexcept { lock_.store(false, std::memory_order_release); }
};
// -------------------------------------------------------------------------------------
// containers
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
template <typename T, size_t N>
struct RingBuffer {
   [[nodiscard]] bool try_push(const T& e)
   {
      if (full())
         return false;
      buffer[mask(write++)] = e;
      return true;
   }

   [[nodiscard]] bool try_pop(T& e)
   {
      if (empty())
         return false;
      e = buffer[mask(read++)];
      return true;
   }

   bool empty() { return read == write; }
   bool full() { return get_size() == N; }
   uint64_t get_size() { return write - read; }
   void reset()
   {
      read = 0;
      write = 0;
   }

  private:
   constexpr uint64_t mask(uint64_t val) { return val & (N - 1); }
   // -------------------------------------------------------------------------------------
   uint64_t read = 0;
   uint64_t write = 0;
   std::array<T, N> buffer{};
   static_assert(N && ((N & (N - 1)) == 0), "N not power of 2");
};
// -------------------------------------------------------------------------------------
template <typename T, size_t N>
struct Stack {
   [[nodiscard]] bool try_push(const T& e)
   {
      assert(size <= N);
      if (full()) {
         return false;
      }
      buffer[size++] = e;
      return true;
   }

   [[nodiscard]] bool try_pop(T& e)
   {
      if (empty()) {
         return false;
      }
      e = buffer[--size];
      return true;
   }
   bool empty() { return size == 0; }
   bool full() { return size == (N); }
   uint64_t get_size() { return size; }
   void reset() { size = 0; }

  private:
   uint64_t size = 0;
   std::array<T, N> buffer{};
};
// -------------------------------------------------------------------------------------
template <typename T, size_t BatchSize, template <class, size_t> class Container>
struct alignas(64) Batch {
   Container<T, BatchSize> container;
   Batch* next = nullptr;
};
// -------------------------------------------------------------------------------------
template <typename T, size_t BatchSize, template <class, size_t> class Container>
struct BatchQueue {
   using B = Batch<T, BatchSize, Container>;
   static constexpr double oversubscription = 1.1;
   // -------------------------------------------------------------------------------------

   struct alignas(64) Metadata {
      B* head{nullptr};
      uint64_t size{0};  // number of batches inside the queue
   };
   // -------------------------------------------------------------------------------------
   alignas(64) std::atomic<uint64_t> elements{0};  // serves as guard variable for pop
   const size_t size;
   std::unique_ptr<B[]> batches;
   SpinLock latch;
   Metadata full;
   Metadata empty;
   // -------------------------------------------------------------------------------------
   BatchQueue(size_t size_) : size((size_ / BatchSize) * oversubscription), batches(std::make_unique<B[]>(size))
   {
      std::for_each(batches.get(), batches.get() + size, [&](auto& batch) { push_empty(&batch); });
   }
   // -------------------------------------------------------------------------------------
   void reset()
   {
      if (full.size == 0) {
         return;  // no elements to clean
      }
      assert(full.head);
      // reset elements
      B* full_batch{nullptr};
      while (try_pop_full(full_batch)) {
         full_batch->container.reset();
         auto rc = try_push_empty(full_batch);
         if (!rc)
            throw std::runtime_error("Cannot push to empty queue");
      }
      assert(full.size == 0);
      assert_no_duplicates();
   }
   void print_size()
   {
      std::cout << "e " << empty.size << "\n";
      std::cout << "f " << full.size << "\n";
      std::cout << "s" << size << std::endl;
   }
   // -------------------------------------------------------------------------------------
   void assert_no_leaks()
   {
      if ((empty.size + full.size) != size)
         throw std::runtime_error("Leaks detected");
   }

   inline void increment_elements(size_t inc_size)
   {
      auto tmp_e = elements.load();
      elements.store(tmp_e + inc_size, std::memory_order_release);
   };
   inline void decrement_elements(size_t dec_size)
   {
      auto tmp_e = elements.load();
      elements.store(tmp_e - dec_size, std::memory_order_release);
   };

   void assert_no_duplicates()
   {
      std::vector<uint64_t> histogram(size);
      auto idx = [&](B* batch) { return (batch - &batches[0]); };
      B* current = full.head;
      while (current) {
         histogram[idx(current)]++;
         current = current->next;
      }
      current = empty.head;
      while (current) {
         histogram[idx(current)]++;
         if (histogram[idx(current)] > 1) {
            throw std::runtime_error("found duplicate at idx " + std::to_string(idx(current)));
         }
         current = current->next;
      }
   }
   // -------------------------------------------------------------------------------------
   [[nodiscard]] bool try_push_full(B*& batch)
   {
      latch.lock();  // spinning lock as we want to allow inserts to complete
      batch->next = full.head;
      increment_elements(batch->container.get_size());
      full.head = batch;
      full.size++;
      latch.unlock();
      return true;
   }
   // -------------------------------------------------------------------------------------
   [[nodiscard]] bool try_push_empty(B*& batch)
   {
      latch.lock();  // spinning lock as we want to allow inserts to complete
      batch->next = empty.head;
      empty.head = batch;
      assert(batch->container.get_size() == 0);
      empty.size++;
      latch.unlock();
      return true;
   }
   // -------------------------------------------------------------------------------------
   [[nodiscard]] bool try_pop_empty(B*& batch)
   {
      latch.lock();
      // -------------------------------------------------------------------------------------
      if (!empty.head) {
         latch.unlock();
         return false;
      }
      // -------------------------------------------------------------------------------------
      batch = empty.head;
      empty.head = empty.head->next;
      assert(batch->container.get_size() == 0);
      empty.size--;
      latch.unlock();
      // -------------------------------------------------------------------------------------
      return true;
   }

   [[nodiscard]] bool push_full_and_try_pop_empty(B*& batch)
   {
      while (!latch.try_lock())
         ;  // spinning without backoff to reduce latency

      if (!empty.head) {
         latch.unlock();
         return false;
      }
      batch->next = full.head;
      increment_elements(batch->container.get_size());
      full.head = batch;
      full.size++;
      // pop empty
      batch = empty.head;
      empty.head = empty.head->next;
      assert(batch->container.get_size() == 0);
      empty.size--;
      latch.unlock();
      return true;
   }

   [[nodiscard]] bool push_empty_and_try_pop_full(B*& batch)
   {
      if (elements.load() == 0) {
         _mm_pause();  // todo remove to outside
         return false;
      }
      // -------------------------------------------------------------------------------------
      if (!latch.try_lock()) {
         _mm_pause();  // make outside
         return false;
      }
      // -------------------------------------------------------------------------------------
      if (!full.head) {
         latch.unlock();
         return false;
      }
      // push empty
      assert(batch->container.get_size() == 0);
      batch->next = empty.head;
      empty.head = batch;
      empty.size++;
      // pop full
      batch = full.head;
      full.head = full.head->next;
      decrement_elements(batch->container.get_size());
      full.size--;
      latch.unlock();
      return true;
   }

   // -------------------------------------------------------------------------------------
   [[nodiscard]] bool try_pop_full(B*& batch)
   {
      // -------------------------------------------------------------------------------------
      // check guard variable first to prevent contention of spinlock
      if (elements.load() == 0) {
         _mm_pause();  // todo remove to outside
         return false;
      }
      // -------------------------------------------------------------------------------------
      if (!latch.try_lock()) {
         _mm_pause();  // make outside
         return false;
      }
      // -------------------------------------------------------------------------------------
      if (!full.head) {
         latch.unlock();
         return false;
      }
      // -------------------------------------------------------------------------------------
      batch = full.head;
      full.head = full.head->next;
      decrement_elements(batch->container.get_size());
      full.size--;
      latch.unlock();
      return true;
   }
   // -------------------------------------------------------------------------------------
   B* pop_empty()
   {
      B* ret{nullptr};
      while (!try_pop_empty(ret))
         ;
      assert(ret->container.get_size() == 0);
      return ret;
   }
   // -------------------------------------------------------------------------------------
   B* pop_full()
   {
      B* ret{nullptr};
      while (!try_pop_full(ret))
         ;
      assert(ret->container.get_size() > 0);
      return ret;
   }
   // -------------------------------------------------------------------------------------
   void push_empty(B* batch)
   {
      assert(batch->container.get_size() == 0);
      while (!try_push_empty(batch))
         ;
   }
   // -------------------------------------------------------------------------------------
   void push_full(B* batch)
   {
      assert(batch->container.get_size() > 0);
      while (!try_push_full(batch))
         ;
   }
   // -------------------------------------------------------------------------------------
   uint64_t approx_size() { return elements.load(); }
};

}  // namespace utils
}  // namespace scalestore
