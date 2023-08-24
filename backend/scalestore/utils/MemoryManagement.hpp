#pragma once
// -------------------------------------------------------------------------------------
#include <sys/mman.h>
#include <cassert>
#include <iostream>
#include <mutex>
#include <memory_resource>
// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace utils
{
// -------------------------------------------------------------------------------------
// helper classes

// -------------------------------------------------------------------------------------

// RAII for huge pages
template <typename T>
class HugePages
{
   T* memory;
   size_t size; // in bytes
   size_t highWaterMark;  // max index
  public:
   HugePages(size_t size) : size(size)
   {
      void* p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
      if (p == MAP_FAILED)
         throw std::runtime_error("mallocHugePages failed");
      memory = static_cast<T*>(p);
      highWaterMark = (size / sizeof(T));
   }

   size_t get_size(){
      return highWaterMark;
   }
   
   inline operator T*() { return memory; }

   
   inline T& operator[](size_t index) const
   {
      return memory[index];
   }
   ~HugePages() { munmap(memory, size); }
};

// -------------------------------------------------------------------------------------
// similar to pmr monotonic buffer however does not grow
class MonotonicBufferResource : public std::pmr::memory_resource
{
  public:
   void* bufferPosition = nullptr;  // moves with new allocations
   size_t bufferSize;
   size_t sizeLeft;
   HugePages<uint8_t> buffer;

   MonotonicBufferResource(size_t bufferSize) : bufferSize(bufferSize), sizeLeft(bufferSize), buffer(bufferSize) {
      bufferPosition = buffer;
   };

   ~MonotonicBufferResource() {}
   void* getUnderlyingBuffer() { return buffer; }
   size_t getSizeLeft() { return sizeLeft; }
   size_t getBufferSize() { return bufferSize; }

  protected:
   void* do_allocate(size_t bytes, size_t alignment) override
   {
      if (sizeLeft < bytes)
         throw std::bad_alloc();
      if (std::align(alignment, bytes, bufferPosition, sizeLeft)) {
         void* result = bufferPosition;
         bufferPosition = (char*)bufferPosition + bytes;
         sizeLeft -= bytes;
         auto iptr = reinterpret_cast<std::uintptr_t>(result);
         if((iptr % alignment) != 0)
            throw std::runtime_error("Alignment failed");
         return result;
      }
      throw std::bad_alloc();
   }
   void do_deallocate(void* /*p*/, size_t /*bytes*/, size_t /*alignment*/) override
   {
      throw std::runtime_error(" Function not supported in monotonic increasing buffer");
   }
   bool do_is_equal(const memory_resource& other) const noexcept override { return this == &other; }
};

// thread safe monotonic memory resource
class SynchronizedMonotonicBufferRessource : public std::pmr::memory_resource
{
   MonotonicBufferResource parent;
   std::mutex allocteMut;

  public:
   SynchronizedMonotonicBufferRessource(size_t bufferSize) : parent(bufferSize){};
   void* getUnderlyingBuffer() { return parent.getUnderlyingBuffer(); }
   size_t getSizeLeft() { return parent.getSizeLeft(); }
   size_t getBufferSize() { return parent.getBufferSize(); }

  protected:
   void* do_allocate(size_t bytes, size_t alignment) override
   {
      std::unique_lock<std::mutex> allocLock(allocteMut);
      return parent.allocate(bytes, alignment);
   }
   void do_deallocate(void* /*p*/, size_t /*bytes*/, size_t /*alignment*/) override
   {
      throw std::runtime_error(" Function not supported in monotonic increasing buffer");
   }
   bool do_is_equal(const memory_resource& other) const noexcept override { return this == &other; }
};

// -------------------------------------------------------------------------------------
// wraps a thread local pointer to allow for proper destruction
template <typename T>
struct ThreadLocalWrapper {
   static_assert(std::is_pointer<T>::value, "Expected a pointer");
   T cached = nullptr;

   inline T operator->() { return cached; }
   inline operator T() { return cached; };

   inline T& operator=(const T& other)
   {
      cached = other;
      return cached;
   }
   ~ThreadLocalWrapper() { delete cached; }
};

}  // namespace utils
}  // namespace scalestore
