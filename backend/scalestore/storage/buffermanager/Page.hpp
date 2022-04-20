#pragma once
// -------------------------------------------------------------------------------------
#include <cstdint>
#include <cassert>
// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace storage
{
// -------------------------------------------------------------------------------------
// constexpr uint64_t PAGE_SIZE = 1024 * 1024 * 2;
// constexpr uint64_t PAGE_SIZE = 1024 * 256 * 1;
constexpr uint64_t PAGE_SIZE = 1024 * 4;
// -------------------------------------------------------------------------------------
struct alignas(512) Page {
   volatile std::size_t magicDebuggingNumber = 999; // INIT used for RDMA debugging
   uint8_t data[PAGE_SIZE - sizeof(magicDebuggingNumber)];
   uint8_t* begin() { return data; }
   uint8_t* end() { return data + (PAGE_SIZE - sizeof(magicDebuggingNumber)) + 1; } // one byte past the end
   template <typename T>
   T& getTuple(uint64_t idx)
   {
      uint8_t* ptr = data + idx;
      ensure((ptr + sizeof(T)) < end()); // CHECK
      return *(reinterpret_cast<T*>(ptr));
   }
};
// -------------------------------------------------------------------------------------
static constexpr uint64_t EFFECTIVE_PAGE_SIZE = sizeof(Page::data);
// -------------------------------------------------------------------------------------
}  // namespace storage

}  // namespace scalestore


