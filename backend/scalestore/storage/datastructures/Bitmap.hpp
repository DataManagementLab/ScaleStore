#pragma once
// -------------------------------------------------------------------------------------
#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <optional>
#include "scalestore/utils/RandomGenerator.hpp"
#include "scalestore/utils/MemoryManagement.hpp"
// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace storage
{
// -------------------------------------------------------------------------------------
template <size_t SIZE>
class StaticBitmap
{
   static constexpr size_t ARRAY_LEN = ((SIZE >> 6));  // does not consume space
  public:
   uint64_t bitmap[(SIZE >> 6)];
   // -------------------------------------------------------------------------------------
   StaticBitmap()
   {
      ensure(SIZE % 64 == 0);
      for (size_t i = 0; i < ARRAY_LEN; i++) {
         bitmap[i] = 0;
      }
   }
   // -------------------------------------------------------------------------------------
   size_t getSize() { return SIZE; }
   // -------------------------------------------------------------------------------------
   void set(size_t bitPostition)
   {
      auto pos = (bitPostition >> 6);
      auto bit = bitPostition & 63;
      bitmap[pos] |= (1UL << bit);
   }
   // -------------------------------------------------------------------------------------
   void reset(size_t bitPostition)
   {
      auto pos = (bitPostition >> 6);
      auto bit = bitPostition & 63;
      bitmap[pos] &= ~(1UL << bit);
   }
   // -------------------------------------------------------------------------------------
   void reset()
   {
      for (size_t k = 0; k < ARRAY_LEN; ++k) {
         bitmap[k] = 0;
      }
   }
   // -------------------------------------------------------------------------------------
   bool test(size_t bitPostition)
   {
      auto pos = (bitPostition >> 6);
      auto bit = bitPostition & 63;
      return (bitmap[pos] & (1UL << bit));
   }
   // -------------------------------------------------------------------------------------
   size_t bitmap_decode_ctz(uint32_t* out)
   {
      size_t pos = 0;
      uint64_t bitset;
      for (size_t k = 0; k < ARRAY_LEN; ++k) {
         bitset = bitmap[k];
         while (bitset != 0) {
            uint64_t t = bitset & -bitset;
            int r = __builtin_ctzll(bitset);
            out[pos++] = k * 64 + r;
            bitset ^= t;
         }
      }
      return pos;
   }
   // -------------------------------------------------------------------------------------
   bool none()
   {
      for (size_t k = 0; k < ARRAY_LEN; ++k) {
         if (bitmap[k] != 0)
            return false;
      }
      return true;
   }
   // -------------------------------------------------------------------------------------
   bool any() { return !(none()); }
   // -------------------------------------------------------------------------------------
   template <typename F>
   size_t applyForAll(F&& func)
   {
      size_t pos = 0;
      uint64_t bitset;
      for (size_t k = 0; k < ARRAY_LEN; ++k) {
         bitset = bitmap[k];
         while (bitset != 0) {
            uint64_t t = bitset & -bitset;
            int r = __builtin_ctzll(bitset);
            func(k * 64 + r);
            bitset ^= t;
         }
      }
      return pos;
   }
   // -------------------------------------------------------------------------------------
   template <typename F>
   void applyToOne(F&& func)
   {
      uint64_t bitset;
      for (size_t k = 0; k < ARRAY_LEN; ++k) {
         bitset = bitmap[k];
         while (bitset != 0) {
            int r = __builtin_ctzll(bitset);
            func(k * 64 + r);
            return;
         }
      }
   }
   // -------------------------------------------------------------------------------------
   size_t getHighestBitSet()
   {
      size_t answ;
      size_t pos = 0;
      uint64_t bitset;
      for (size_t k = 0; k < ARRAY_LEN; ++k) {
         bitset = bitmap[k];
         while (bitset != 0) {
            uint64_t t = bitset & -bitset;
            int r = __builtin_ctzll(bitset);
            answ = k * 64 + r;
            bitset ^= t;
         }
      }
      return answ;
   }
   // -------------------------------------------------------------------------------------
   size_t count() const
   {
      size_t answ = 0;
      for (size_t k = 0; k < ARRAY_LEN; ++k) {
         answ += __builtin_popcountll(bitmap[k]);
      }
      return answ;
   }
   // -------------------------------------------------------------------------------------
   std::optional<size_t> getFreeSlot()
   {
      uint64_t bitset = 0;
      for (size_t k = 0; k < ARRAY_LEN; ++k) {
         while (bitmap[k] != 0xFFFFFFFFFFFFFFFF) {
            bitset = ~(bitmap[k]);
            int r = __builtin_ctzll(bitset);
            return std::optional<size_t>{k * 64 + r};
         }
      }
      return std::nullopt;
   }
};
// -------------------------------------------------------------------------------------
// specialization for 64 use case
// implicit conversion and init
// -------------------------------------------------------------------------------------
template <>
class StaticBitmap<64>
{
  public:
   uint64_t bitmap;
   // -------------------------------------------------------------------------------------
   StaticBitmap(uint64_t bitmap) : bitmap(bitmap) {}
   StaticBitmap() { bitmap = 0;}
   // -------------------------------------------------------------------------------------
   operator uint64_t(){return bitmap;}
   // -------------------------------------------------------------------------------------
   size_t getSize() { return 64; }
   // -------------------------------------------------------------------------------------
   void set(size_t bitPostition)
   {
      auto bit = bitPostition & 63;
      bitmap |= (1UL << bit);
   }
   // -------------------------------------------------------------------------------------
   void reset(size_t bitPostition)
   {
      auto bit = bitPostition & 63;
      bitmap &= ~(1UL << bit);
   }
   // -------------------------------------------------------------------------------------
   void reset()
   {
         bitmap = 0;
   }
   // -------------------------------------------------------------------------------------
   bool test(size_t bitPostition)
   {
      auto bit = bitPostition & 63;
      return (bitmap & (1UL << bit));
   }
   // -------------------------------------------------------------------------------------
   size_t bitmap_decode_ctz(uint32_t* out)
   {
      size_t pos = 0;
      uint64_t bitset;
      bitset = bitmap;
      while (bitset != 0) {
         uint64_t t = bitset & -bitset;
         int r = __builtin_ctzll(bitset);
         out[pos++] =+ r;
         bitset ^= t;
      }
      
      return pos;
   }
   // -------------------------------------------------------------------------------------
   bool none()
      {
         if (bitmap != 0)
            return false;
         return true;
      }
   // -------------------------------------------------------------------------------------
   bool any() { return !(none()); }
   // -------------------------------------------------------------------------------------
   template <typename F>
   size_t applyForAll(F&& func)
      {
         size_t pos = 0;
         uint64_t bitset;
         bitset = bitmap;
         while (bitset != 0) {
            uint64_t t = bitset & -bitset;
            int r = __builtin_ctzll(bitset);
            func(r);
            bitset ^= t;
         }
      
      return pos;
   }

   template <typename F>
   void applyToOneRnd(F&& func)
   {
      uint64_t cnt = count();
      if (cnt == 1) {
         return applyToOne(func);
      }
      // -------------------------------------------------------------------------------------
      uint64_t pick = utils::RandomGenerator::getRandU64(0, cnt - 1);
      uint64_t bitset;
      bitset = bitmap;
      uint64_t idx = 0;
      while (bitset != 0) {
         if (idx == pick) {
            int r = __builtin_ctzll(bitset);
            func(r);
            return;
         }
         uint64_t t = bitset & -bitset;
         bitset ^= t;
         idx++;
      }
      // -------------------------------------------------------------------------------------
   }
   // -------------------------------------------------------------------------------------
   template <typename F>
   void applyToOne(F&& func)
   {
      uint64_t bitset;

      bitset = bitmap;
      while (bitset != 0) {
         int r = __builtin_ctzll(bitset);
         func(r);
         return;
      }
      
   }
   // -------------------------------------------------------------------------------------
   size_t getHighestBitSet()
      {
         size_t answ;
         uint64_t bitset;
         bitset = bitmap;
         while (bitset != 0) {
            uint64_t t = bitset & -bitset;
            int r = __builtin_ctzll(bitset);
            answ = r;
            bitset ^= t;
         }
      
      return answ;
   }
   // -------------------------------------------------------------------------------------
   size_t count() const
   {
      size_t answ = 0;
      answ += __builtin_popcountll(bitmap);
      return answ;
   }
   // -------------------------------------------------------------------------------------
   std::optional<size_t> getFreeSlot()
   {
      uint64_t bitset = 0;

         while (bitmap != 0xFFFFFFFFFFFFFFFF) {
            bitset = ~(bitmap);
            int r = __builtin_ctzll(bitset);
            return std::optional<size_t>{r};
         }
      
      return std::nullopt;
   }
};
// -------------------------------------------------------------------------------------
class Bitmap
{
  public:
   uint64_t* bitmap{nullptr};
   size_t size;
   size_t sizeBits;
   // -------------------------------------------------------------------------------------
   Bitmap(const size_t sizeBits) : sizeBits(sizeBits)
   {
      ensure(sizeBits % 64 == 0);
      size = (sizeBits >> 6);
      bitmap = new size_t[size];
      for (size_t i = 0; i < size; i++) {
         bitmap[i] = 0;
      }
   }
   // -------------------------------------------------------------------------------------
   ~Bitmap()
   {
      if (bitmap != nullptr)
         delete[] bitmap;
   }
   // -------------------------------------------------------------------------------------
   size_t getSize() { return sizeBits; }
   // -------------------------------------------------------------------------------------
   void set(size_t bitPostition)
   {
      auto pos = (bitPostition >> 6);
      auto bit = bitPostition & 63;
      bitmap[pos] |= (1UL << bit);
   }
   // -------------------------------------------------------------------------------------

   void reset(size_t bitPostition)
   {
      auto pos = (bitPostition >> 6);
      auto bit = bitPostition & 63;
      bitmap[pos] &= ~(1UL << bit);
   }
   // -------------------------------------------------------------------------------------
   bool test(size_t bitPostition)
   {
      auto pos = (bitPostition >> 6);
      auto bit = bitPostition & 63;
      return (bitmap[pos] & (1UL << bit));
   }
   // -------------------------------------------------------------------------------------
   size_t bitmap_decode_ctz(uint32_t* out)
   {
      size_t pos = 0;
      uint64_t bitset;
      for (size_t k = 0; k < size; ++k) {
         bitset = bitmap[k];
         while (bitset != 0) {
            uint64_t t = bitset & -bitset;
            int r = __builtin_ctzll(bitset);
            out[pos++] = k * 64 + r;
            bitset ^= t;
         }
      }
      return pos;
   }
   // -------------------------------------------------------------------------------------
   template <typename F>
   size_t applyForAll(F& func)
   {
      size_t pos = 0;
      uint64_t bitset;
      for (size_t k = 0; k < size; ++k) {
         bitset = bitmap[k];
         while (bitset != 0) {
            uint64_t t = bitset & -bitset;
            int r = __builtin_ctzll(bitset);
            func(k * 64 + r);
            bitset ^= t;
         }
      }
      return pos;
   }
   // -------------------------------------------------------------------------------------
   size_t getHighestBitSet()
   {
      size_t answ;
      uint64_t bitset;
      for (size_t k = 0; k < size; ++k) {
         bitset = bitmap[k];
         while (bitset != 0) {
            uint64_t t = bitset & -bitset;
            int r = __builtin_ctzll(bitset);
            answ = k * 64 + r;
            bitset ^= t;
         }
      }
      return answ;
   }
   // -------------------------------------------------------------------------------------
   size_t count() const
   {  // count number of set bits

      size_t answ = 0;
      for (size_t k = 0; k < size; ++k) {
         answ += __builtin_popcountll(bitmap[k]);
      }

      return answ;
   }
   // -------------------------------------------------------------------------------------
   std::optional<size_t> getFreeSlot()
   {
      uint64_t bitset = 0;
      for (size_t k = 0; k < size; ++k) {
         while (bitmap[k] != 0xFFFFFFFFFFFFFFFF) {
            bitset = ~(bitmap[k]);
            int r = __builtin_ctzll(bitset);
            return std::optional<size_t>{k * 64 + r};
         }
      }
      return std::nullopt;
   }
   // -------------------------------------------------------------------------------------
};
// -------------------------------------------------------------------------------------
class AtomicBitmap
{
  public:
   size_t sizeBits;
   size_t size;
   scalestore::utils::HugePages<std::atomic<uint64_t>> bitmap;
   // -------------------------------------------------------------------------------------
   AtomicBitmap(const size_t sizeBits) : sizeBits(sizeBits), size((sizeBits >> 6)), bitmap(size * sizeof(std::atomic<uint64_t>))
   {
      for (size_t i = 0; i < size; i++) {
         bitmap[i] = 0;
      }
   }
   
   // -------------------------------------------------------------------------------------
   size_t getSize() { return sizeBits; }
   // -------------------------------------------------------------------------------------
   void set(size_t bitPostition)
   {
      auto pos = (bitPostition >> 6);
      auto bit = bitPostition & 63;
      bitmap[pos].fetch_or((1UL << bit));
   }
   // -------------------------------------------------------------------------------------
   size_t count() const
   {  // count number of set bits

      size_t answ = 0;
      for (size_t k = 0; k < size; ++k) {
         answ += __builtin_popcountll(bitmap[k]);
      }

      return answ;
   }
   // -------------------------------------------------------------------------------------
   void reset(size_t bitPostition)
   {
      auto pos = (bitPostition >> 6);
      auto bit = bitPostition & 63;
      bitmap[pos].fetch_and(~(1UL << bit));
   }
   // -------------------------------------------------------------------------------------
   bool test(size_t bitPostition)
   {
      auto pos = (bitPostition >> 6);
      auto bit = bitPostition & 63;
      return (bitmap[pos] & (1UL << bit));
   }
   // -------------------------------------------------------------------------------------

   template <typename F>
   void applyToOneRnd(F&& func)
   {
      while (true) {
         // -------------------------------------------------------------------------------------
         uint64_t pick = utils::RandomGenerator::getRandU64(0, sizeBits - 1);
         uint64_t bitset = bitmap[pick >> 6];
         uint64_t idx = pick;
         while (bitset != 0) {
            uint64_t r = __builtin_ctzll(bitset);
            r = r + (64 * (pick >> 6));
            if (r >= pick) {
               func(r);
               return;
            }
            uint64_t t = bitset & -bitset;
            bitset ^= t;
            idx++;
         }
      }
      // -------------------------------------------------------------------------------------
   }
      
   // -------------------------------------------------------------------------------------
   template <typename F>
   size_t applyForAll(F&& func, uint64_t startIdx, uint64_t blocks) // start idx in uint64_t not bit and blocks as well
    {
      size_t pos = 0;
      uint64_t bitset;
      ensure(startIdx+blocks < size);
      for (size_t k = startIdx; k < startIdx + blocks; ++k) {
         bitset = bitmap[k].load(std::memory_order_relaxed);
         while (bitset != 0) {
            uint64_t t = bitset & -bitset;
            int r = __builtin_ctzll(bitset);
            func(k * 64 + r);
            bitset ^= t;
         }
      }
      return pos;
   }
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace scalestore
