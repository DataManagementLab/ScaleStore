#pragma once
#include "scalestore/Config.hpp"
#include "Defs.hpp"
#include <immintrin.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>
// -------------------------------------------------------------------------------------
#include <atomic>
#include <cassert>
#include <optional>
#include <shared_mutex>
// -------------------------------------------------------------------------------------
#define RESTART(condition, label) if(condition) goto label;
// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace storage
{
// -------------------------------------------------------------------------------------
using Version = size_t;
// -------------------------------------------------------------------------------------
/* Optimistic Latch:
 * Used in every hashtable bucket, preferred over HybridLatch due to size
 */

struct OptimisticLatch {
   std::atomic<uint64_t> typeVersionLockObsolete{0b100};

   bool isLatched(uint64_t version) { return ((version & 0b10) == 0b10); }
   bool isLatched() { return ((typeVersionLockObsolete & 0b10) == 0b10); }

   std::optional<Version> optimisticLatchOrRestart()
   {
      Version version;
      version = typeVersionLockObsolete.load();
      if (isLatched(version)) {
         return std::nullopt;
      }
      return {version};
   }

   bool tryLatchExclusive()
   {
      auto version = optimisticLatchOrRestart();
      if (!version.has_value())
         return false;
      if (!upgradeToExclusiveLatch(version.value()))
         return false;
      return true;
   }

   bool upgradeToExclusiveLatch(uint64_t& version)
   {
      if (isLatched(version)) {
         return false;
      }

      if (typeVersionLockObsolete.compare_exchange_strong(version, version + 0b10)) {
         version = version + 0b10;
      } else {
         return false;
      }
      return true;
   }

   uint64_t downgradeToOptimisticLatch()
   {
      uint64_t version = typeVersionLockObsolete + 0b10;
      unlatchExclusive();
      return version;
   }

   void unlatchExclusive()
   {
      ensure(isLatched());
      Version v = typeVersionLockObsolete.load();
      v += 0b10;
      typeVersionLockObsolete.store(v, std::memory_order_release);
   }

   bool checkOrRestart(uint64_t startRead) const { return optimisticUnlatch(startRead);}
   bool optimisticUnlatch(uint64_t startRead) const { return (startRead == typeVersionLockObsolete.load()); }
   void writeUnlockObsolete() { typeVersionLockObsolete.fetch_add(0b11); }
};
// -------------------------------------------------------------------------------------
/* HybridLatch Latch:
 * Similar to Optimistic Latche but backed by std::shared_mutex to allow efficient spinning
 */
struct alignas(64) HybridLatch {
   std::atomic<Version> version{0b100};  // last bit reserved
   std::shared_mutex smut;               // test recursive

   Version getVersion(){return version.load();}
   
   // vector of tid
   void latchShared()
   {
      smut.lock_shared();
      ensure(!isLatched());
   }
   void unlatchShared() { smut.unlock_shared(); }

   void latchExclusive()
   {
      smut.lock();
      Version v = version.load();
      v += 0b10;
      version.store(v, std::memory_order_release);
      ensure(isLatched());
   }
   void unlatchExclusive()
   {
      ensure(isLatched());
      Version v = version.load();
      v += 0b10;
      version.store(v, std::memory_order_release);
      ensure(!isLatched());
      smut.unlock();
   }

   bool tryDowngradeExclusiveToShared(){
      ensure(isLatched());
      Version v = version.load();
      v += 0b10;
      version.store(v, std::memory_order_release);
      ensure(!isLatched());
      smut.unlock();
      // -------------------------------------------------------------------------------------
      latchShared();
      // -------------------------------------------------------------------------------------
      if (v != version.load()) {
         unlatchShared();
         return false;
      }
      return true;
   }
   
   bool tryLatchExclusive()
   {
      if (!smut.try_lock()) {
         return false;
      }
      Version v = version.load();
      v += 0b10;
      version.store(v, std::memory_order_release);
      return true;
   }

   bool tryLatchShared()
   {
      auto success = smut.try_lock_shared();
      return success;
   }

   
   uint64_t downgradeExclusiveToOptimistic()
   {
      uint64_t v = version + 0b10;
      unlatchExclusive();
      return v;
   }


   uint64_t downgradeSharedToOptimistic()
   {
      uint64_t v = version;
      unlatchShared();
      return v;
   }

   
   bool isLatched(Version version_) { return ((version_ & 0b10) == 0b10); }

   bool isLatched() { return ((version & 0b10) == 0b10); }

   bool optimisticUpgradeToShared(Version startRead)
   {
      Version v = version.load();
      if (isLatched(v))
         return false;
      if (v != startRead)
         return false;
      if (!tryLatchShared())
         return false;
      if (version.load() == startRead) {
         // ensure same version
         return true;  // latch upgraded
      }
      unlatchShared();
      return false;  // not successfull
   }

   bool optimisticUpgradeToExclusive(Version startRead)
   {
      Version v = version.load();
      if (v != startRead)
         return false;
      bool success = tryLatchExclusive();  // not successfull
      if(!success)
         return false;

      if(startRead + 0b10 != version){
         ensure(isLatched());
         unlatchExclusive();
         return false;
      }
      ensure(isLatched());
      return true;
   }

   // implement optimistic latching as well
   std::optional<Version> optimisticLatchOrRestart()
   {
      Version v;
      v = version.load();
      if (isLatched(v)) {
         return std::nullopt;
      }
      return {v};
   }

   bool optimisticReadUnlatchOrRestart(Version startRead) { return (startRead == version.load()); }
   bool optimisticCheckOrRestart(Version version) { return optimisticReadUnlatchOrRestart(version); }

   template <typename F, class... Args>
   bool optimisticReadUnlatchOrRestart(Version version, F& failureCallback, Args&&... args)
   {
      if (!optimisticReadUnlatchOrRestart(version)) {
         failureCallback(args...);
         return false;
      }
      return true;
   }
};

}  // namespace storage
}  // namespace scalestore
