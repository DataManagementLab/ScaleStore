#pragma once
#include <cassert>
#include <utility>
#include <cstdint>
#include <vector>
#include <algorithm>
#include <list>
#include <numa.h>
namespace scalestore {
namespace storage {

template<class K, class V>
struct SmallMap {
   struct Entry {
      std::pair<K, V> kv;
      uint16_t next;
   };

   unsigned mask;
   std::vector<Entry> entries;
   std::vector<uint16_t> ht;
   uint16_t freeList;

   SmallMap(unsigned htCount, unsigned entryCount) {
      ensure(htCount <= 1<<16);
      entries.resize(1);
      entries.reserve(entryCount + 1);
      ht.resize(htCount);
      mask = htCount - 1;
      freeList = 0;
   }

   static uint64_t hashKey(uint64_t k) {
      //MurmurHash64A
      const uint64_t m = 0xc6a4a7935bd1e995;
      const int r = 47;
      uint64_t h = 0x8445d61a4e774912 ^ (8*m);
      k *= m;
      k ^= k >> r;
      k *= m;
      h ^= k;
      h *= m;
      h ^= h >> r;
      h *= m;
      h ^= h >> r;
      return h;
   }

   void insert(std::pair<K, V>&& kv) {
      uint16_t& pos = ht[hashKey(kv.first) & mask];
      if (freeList) {
         Entry& e = entries[freeList];
         e.kv = kv;
         uint16_t oldPos = freeList;
         freeList = e.next;
         e.next = pos;
         pos = oldPos;
      } else {
         uint16_t oldPos = pos;
         pos = entries.size();
         entries.push_back({kv, oldPos});
      }
   }

   void erase(K key) {
      uint16_t* pos = &ht[hashKey(key) & mask];
      while (*pos) {
         Entry& e = entries[*pos];
         if (e.kv.first == key) {
            unsigned oldPos = *pos;
            *pos = e.next;
            e.next = freeList;
            freeList = oldPos;
            return;
         }
         pos = &e.next;
      }
   }

   std::pair<K, V>* find(K key) {
      unsigned pos = ht[hashKey(key) & mask];
      while (pos) {
         Entry& e = entries[pos];
         if (e.kv.first == key)
            return &e.kv;
         pos = e.next;
      }
      return nullptr;
   }

   std::pair<K, V>* end() {
      return nullptr;
   }
};


}  // storage
}  // scalestore
