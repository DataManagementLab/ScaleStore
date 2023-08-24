#pragma once
// -------------------------------------------------------------------------------------
#include "Defs.hpp"
#include "scalestore/Config.hpp"
#include "scalestore/storage/buffermanager/BufferFrameGuards.hpp"
// -------------------------------------------------------------------------------------
#include <optional>
#include <functional>
#include <experimental/source_location>
// -------------------------------------------------------------------------------------
namespace scalestore {
namespace storage {

// -------------------------------------------------------------------------------------
enum class PageType : uint8_t { BTreeInner = 1, BTreeLeaf = 2 };
static constexpr uint64_t pageSize = EFFECTIVE_PAGE_SIZE;
// -------------------------------------------------------------------------------------
template <class Key>
struct FenceKey {
   bool isInfinity = true;
   Key key;
};
template <class Key>
std::ostream& operator<<(std::ostream& os, const FenceKey<Key>& c) {
   os << "is inf. " << ((c.isInfinity) ? "yes" : "no") << " " << c.key;
   return os;
}
template <class Key>
struct FenceKeys {
   FenceKey<Key> lower;  // exclusive
   FenceKey<Key> upper;  // inclusive

   // -------------------------------------------------------------------------------------
   bool isLowerInfinity() { return lower.isInfinity; }
   bool isUpperInfinity() { return upper.isInfinity; }
   FenceKey<Key> getLower() { return lower; }
   FenceKey<Key> getUpper() { return upper; }
   // -------------------------------------------------------------------------------------
};
// -------------------------------------------------------------------------------------
struct NodeBase {
   PageType type;
   uint16_t count;
};
// -------------------------------------------------------------------------------------
// Leaf
// -------------------------------------------------------------------------------------
struct BTreeLeafBase : public NodeBase {
   static const PageType typeMarker = PageType::BTreeLeaf;
};
// -------------------------------------------------------------------------------------
template <class Key, class Payload>
struct BTreeLeaf : public BTreeLeafBase {
   using FK = FenceKeys<Key>;
   // -------------------------------------------------------------------------------------
   // iterators
   // -------------------------------------------------------------------------------------
   struct asc_iterator {
      BTreeLeaf& leaf;
      uint64_t current_pos = 0;

      asc_iterator(BTreeLeaf& leaf) : leaf(leaf), current_pos(0){};
      asc_iterator(BTreeLeaf& leaf, uint64_t pos) : leaf(leaf), current_pos(pos){};

      asc_iterator& operator++() {
         current_pos++;
         return *this;
      }

      uint64_t operator*() const { return current_pos; }

      friend bool operator==(const asc_iterator& lhs, const asc_iterator& rhs) {
         return (&lhs.leaf == &rhs.leaf && lhs.current_pos == rhs.current_pos);
      }
      friend bool operator!=(const asc_iterator& lhs, const asc_iterator& rhs) { return !(lhs == rhs); }
   };
   // -------------------------------------------------------------------------------------
   struct desc_iterator {
      BTreeLeaf& leaf;
      uint64_t current_pos = 0;

      desc_iterator(BTreeLeaf& leaf) : leaf(leaf), current_pos(0){};
      desc_iterator(BTreeLeaf& leaf, uint64_t pos) : leaf(leaf), current_pos(pos){};

      desc_iterator& operator++() {
         current_pos--;
         return *this;
      }

      uint64_t operator*() const { return current_pos; }

      friend bool operator==(const desc_iterator& lhs, const desc_iterator& rhs) {
         return (&lhs.leaf == &rhs.leaf && lhs.current_pos == rhs.current_pos);
      }
      friend bool operator!=(const desc_iterator& lhs, const desc_iterator& rhs) { return !(lhs == rhs); }
   };
   desc_iterator rbegin() { return desc_iterator(*this, count); }
   desc_iterator rend() { return desc_iterator(*this, 0); }
   asc_iterator begin() { return asc_iterator(*this, 0); }
   asc_iterator end() { return asc_iterator(*this, count); }
   // -------------------------------------------------------------------------------------
   static const uint64_t maxEntries = (pageSize - sizeof(NodeBase) - sizeof(FK)) / (sizeof(Key) + sizeof(Payload));
   // -------------------------------------------------------------------------------------
   static const uint64_t underflowSize = maxEntries / 4;
   // -------------------------------------------------------------------------------------
   FK fenceKeys;
   Key keys[maxEntries];
   Payload payloads[maxEntries];

   BTreeLeaf() {
      count = 0;
      type = typeMarker;
   }

   bool isFull() { return count == maxEntries; }
   bool isUnderflow(){return count <= underflowSize;}
   // -------------------------------------------------------------------------------------
   void setFences(FenceKey<Key> lower, FenceKey<Key> upper) {
      fenceKeys.lower = lower;
      fenceKeys.upper = upper;
   }
   // -------------------------------------------------------------------------------------
   // finds the first key which matches k if exists
   // otherwise returns next larger key
   // meaning a key which is not less than k
   unsigned lowerBound(Key k) {
      unsigned lower = 0;
      unsigned upper = count;
      do {
         unsigned mid = ((upper - lower) / 2) + lower;
         if (k < keys[mid]) {
            upper = mid;
         } else if (k > keys[mid]) {
            lower = mid + 1;
         } else {
            return mid;
         }
      } while (lower < upper);
      return lower;
   }

   unsigned lowerBoundBF(Key k) {
      auto base = keys;
      unsigned n = count;
      while (n > 1) {
         const unsigned half = n / 2;
         base = (base[half] < k) ? (base + half) : base;
         n -= half;
      }
      return (*base < k) + base - keys;
   }

   bool remove(Key k) {
      if (count) {
         unsigned pos = lowerBound(k);
         if ((pos < count) && (keys[pos] == k)) {
            memmove(keys + pos, keys + pos + 1, sizeof(Key) * (count - (pos + 1)));
            memmove(payloads + pos, payloads + pos + 1, sizeof(Payload) * (count - (pos + 1)));
            count--;
            return true;
         }
      }
      return false;
   }

   void insert(Key k, Payload p) {
      assert(count < maxEntries);
      if (count) {
         unsigned pos = lowerBound(k);
         if ((pos < count) && (keys[pos] == k)) {
            // Upsert
            payloads[pos] = p;
            return;
         }
         memmove(keys + pos + 1, keys + pos, sizeof(Key) * (count - pos));
         memmove(payloads + pos + 1, payloads + pos, sizeof(Payload) * (count - pos));
         keys[pos] = k;
         payloads[pos] = p;
      } else {
         keys[0] = k;
         payloads[0] = p;
      }
      count++;
   }

   PID split(Key& sep) {
      // PRE CONDITION: parent and current node need to be write locked
      // freshly created node can be unlocked here as it will be published later
      // -------------------------------------------------------------------------------------
      ensure(count == maxEntries);
      // -------------------------------------------------------------------------------------
      ExclusiveBFGuard xg_leaf;
      auto& newLeaf = xg_leaf.allocate<BTreeLeaf>();
      newLeaf.count = count - (count / 2);
      count = count - newLeaf.count;
      memcpy(newLeaf.keys, keys + count, sizeof(Key) * newLeaf.count);
      memcpy(newLeaf.payloads, payloads + count, sizeof(Payload) * newLeaf.count);
      sep = keys[count - 1];
      newLeaf.setFences({.isInfinity = false, .key = sep}, fenceKeys.getUpper());  // order is important
      setFences(fenceKeys.getLower(), {.isInfinity = false, .key = sep});
      return xg_leaf.getFrame().pid;
   }
};
// -------------------------------------------------------------------------------------
// Inner
// -------------------------------------------------------------------------------------
struct BTreeInnerBase : public NodeBase {
   static const PageType typeMarker = PageType::BTreeInner;
};

template <class Key>
struct BTreeInner : public BTreeInnerBase {
   using FK = FenceKeys<Key>;
   // -------------------------------------------------------------------------------------
   static const uint64_t maxEntries = (pageSize - sizeof(NodeBase) - sizeof(FK)) / (sizeof(Key) + sizeof(NodeBase*));
   // -------------------------------------------------------------------------------------
   FK fenceKeys;
   Key keys[maxEntries];
   PID children[maxEntries];

   BTreeInner() {
      count = 0;
      type = typeMarker;
   }

   bool isFull() { return count == (maxEntries - 1); };
   // -------------------------------------------------------------------------------------
   void setFences(FenceKey<Key> lower, FenceKey<Key> upper) {
      fenceKeys.lower = lower;
      fenceKeys.upper = upper;
   }

   // finds the first key which is larger than k
   unsigned upperBound(Key k) {
      unsigned lower = 0;
      unsigned upper = count;
      do {
         unsigned mid = ((upper - lower) / 2) + lower;
         if (k < keys[mid]) {
            upper = mid;
         } else if (k >= keys[mid]) {
            lower = mid + 1;
         } else {
            return mid;
         }
      } while (lower < upper);
      return lower;
   }

   // finds the first key which matches k if exists
   // otherwise returns next larger key
   // meaning a key which is not less than k
   unsigned lowerBound(Key k) {
      unsigned lower = 0;
      unsigned upper = count;
      do {
         unsigned mid = ((upper - lower) / 2) + lower;
         if (k < keys[mid]) {
            upper = mid;
         } else if (k > keys[mid]) {
            lower = mid + 1;
         } else {
            return mid;
         }
      } while (lower < upper);
      return lower;
   }

   unsigned lowerBoundBF(Key k) {
      auto base = keys;
      unsigned n = count;
      while (n > 1) {
         const unsigned half = n / 2;
         base = (base[half] < k) ? (base + half) : base;
         n -= half;
      }
      return (*base < k) + base - keys;
   }

   PID split(Key& sep) {
      // pre parent and current node need to be write locked
      // freshly created node can be unlocked here as it will be published later
      // -------------------------------------------------------------------------------------
      ExclusiveBFGuard xg_inner;
      auto& newInner = xg_inner.allocate<BTreeInner>();
      newInner.count = count - (count / 2);
      count = count - newInner.count - 1;
      sep = keys[count];
      memcpy(newInner.keys, keys + count + 1, sizeof(Key) * (newInner.count + 1));
      memcpy(newInner.children, children + count + 1, sizeof(NodeBase*) * (newInner.count + 1));
      newInner.setFences({.isInfinity = false, .key = sep}, fenceKeys.getUpper());  // order is important otherwise fence overwritten
      setFences(fenceKeys.getLower(), {.isInfinity = false, .key = sep});
      //-------------------------------------
      return xg_inner.getFrame().pid;
   }

   void insert(Key k, PID child) {
      ensure(count < maxEntries - 1);
      unsigned pos = lowerBound(k);
      memmove(keys + pos + 1, keys + pos, sizeof(Key) * (count - pos + 1));
      memmove(children + pos + 1, children + pos, sizeof(size_t) * (count - pos + 1));
      keys[pos] = k;
      children[pos] = child;
      std::swap(children[pos], children[pos + 1]);
      count++;
   }
   bool remove(uint64_t pos) {
      if (count) {
         if ((pos < count)) {                 
            memmove(keys + pos, keys + pos+1, sizeof(Key) * (count - (pos+1)));
            memmove(children + pos + 1, children + pos + 2, sizeof(NodeBase*) * (count - (pos + 2) +1));
            count--;
            return true;
         }
      }
      return false;
   }
};
// -------------------------------------------------------------------------------------
template <class Key, class Value>
struct BTree {
   PID entryPage;
   struct BTreeEntry {
      PID root = EMPTY_PID;
      uint64_t height = 0;
   };
   // -------------------------------------------------------------------------------------
   BTree() {
      // -------------------------------------------------------------------------------------
      // create entry point for BTree
      ExclusiveBFGuard xg_entryPage;
      auto& entry = xg_entryPage.allocate<BTreeEntry>();
      entryPage = xg_entryPage.getFrame().pid;
      // -------------------------------------------------------------------------------------
      // create root
      ExclusiveBFGuard xg_root;
      xg_root.allocate<BTreeLeaf<Key, Value>>();
      entry.root = xg_root.getFrame().pid;
      entry.height = 0;
   };
   // -------------------------------------------------------------------------------------
   BTree(PID entryPage) : entryPage(entryPage){};
   // -------------------------------------------------------------------------------------
   void makeRoot(Key k, PID left, PID right, ExclusiveBFGuard& g_entry) {
      ensure(g_entry.getFrame().latch.isLatched());
      // -------------------------------------------------------------------------------------
      // new root
      ExclusiveBFGuard g_root;
      auto& root = g_root.allocate<BTreeInner<Key>>();
      // -------------------------------------------------------------------------------------
      // test
      g_root.getFrame().epoch = EMPTY_EPOCH-1;
      // -------------------------------------------------------------------------------------
      ensure(root.type == PageType::BTreeInner);
      // -------------------------------------------------------------------------------------
      root.count = 1;
      root.keys[0] = k;
      root.children[0] = left;
      root.children[1] = right;
      // -------------------------------------------------------------------------------------
      // update entry point
      // -------------------------------------------------------------------------------------
      auto& entry = g_entry.as<BTreeEntry>(0);
      entry.root = g_root.getFrame().pid;
      entry.height = entry.height + 1;
   }

   // -------------------------------------------------------------------------------------
   // OLC for inner and leaf is write / latched
   // -------------------------------------------------------------------------------------
   void insert(Key k, Value v) {
      // -------------------------------------------------------------------------------------
      threads::Worker::my().counters.incr(profiling::WorkerCounters::btree_traversals);
      // -------------------------------------------------------------------------------------
      using Inner = BTreeInner<Key>;
      using Leaf = BTreeLeaf<Key, Value>;
   restart:
      threads::Worker::my().counters.incr(profiling::WorkerCounters::btree_restarted);
      OptimisticBFGuard g_parent(entryPage);
      // -------------------------------------------------------------------------------------
      auto* entry =  g_parent.asPtr<BTreeEntry>(0);
      if (g_parent.retry())
         goto restart;
      ensure(entry);
      PID currentPID = entry->root;
      uint64_t height = entry->height;
      uint64_t currentLevel = 0;
      NodeBase* node = nullptr;
      // -------------------------------------------------------------------------------------
      while (currentLevel < height) {
         // get current node from PID
         OptimisticBFGuard g_node(currentPID);
         if (g_parent.retry())
            goto restart;
         node = g_node.asPtr<NodeBase>(0);
         if (g_node.retry())
            goto restart;  // check inner
         ensure(node->type == PageType::BTreeInner);
         auto& inner = *reinterpret_cast<Inner*>(node);
         // -------------------------------------------------------------------------------------
         // split
         // -----------------------------------------------------------------------------------
         if (inner.isFull()) {
            ExclusiveBFGuard xg_parent(std::move(g_parent));
            if (xg_parent.retry())
               goto restart;
            ExclusiveBFGuard xg_node(std::move(g_node));
            if (xg_node.retry())
               goto restart;
            Key sep;
            PID newInner = inner.split(sep);
            if (xg_parent.getFrame().pid == entryPage) {
               makeRoot(sep, g_node.getFrame().pid, newInner, xg_parent);
            } else {
               xg_parent.as<Inner>(0).insert(sep, newInner);
            }
            goto restart;
         }
         // -------------------------------------------------------------------------------------
         ensure(g_node.g.latchState != LATCH_STATE::EXCLUSIVE);
         // -------------------------------------------------------------------------------------
         if (g_parent.retry())
            goto restart;
         currentPID = inner.children[inner.lowerBoundBF(k)];
         if (g_node.retry())
            goto restart;  // check inner
         g_parent = std::move(g_node);
         ensure(g_node.g.latchState != LATCH_STATE::EXCLUSIVE);
         // -------------------------------------------------------------------------------------
         currentLevel++;
      }
      // -------------------------------------------------------------------------------------
      // Leaf
      // -------------------------------------------------------------------------------------
      ExclusiveBFGuard xg_node(currentPID);
      if (xg_node.retry())
         goto restart;
      ensure(xg_node.getFrame().latch.isLatched());
      // -------------------------------------------------------------------------------------
      node = xg_node.asPtr<NodeBase>(0);
      ensure(node->type == PageType::BTreeLeaf);
      // -------------------------------------------------------------------------------------
      auto& leaf = *reinterpret_cast<Leaf*>(node);
      if (g_parent.retry()) goto restart;
      // -------------------------------------------------------------------------------------
      // Split Leaf
      // -------------------------------------------------------------------------------------
      if (leaf.isFull()) {
         ExclusiveBFGuard xg_parent(std::move(g_parent));
         if (xg_parent.retry()) goto restart;
         // -------------------------------------------------------------------------------------
         Key sep;
         PID newLeaf = leaf.split(sep);
         if (xg_parent.getFrame().pid == entryPage) { 
            // make new root
            makeRoot(sep, xg_node.getFrame().pid, newLeaf, xg_parent);
         } else {
            xg_parent.as<Inner>(0).insert(sep, newLeaf);
         }
         goto restart;
      }
      // -------------------------------------------------------------------------------------
      leaf.insert(k, v);
   }

   // -------------------------------------------------------------------------------------
   // latches the leaf exclusive and removes the key
   // -------------------------------------------------------------------------------------
   bool remove(Key k) {
      using Inner = BTreeInner<Key>;
      using Leaf = BTreeLeaf<Key, Value>;
   restart:
      OptimisticBFGuard g_parent(entryPage);
      // -------------------------------------------------------------------------------------
      auto* entry = g_parent.asPtr<BTreeEntry>(0);
      if (g_parent.retry()) goto restart;
      ensure(entry);
      PID rootPid = entry->root;
      OptimisticBFGuard g_node(rootPid);
      if (g_parent.retry()) goto restart;
      // -------------------------------------------------------------------------------------
      // Inner
      // -------------------------------------------------------------------------------------
      auto* node = g_node.asPtr<NodeBase>(0);
      if (g_node.retry()) goto restart;  // check inner
      // -------------------------------------------------------------------------------------
      while (node->type == PageType::BTreeInner) {
         auto& inner = *reinterpret_cast<Inner*>(node);
         // -------------------------------------------------------------------------------------
         if (g_parent.retry()) goto restart;

         PID nextPid = inner.children[inner.lowerBound(k)];
         if (g_node.retry()) goto restart;  // check inner

         g_parent = std::move(g_node);
         g_node = OptimisticBFGuard(nextPid);
         node = g_node.asPtr<NodeBase>(0);
         if (g_node.retry()) goto restart;  // check inner
      }
      // -------------------------------------------------------------------------------------
      // Leaf
      // -------------------------------------------------------------------------------------
      auto& leaf = *reinterpret_cast<Leaf*>(node);
      // merge leaf if underfull
      // check if we have a parent and if leaf is underflow
      if(leaf.isUnderflow() && (g_parent.getFrame().pid != entryPage)) {
         ExclusiveBFGuard xg_parent(std::move(g_parent));
         if (xg_parent.retry()) goto restart;
         ExclusiveBFGuard xg_node(std::move(g_node));
         if (xg_node.retry()) goto restart;

         auto& inner = xg_parent.as<Inner>(0);
         auto pos = inner.lowerBound(k);
         if ((inner.count >= 2) && ((pos + 1) < inner.count)) {
            // get right node
            PID pid_right = inner.children[pos+1];
            ExclusiveBFGuard xg_right(pid_right);
            if (xg_right.retry()) goto restart;
            auto& right = xg_right.as<Leaf>(0);
            // check if right fits into current node
            if (leaf.count + right.count >= Leaf::maxEntries) {
               // does not fit therefore remove key and return 
               auto removed = leaf.remove(k);
               return removed;
            }
            // right fits into leaf
            leaf.setFences(leaf.fenceKeys.getLower(), right.fenceKeys.getUpper());
            memcpy(leaf.keys + leaf.count, right.keys, sizeof(Key) * right.count);
            memcpy(leaf.payloads + leaf.count, right.payloads, sizeof(Value) * right.count);
            // adjust count
            leaf.count+= right.count;
            inner.remove(pos);
            xg_right.reclaim();
         }
         auto removed = leaf.remove(k);
         return removed;
      }
      
      ExclusiveBFGuard xg_node(std::move(g_node));
      if (xg_node.retry()) { goto restart; }
      ensure(xg_node.getFrame().latch.isLatched());
      // -------------------------------------------------------------------------------------
      // release parent lock
      if (g_parent.retry()) goto restart;

      auto removed = leaf.remove(k);
      return removed;
   }
   // -------------------------------------------------------------------------------------
   // latches the leaf exclusive and executes the callback
   // -------------------------------------------------------------------------------------
   bool lookupAndUpdate(Key k, std::function<void(Value& value)> callback) {
      using Inner = BTreeInner<Key>;
      using Leaf = BTreeLeaf<Key, Value>;
   restart:
      OptimisticBFGuard g_parent(entryPage);
      // -------------------------------------------------------------------------------------
      auto* entry = g_parent.asPtr<BTreeEntry>(0);
      if (g_parent.retry()) goto restart;
      ensure(entry);
      PID currentPID = entry->root;
      uint64_t height = entry->height;
      uint64_t currentLevel = 0;
      NodeBase* node = nullptr;
      // -------------------------------------------------------------------------------------
      // Inner
      // -------------------------------------------------------------------------------------
      while (currentLevel < height) {
         // get current node from PID
         OptimisticBFGuard g_node(currentPID);
         if (g_parent.retry())
            goto restart;
         node = g_node.asPtr<NodeBase>(0);
         if (g_node.retry())
            goto restart;  // check inner
         ensure(node->type == PageType::BTreeInner);
         auto& inner = *reinterpret_cast<Inner*>(node);
         // -------------------------------------------------------------------------------------
         ensure(g_node.g.latchState != LATCH_STATE::EXCLUSIVE);
         // -------------------------------------------------------------------------------------
         if (g_parent.retry())
            goto restart;
         currentPID = inner.children[inner.lowerBoundBF(k)];
         if (g_node.retry())
            goto restart;  // check inner
         g_parent = std::move(g_node);
         ensure(g_node.g.latchState != LATCH_STATE::EXCLUSIVE);
         // -------------------------------------------------------------------------------------
         currentLevel++;
      }     
      // -------------------------------------------------------------------------------------
      // Leaf
      // -------------------------------------------------------------------------------------
      ExclusiveBFGuard xg_node(currentPID);
      if (xg_node.retry())
         goto restart;
      ensure(xg_node.getFrame().latch.isLatched());
      // -------------------------------------------------------------------------------------
      node = xg_node.asPtr<NodeBase>(0);
      ensure(node->type == PageType::BTreeLeaf);
      // -------------------------------------------------------------------------------------
      auto& leaf = *reinterpret_cast<Leaf*>(node);
      if (g_parent.retry()) goto restart;
      uint64_t pos = leaf.lowerBound(k);
      if ((pos < leaf.count) && (leaf.keys[pos] == k)) {
         callback(leaf.payloads[pos]);
         return true;
      }
      ensure(false);
      return false;
   }

   // -------------------------------------------------------------------------------------
   // Lookup (leaf is shared latched)
   // -------------------------------------------------------------------------------------
   bool lookup(Key k, Value& returnValue) {
      using Inner = BTreeInner<Key>;
      using Leaf = BTreeLeaf<Key, Value>;
   restart:
      OptimisticBFGuard g_parent(entryPage);
      // -------------------------------------------------------------------------------------
      auto* entry = g_parent.asPtr<BTreeEntry>(0);
      if (g_parent.retry()) goto restart;
      ensure(entry);
      PID currentPID = entry->root;
      uint64_t height = entry->height;
      uint64_t currentLevel = 0;
      NodeBase* node = nullptr;
      // -------------------------------------------------------------------------------------
      // Inner
      while (currentLevel < height) {
         // get current node from PID
         OptimisticBFGuard g_node(currentPID);
         if (g_parent.retry())
            goto restart;
         node = g_node.asPtr<NodeBase>(0);
         if (g_node.retry())
            goto restart;  // check inner
         ensure(node->type == PageType::BTreeInner);
         auto& inner = *reinterpret_cast<Inner*>(node);
         // -------------------------------------------------------------------------------------
         ensure(g_node.g.latchState != LATCH_STATE::EXCLUSIVE);
         // -------------------------------------------------------------------------------------
         if (g_parent.retry())
            goto restart;
         currentPID = inner.children[inner.lowerBoundBF(k)];
         if (g_node.retry())
            goto restart;  // check inner
         g_parent = std::move(g_node);
         ensure(g_node.g.latchState != LATCH_STATE::EXCLUSIVE);
         // -------------------------------------------------------------------------------------
         currentLevel++;
      }
      // -------------------------------------------------------------------------------------
      // Leaf
      // -------------------------------------------------------------------------------------
      // todo fix this workaround once we resolved the glibc bug by updateing the OS
      /*
      OptimisticBFGuard leaf_g(currentPID);
      if (leaf_g.retry())
            goto restart;  // check inner
      SharedBFGuard sg_node(std::move(leaf_g));
      */
      SharedBFGuard sg_node(currentPID);
      if (sg_node.retry()) { goto restart; }
      node = &sg_node.as<NodeBase>(0);
      ensure(!sg_node.getFrame().latch.isLatched());
      auto& leaf = *reinterpret_cast<Leaf*>(node);
      // -------------------------------------------------------------------------------------
      // release parent lock
      if (g_parent.retry()) goto restart;
      uint64_t pos = leaf.lowerBound(k);
      returnValue = leaf.payloads[pos];
      if ((pos < leaf.count) && (leaf.keys[pos] == k)) return true;

      ensure(false);
      return false;
   }
   
   // -------------------------------------------------------------------------------------
   // optimistic lookup
   // -------------------------------------------------------------------------------------
   template <class Fn>
   bool lookup_opt(Key k, Fn&& read_function) {
      threads::Worker::my().counters.incr(profiling::WorkerCounters::btree_traversals);
      using Inner = BTreeInner<Key>;
      using Leaf = BTreeLeaf<Key, Value>;
   restart:
      threads::Worker::my().counters.incr(profiling::WorkerCounters::btree_restarted);
      OptimisticBFGuard g_parent(entryPage);
      // -------------------------------------------------------------------------------------
      auto* entry = g_parent.asPtr<BTreeEntry>(0);
      if (g_parent.retry()) goto restart;
      ensure(entry);
      PID currentPID = entry->root;
      uint64_t height = entry->height;
      uint64_t currentLevel = 0;
      NodeBase* node = nullptr;
      // -------------------------------------------------------------------------------------
      while (currentLevel < height) {
         // get current node from PID
         OptimisticBFGuard g_node(currentPID);
         if (g_parent.retry())
            goto restart;
         node = g_node.asPtr<NodeBase>(0);
         if (g_node.retry())
            goto restart;  // check inner
         ensure(node->type == PageType::BTreeInner);
         auto& inner = *reinterpret_cast<Inner*>(node);
         // -------------------------------------------------------------------------------------
         ensure(g_node.g.latchState != LATCH_STATE::EXCLUSIVE);
         // -------------------------------------------------------------------------------------
         if (g_parent.retry())
            goto restart;
         currentPID = inner.children[inner.lowerBoundBF(k)];
         if (g_node.retry())
            goto restart;  // check inner
         g_parent = std::move(g_node);
         ensure(g_node.g.latchState != LATCH_STATE::EXCLUSIVE);
         // -------------------------------------------------------------------------------------
         currentLevel++;
      }      
      // -------------------------------------------------------------------------------------
      // Leaf
      // -------------------------------------------------------------------------------------
   restartLeaf:
      OptimisticBFGuard og_node(currentPID);
      // -------------------------------------------------------------------------------------
      node = og_node.asPtr<NodeBase>(0);
      // -------------------------------------------------------------------------------------
      auto& leaf = *reinterpret_cast<Leaf*>(node);
      if (g_parent.retry()) goto restart;
      if (og_node.retry()) { goto restartLeaf; }
      // -------------------------------------------------------------------------------------
      uint64_t pos = leaf.lowerBound(k);
      if ((pos < leaf.count) && (leaf.keys[pos] == k)) { 
         read_function(leaf.payloads[pos]); // pass value to function
         if (g_parent.retry()) goto restart;
         if (og_node.retry()) { goto restartLeaf; }
         return true;
      }
      if (g_parent.retry()) goto restart;
      if (og_node.retry()) { goto restartLeaf; }
      ensure(false);
      return false;
   }

   bool lookup_opt(Key k, Value& returnValue) {
      using Inner = BTreeInner<Key>;
      using Leaf = BTreeLeaf<Key, Value>;
      threads::Worker::my().counters.incr(profiling::WorkerCounters::btree_traversals);
   restart:
      threads::Worker::my().counters.incr(profiling::WorkerCounters::btree_restarted);
      OptimisticBFGuard g_parent(entryPage);
      // -------------------------------------------------------------------------------------
      auto* entry = g_parent.asPtr<BTreeEntry>(0);
      if (g_parent.retry()) goto restart;
      ensure(entry);
      PID currentPID = entry->root;
      uint64_t height = entry->height;
      uint64_t currentLevel = 0;
      NodeBase* node = nullptr;
      // -------------------------------------------------------------------------------------
      while (currentLevel < height) {
         // get current node from PID
         OptimisticBFGuard g_node(currentPID);
         if (g_parent.retry())
            goto restart;
         node = g_node.asPtr<NodeBase>(0);
         if (g_node.retry())
            goto restart;  // check inner
         ensure(node->type == PageType::BTreeInner);
         auto& inner = *reinterpret_cast<Inner*>(node);
         // -------------------------------------------------------------------------------------
         ensure(g_node.g.latchState != LATCH_STATE::EXCLUSIVE);
         // -------------------------------------------------------------------------------------
         if (g_parent.retry())
            goto restart;
         currentPID = inner.children[inner.lowerBoundBF(k)];
         if (g_node.retry())
            goto restart;  // check inner
         g_parent = std::move(g_node);
         ensure(g_node.g.latchState != LATCH_STATE::EXCLUSIVE);
         // -------------------------------------------------------------------------------------
         currentLevel++;
      }      
      // -------------------------------------------------------------------------------------
      // Leaf
      // -------------------------------------------------------------------------------------
   restartLeaf:
      OptimisticBFGuard og_node(currentPID);
      // -------------------------------------------------------------------------------------
      node = og_node.asPtr<NodeBase>(0);
      // -------------------------------------------------------------------------------------
      auto& leaf = *reinterpret_cast<Leaf*>(node);
      if (g_parent.retry()) goto restart;
      if (og_node.retry()) { goto restartLeaf; }
      // -------------------------------------------------------------------------------------
      uint64_t pos = leaf.lowerBound(k);
      returnValue = leaf.payloads[pos];
      if ((pos < leaf.count) && (leaf.keys[pos] == k)) {
         if (g_parent.retry()) goto restart;
         if (og_node.retry()) { goto restartLeaf; }
         return true;
      }
      if (g_parent.retry()) goto restart;
      if (og_node.retry()) { goto restartLeaf; }
      ensure(false);
      return false;
   }
   // -------------------------------------------------------------------------------------
   // Scan code
   // -------------------------------------------------------------------------------------   
   enum class RC : uint8_t { FINISHED = 0, CONTINUE = 2 };
   struct op_result {
      RC return_code = RC::FINISHED;
      Key next_sep;  // used when scan needs to continue
   };
   // -------------------------------------------------------------------------------------
   // Functors
   // -------------------------------------------------------------------------------------
   struct DESC_SCAN {
      bool isFirstTraversal;
      // -------------------------------------------------------------------------------------
      DESC_SCAN(bool isFirstTraversal) : isFirstTraversal(isFirstTraversal){};
      // -------------------------------------------------------------------------------------
      // inner traversal
      auto next_node(Key k, BTreeInner<Key>& inner) { return inner.children[inner.lowerBound(k)]; }

      // -------------------------------------------------------------------------------------
      template <class Fn>
      op_result operator()(Key k, BTreeLeaf<Key, Value>& leaf, Fn&& func) {
         if (leaf.count == 0) {
            if (leaf.fenceKeys.isLowerInfinity())  // scan done
               return {RC::FINISHED, leaf.fenceKeys.getLower().key};           
            return {RC::CONTINUE, leaf.fenceKeys.getLower().key}; 
         }
         // -------------------------------------------------------------------------------------
         auto lower_bound = leaf.lowerBound(k);  // if fence key is deleted
         if (lower_bound >= leaf.count) lower_bound = leaf.count - 1;
         auto it = typename BTreeLeaf<Key, Value>::desc_iterator(leaf, lower_bound);  // returns first key not less than k
         auto end = leaf.rend();
         bool enter_scan = true;

         if (leaf.keys[*it] > k) {  // lower bound returned one larger element
            if (*it == 0) {
               enter_scan = false;  // goes to next leaf;
            } else {
               ++it;  // adjust by one to left
            }
         }
         
         while (enter_scan) {
            if (!func(leaf.keys[*it], leaf.payloads[*it])) return {RC::FINISHED, leaf.fenceKeys.getLower().key};
            if (it == end) {  // end is 0 meaning it is including the last which is unusual
               break;
            }
            ++it;
         }
         if (leaf.fenceKeys.isLowerInfinity())  // scan done
            return {RC::FINISHED, leaf.fenceKeys.getLower().key};
         return {RC::CONTINUE, leaf.fenceKeys.getLower().key};
      }
   };
   // -------------------------------------------------------------------------------------
   struct ASC_SCAN {
      bool isFirstTraversal;
      // -------------------------------------------------------------------------------------
      ASC_SCAN(bool isFirstTraversal) : isFirstTraversal(isFirstTraversal){};
      // -------------------------------------------------------------------------------------
      // inner traversal
      PID next_node(Key k, BTreeInner<Key>& inner) {
         if (isFirstTraversal)
            return inner.children[inner.lowerBound(k)];
         else
            return inner.children[inner.upperBound(k)]; 
      }
      // -------------------------------------------------------------------------------------
      template <class Fn>
      op_result operator()(Key k, BTreeLeaf<Key, Value>& leaf, Fn&& func) {
         if (leaf.count == 0) {
            if (leaf.fenceKeys.isUpperInfinity())  // scan done
               return {RC::FINISHED, leaf.fenceKeys.getUpper().key};
            return {RC::CONTINUE, leaf.fenceKeys.getUpper().key}; 
         }
         // -------------------------------------------------------------------------------------
         auto it = typename BTreeLeaf<Key, Value>::asc_iterator(leaf, leaf.lowerBound(k));
         auto end = leaf.end();
         // -------------------------------------------------------------------------------------
         while (it != end) {
            if (!func(leaf.keys[*it], leaf.payloads[*it])) return {RC::FINISHED, leaf.fenceKeys.getUpper().key};
            ++it;
         }
         if (leaf.fenceKeys.isUpperInfinity())  // scan done
            return {RC::FINISHED, leaf.fenceKeys.getUpper().key};
         return {RC::CONTINUE, leaf.fenceKeys.getUpper().key};
      }
   };
   // -------------------------------------------------------------------------------------
   // Scan driver
   // -------------------------------------------------------------------------------------
   
   template <class SCAN_DIRECTION, class Fn>
   void scan(Key k, Fn&& func) {
      bool isFirstTraversal = true;
      op_result res;
      res.next_sep = k;
      do {
         res = scan_(res.next_sep, func, SCAN_DIRECTION(isFirstTraversal));
         isFirstTraversal =
             false;  // important for asc scan; after the first traversal it needs to use upperbound in inner nodes for fence keys
      } while (res.return_code == RC::CONTINUE);
   }
   // -------------------------------------------------------------------------------------
  private:
   // -------------------------------------------------------------------------------------     
   // Scan 
   // -------------------------------------------------------------------------------------
   template <class Fn, class SCAN_DIRECTION>
   op_result scan_(Key k, Fn&& func, SCAN_DIRECTION scan_functor) {
      using Inner = BTreeInner<Key>;
      using Leaf = BTreeLeaf<Key, Value>;
   restart:
      OptimisticBFGuard g_parent(entryPage);
      // -------------------------------------------------------------------------------------
      auto* entry = g_parent.asPtr<BTreeEntry>(0);
      if (g_parent.retry()) goto restart;
      ensure(entry);
      PID rootPid = entry->root;
      OptimisticBFGuard g_node(rootPid);
      if (g_parent.retry()) goto restart;
      // -------------------------------------------------------------------------------------
      // Inner
      // -------------------------------------------------------------------------------------
      auto* node = g_node.asPtr<NodeBase>(0);
      if (g_node.retry()) goto restart;  // check inner
      // -------------------------------------------------------------------------------------
      while (node->type == PageType::BTreeInner) {
         auto& inner = *reinterpret_cast<Inner*>(node);
         // -------------------------------------------------------------------------------------
         if (g_parent.retry()) goto restart;

         PID nextPid = scan_functor.next_node(k,inner); // upper or lower bound search
         if (g_node.retry()) goto restart;  // check inner

         g_parent = std::move(g_node);
         g_node = OptimisticBFGuard(nextPid);
         node = g_node.asPtr<NodeBase>(0);
         if (g_node.retry()) goto restart;  // check inner
      }
      // -------------------------------------------------------------------------------------
      // Leaf
      // -------------------------------------------------------------------------------------
      auto& leaf = *reinterpret_cast<Leaf*>(node);

      SharedBFGuard sg_node(std::move(g_node));
      if (sg_node.retry()) { goto restart; }
      ensure(!sg_node.getFrame().latch.isLatched());
      // -------------------------------------------------------------------------------------
      // release parent lock
      if (g_parent.retry()) goto restart;
      auto op_code = scan_functor(k, leaf, func);
      return op_code;
   }
   
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace scalestore
