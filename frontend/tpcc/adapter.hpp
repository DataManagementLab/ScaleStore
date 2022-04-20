#pragma once
#include "types.hpp"
// -------------------------------------------------------------------------------------
#include "scalestore/Config.hpp"
#include "scalestore/ScaleStore.hpp"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <map>
#include <string> 

using namespace scalestore;

template <class Record>
struct ScaleStoreAdapter{
   using BTree = storage::BTree<typename Record::Key, Record>;
   std::string name;
   PID tree_pid;
   ScaleStoreAdapter(){};
   ScaleStoreAdapter(ScaleStore& db, std::string name): name(name){
      auto& catalog = db.getCatalog();
      if(db.getNodeID() == 0){
         db.createBTree<typename Record::Key, Record>();
      }
      tree_pid = catalog.getCatalogEntry(Record::id).pid;
   };

   void insert(const typename Record::Key& rec_key, const Record& record){
      BTree tree(tree_pid);
      tree.insert(rec_key, record);
   }

   template <class Fn>
   void scan(const typename Record::Key& key, const Fn& fn) {
      BTree tree(tree_pid);
      tree.template scan<typename BTree::ASC_SCAN>(key, [&](typename Record::Key& key, Record& record) { return fn(key, record); });
   }

   template <class Fn>
   void scanDesc(const typename Record::Key& key, const Fn& fn) {
      BTree tree(tree_pid);
      tree.template scan<typename BTree::DESC_SCAN>(key, [&](typename Record::Key& key, Record& record) { return fn(key, record); });
   }
   
   template <class Field>
   auto lookupField(const typename Record::Key& key, [[maybe_unused]] Field Record::*f) {
      BTree tree(tree_pid);
      Field local_f;
      auto res = tree.lookup_opt(key, [&](Record& value) { local_f = value.*f; });
      ensure(res);
      return local_f;
   }

   template <class Fn>
   // void update1(const typename Record::Key& key, const Fn& fn, storage::btree::WALUpdateGenerator wal_update_generator)
   void update1(const typename Record::Key& key, const Fn& fn)
   {
      BTree tree(tree_pid);
      auto res = tree.lookupAndUpdate(key, [&](Record& value) { fn(value); });
      ensure(res);
   }

   template <class Fn>
   void lookup1(const typename Record::Key& key, const Fn& fn) {
      BTree tree(tree_pid);
      const auto res = tree.lookup_opt(key, fn);
      ensure(res);
   }

   bool erase(const typename Record::Key& key) {
      BTree tree(tree_pid);
      const auto res = tree.remove(key);
      return res;
   }
};
