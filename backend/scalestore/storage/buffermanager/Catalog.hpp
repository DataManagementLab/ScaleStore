#pragma once
// -------------------------------------------------------------------------------------
namespace scalestore {
namespace storage {

// -------------------------------------------------------------------------------------
// metadata are placed in the catalogPID {0/0} is located on node 0
// -------------------------------------------------------------------------------------
enum class DS_TYPE : uint8_t{
   EMPTY = 0,
   BARRIER = 1,
   TABLE = 2,
   BTREE = 3,
   LLIST = 4,
   MISC = 5, // other data which need to be shared e.g. keys
};
// -------------------------------------------------------------------------------------
struct CatalogEntry{
   uint64_t datastructureId;
   DS_TYPE type;
   PID pid; // e.g. root in btree/ barrier/ first page in table
};
// -------------------------------------------------------------------------------------
struct CatalogPage{
// ATTENTION: when adding new variables adapt maxEntries
   uint64_t datastructureIds {0};
   uint64_t numEntries = 0;
   static constexpr uint64_t maxEntries = (EFFECTIVE_PAGE_SIZE - (sizeof(uint64_t) + sizeof(uint64_t)  + sizeof(uint64_t) + sizeof(bool))) / (sizeof(CatalogEntry));
   bool isInitialized = false; // node 0 initializes the global catalog
   CatalogEntry entries[maxEntries];
};
// -------------------------------------------------------------------------------------
struct Catalog{

   void init(NodeID nodeId){
         if (nodeId == CATALOG_OWNER) { // implicit dependency to BM as catalog page need to exist
         ExclusiveBFGuard guard(CATALOG_PID); 
         auto& catalogPage = guard.getFrame().page->getTuple<CatalogPage>(0);
         catalogPage.isInitialized = true;
         guard.getFrame().epoch = (~uint64_t(0));
      }else{
         bool isInitialized = false;
         while(!isInitialized){
            SharedBFGuard guard(CATALOG_PID);
            guard.getFrame().epoch = (~uint64_t(0));
            auto& catalogPage = guard.getFrame().page->getTuple<CatalogPage>(0);
            isInitialized = catalogPage.isInitialized;
         }
      }
   }
   
   CatalogEntry& getCatalogEntry(uint64_t id)
   {
      do {
         SharedBFGuard guard(CATALOG_PID);
         guard.getFrame().epoch = (~uint64_t(0));
         auto& catalogPage = guard.getFrame().page->getTuple<CatalogPage>(0);
         if (catalogPage.datastructureIds >= id && catalogPage.entries[id].type != DS_TYPE::EMPTY)  // not yet initialized
            return catalogPage.entries[id];
      } while (true);
   }

   uint64_t insertCatalogEntry(DS_TYPE type, PID pid){    
      ExclusiveBFGuard guard(CATALOG_PID);
      guard.getFrame().epoch = (~uint64_t(0));
      auto& catalogPage = guard.getFrame().page->getTuple<CatalogPage>(0);
      catalogPage.entries[catalogPage.datastructureIds] = {.datastructureId = catalogPage.datastructureIds, .type = type, .pid=pid};
      auto tmp = catalogPage.datastructureIds;
      catalogPage.datastructureIds++;
      return tmp;
   }
   
};
}  // storage
}  // scalestore
