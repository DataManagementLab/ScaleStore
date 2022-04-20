#pragma once
// -------------------------------------------------------------------------------------
namespace scalestore {
namespace storage {
// -------------------------------------------------------------------------------------
// Attention: Not thread safe, only used to bulk load and share data between noded, e.g., experiment data
// -------------------------------------------------------------------------------------


// template<typename F>
// class A {
//     F function;
    
// public:
//     A(const F& f) : function(f) {}

//     template<typename ...Args>
//     auto f(Args... args) -> std::result_of_t<F(Args...)> {
//         return function(std::forward<Args>(args)...);
//     }
// };

template <typename T>
struct LinkedList{
   
   struct LinkedListNode{
      static constexpr uint64_t maxEntries = ((EFFECTIVE_PAGE_SIZE) - sizeof(PID) - sizeof(uint64_t)) / (sizeof(T));
      uint64_t count = 0;
      PID next = EMPTY_PID;
      T entries[maxEntries];


      bool isFull(){ return ( count == maxEntries);}
      void push(T element){
         ensure(!isFull());
         entries[count] = element;
         count++;
      }
      T& get(uint64_t idx){
         ensure(idx < count);
         return entries[idx];
      }
   };

   LinkedList() {
      ExclusiveBFGuard xg_head;
      xg_head.getFrame().epoch = (~uint64_t(0)); // max to never evict barrier
      auto& headNode = xg_head.allocate<LinkedListNode>();
      ensure(headNode.count == 0);
      ensure(headNode.next == EMPTY_PID);
      head = xg_head.getFrame().pid;
   }
   LinkedList(PID headPage) : head(headPage){}
   
   PID head;
 
   template <typename F> 
   void fill(std::vector<T>& content, F&& allocation){
      ExclusiveBFGuard xg_current(head);
      ensure(!xg_current.retry());

      while ((xg_current.as<LinkedListNode>(0)).next != EMPTY_PID) {
         xg_current = ExclusiveBFGuard(xg_current.as<LinkedListNode>(0).next);
      }

      for (auto& c : content) {
         auto& node = xg_current.as<LinkedListNode>(0);
         if (xg_current.as<LinkedListNode>(0).isFull()) {
            // create new node
            ExclusiveBFGuard xg_newNode = allocation();
            auto& headNode = xg_newNode.allocate<LinkedListNode>();
            xg_current.as<LinkedListNode>(0).next = xg_newNode.getFrame().pid;
            xg_current = std::move(xg_newNode);
         }
         xg_current.as<LinkedListNode>(0).push(c);
      }
      // find tail by iterating over all things if last full insert
   }

   template <typename F> 
   void fill(typename std::vector<T>::iterator& startIterator, uint64_t numberElements, F&& allocation){
      ExclusiveBFGuard xg_current(head);
      ensure(!xg_current.retry());

      while ((xg_current.as<LinkedListNode>(0)).next != EMPTY_PID) {
         xg_current = ExclusiveBFGuard(xg_current.as<LinkedListNode>(0).next);
      }

      uint64_t elements = 0;
      while (elements < numberElements) {
         auto& node = xg_current.as<LinkedListNode>(0);
         if (xg_current.as<LinkedListNode>(0).isFull()) {
            // create new node
            ExclusiveBFGuard xg_newNode = allocation();
            auto& headNode = xg_newNode.allocate<LinkedListNode>();
            xg_current.as<LinkedListNode>(0).next = xg_newNode.getFrame().pid;
            xg_current = std::move(xg_newNode);
         }
         xg_current.as<LinkedListNode>(0).push(*startIterator);
         elements++;
         startIterator++;
      }
   }

   template <typename F>
   void fillNumbers(uint64_t numberElements, F&& allocation)
   {
      ExclusiveBFGuard xg_current(head);
      ensure(!xg_current.retry());

      while ((xg_current.as<LinkedListNode>(0)).next != EMPTY_PID) {
         xg_current = ExclusiveBFGuard(xg_current.as<LinkedListNode>(0).next);
      }

      uint64_t elements = 0;
      while (elements < numberElements) {
         auto& node = xg_current.as<LinkedListNode>(0);
         if (xg_current.as<LinkedListNode>(0).isFull()) {
            // create new node
            ExclusiveBFGuard xg_newNode = allocation();
            auto& headNode = xg_newNode.allocate<LinkedListNode>();
            xg_current.as<LinkedListNode>(0).next = xg_newNode.getFrame().pid;
            xg_current = std::move(xg_newNode);
         }
         xg_current.as<LinkedListNode>(0).push(elements);
         elements++;
      }
   }

   
   template <typename FI, typename F>
   void fillNumbers(FI&& incrFunction, uint64_t numberElements, F&& allocation, std::vector<PID>& pids)
   {
      ExclusiveBFGuard xg_current(head);
      ensure(!xg_current.retry());

      while ((xg_current.as<LinkedListNode>(0)).next != EMPTY_PID) {
         xg_current = ExclusiveBFGuard(xg_current.as<LinkedListNode>(0).next);
      }

      uint64_t elements = 0;
      while (elements < numberElements) {
         if (xg_current.as<LinkedListNode>(0).isFull()) {
            // create new node
            ExclusiveBFGuard xg_newNode = allocation();
            pids.push_back(xg_newNode.getFrame().pid);
            xg_newNode.allocate<LinkedListNode>();
            xg_current.as<LinkedListNode>(0).next = xg_newNode.getFrame().pid;
            xg_current = std::move(xg_newNode);
         }
         xg_current.as<LinkedListNode>(0).push(incrFunction(elements, xg_current.getFrame().pid));
         elements++;
      }
   }

   void push(T element){
      ExclusiveBFGuard xg_current(head);
      ensure(!xg_current.retry());

      while((xg_current.as<LinkedListNode>(0)).next != EMPTY_PID){
         xg_current = ExclusiveBFGuard(xg_current.as<LinkedListNode>(0).next);
      }

      auto& node =xg_current.as<LinkedListNode>(0);
      if(xg_current.as<LinkedListNode>(0).isFull()){
         // create new node
         ExclusiveBFGuard xg_newNode;
         auto& headNode = xg_newNode.allocate<LinkedListNode>();
         xg_current.as<LinkedListNode>(0).next = xg_newNode.getFrame().pid;
         xg_newNode.as<LinkedListNode>(0).push(element);
         return;
      }
      xg_current.as<LinkedListNode>(0).push(element);
      // find tail by iterating over all things if last full insert
   }

   T& get(uint64_t idx){

      // find index first
      auto pages = idx / LinkedListNode::maxEntries;

      SharedBFGuard sg_current(head);
      ensure(!sg_current.retry());

      for(uint64_t p_i = 0; p_i < pages; p_i++)
         sg_current = SharedBFGuard(sg_current.as<LinkedListNode>(0).next);

      auto page_idx = idx %  LinkedListNode::maxEntries;
      ensure( page_idx < sg_current.as<LinkedListNode>(0).count);
      return sg_current.as<LinkedListNode>(0).entries[page_idx];
   }

   template <typename F>
   void applyToRange(F&& func, uint64_t startIdx, uint64_t endIdx)
   {
      // find index first
      auto pages = startIdx / LinkedListNode::maxEntries;

      SharedBFGuard sg_current(head);
      ensure(!sg_current.retry());

      for (uint64_t p_i = 0; p_i < pages; p_i++){
         PID next_pid = sg_current.as<LinkedListNode>(0).next;
         sg_current = SharedBFGuard(sg_current.as<LinkedListNode>(0).next);
         ensure(next_pid == sg_current.getFrame().pid);
      }
      
      auto page_idx = startIdx % LinkedListNode::maxEntries;
      ensure(page_idx < sg_current.as<LinkedListNode>(0).count);

      
      while(startIdx < endIdx){
         auto& node = sg_current.as<LinkedListNode>(0);
         for(; page_idx < node.count; page_idx++, startIdx++){
            if(startIdx == endIdx)
               return;
            func(node.entries[page_idx]);
         }
         if(startIdx == endIdx)
            return;
         PID next_pid = node.next;
         sg_current = SharedBFGuard(node.next);
         ensure(next_pid == sg_current.getFrame().pid);
         page_idx = 0;
      }     
   }
};

}  // storage
}  // scalestore
