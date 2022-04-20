#pragma once
// -------------------------------------------------------------------------------------
#include "Defs.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
#include <stdexcept>

namespace scalestore {

namespace storage {

template<typename T> class RingBuffer {
   // don't use default ctor
   u64 size;
   T *data;
   u64 front;
   u64 count;
public:
   RingBuffer(u64 size);
   RingBuffer() = delete;
   ~RingBuffer();

   bool empty() { return count == 0; }
   bool full() { return count == size; }
   size_t getSize() { return count; }
   bool add(const T&);
   T& remove();
   T& peek();
   u64 getCurrentIndex(){
      return (front + count) & (size - 1);
   }
   void clear(){
      front = 0;
      count = 0;
   }
};

template<typename T> RingBuffer<T>::RingBuffer(u64 sz) {
   size = (Helper::powerOfTwo(sz)) ? sz : Helper::nextPowerTwo(sz);
   if (sz==0) throw std::invalid_argument("size cannot be zero");
   data = new T[size];
   front = 0;
   count = 0;
}
template<typename T> RingBuffer<T>::~RingBuffer() {
   delete[] data;
}

template<typename T> bool RingBuffer<T>::add(const T &t) {
      int end = (front + count) & (size - 1);
      data[end] = t;
      count++;
      return true;
}


template<typename T>
T& RingBuffer<T>::peek() {
      T& t = data[front];     
      return t;
}


template<typename T>
T& RingBuffer<T>::remove() {
      T& t = data[front];
      front =  (front + 1) & (size -1);
      ensure(count > 0);
      count--;
      return t;
}


}  // storage
}  // scalestore
