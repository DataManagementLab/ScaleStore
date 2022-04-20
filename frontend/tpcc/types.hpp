#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>

typedef int32_t Integer;
typedef uint64_t Timestamp;
typedef double Numeric;

static constexpr Integer minInteger = std::numeric_limits<int>::min();

template <int maxLength>
struct Varchar {
   int16_t length;
   char data[maxLength];

   Varchar() : length(0) {}
   Varchar(const char* str) {
      int l = strlen(str);
      assert(l <= maxLength);
      length = l;
      memcpy(data, str, l);
   }
   template <int otherMaxLength>
   Varchar(const Varchar<otherMaxLength>& other) {
      assert(other.length <= maxLength);
      length = other.length;
      memcpy(data, other.data, length);
   }

   void append(char x) { data[length++] = x; };
   std::string toString() { return std::string(data, length); };

   template <int otherMaxLength>
   Varchar<maxLength> operator||(const Varchar<otherMaxLength>& other) const {
      Varchar<maxLength> tmp;
      assert((static_cast<int32_t>(length) + other.length) <= maxLength);
      tmp.length = length + other.length;
      memcpy(tmp.data, data, length);
      memcpy(tmp.data + length, other.data, other.length);
      return tmp;
   }

   bool operator==(const Varchar<maxLength>& other) const { return (length == other.length) && (memcmp(data, other.data, length) == 0); }

   bool operator>(const Varchar<maxLength>& other) const {
      int cmp = memcmp(data, other.data, (length < other.length) ? length : other.length);
      if (cmp)
         return cmp > 0;
      else
         return length > other.length;
   }

   bool operator<(const Varchar<maxLength>& other) const {
      int cmp = memcmp(data, other.data, (length < other.length) ? length : other.length);
      if (cmp)
         return cmp < 0;
      else
         return length < other.length;
   }
};
