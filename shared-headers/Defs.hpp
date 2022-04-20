#pragma once
// -------------------------------------------------------------------------------------
#include <cstdint>
#include <iostream>
#include <vector>
#include <string>
#include <csignal>
#include <immintrin.h>
#include <experimental/source_location>
#include <sstream>
// -------------------------------------------------------------------------------------
#define GDB() std::raise(SIGINT);
// -------------------------------------------------------------------------------------
#define BACKOFF()                          \
   int const max = 4;                     \
   if (true) {                    \
      for (int i = mask; i; --i) {         \
         _mm_pause();                      \
      }                                    \
      mask = mask < max ? mask << 1 : max; \
   }                                       \

enum LOG_LEVEL{
   RELEASE = 0,
   CSV = 1,
   TEST = 2,
   TRACE = 3,
};

// -------------------------------------------------------------------------------------
// ensure is similar to assert except that it is never out compiled
#define always_check(e)                                                               \
    do {                                                                        \
        if (__builtin_expect(!(e), 0)) {                                        \
            std::stringstream ss;                                               \
            ss << __func__ << " in " << __FILE__ << ":" << __LINE__ << '\n';      \
            ss << " msg: " << std::string(#e);                                  \
            throw std::runtime_error(ss.str());                                 \
        }                                                                       \
    } while (0)

#define ENSURE_ENABLED 1
#ifdef ENSURE_ENABLED
#define ensure(e) always_check(e);
#else
#define ensure(e) do {} while(0);
#endif

template <typename T>
inline void DO_NOT_OPTIMIZE(T const& value)
{
#if defined(__clang__)
  asm volatile("" : : "g"(value) : "memory");
#else
  asm volatile("" : : "i,r,m"(value) : "memory");
#endif
}

#define posix_check(expr) \
   if (!(expr)) {         \
      perror(#expr);      \
      assert(false);      \
   }

using u64 = uint64_t;
using s32 = int32_t;
constexpr size_t CACHE_LINE = 64;
constexpr size_t MAX_NODES = 64; // only supported due to bitmap
constexpr size_t PARTITIONS = 64;  // partitions for partitioned queue 
constexpr size_t BATCH_SIZE = 128; // for partitioned queue 

constexpr auto ACTIVE_LOG_LEVEL = LOG_LEVEL::RELEASE;

const std::vector<std::vector<std::string>> NODES{
    {""},                                                                                              // 0 to allow direct offset
    {"172.18.94.80"},                                                                                  // 1
    {"172.18.94.80", "172.18.94.70"},                                                                  // 2
    {"172.18.94.80", "172.18.94.70", "172.18.94.10"},                                                  // 3
    {"172.18.94.80", "172.18.94.70", "172.18.94.10", "172.18.94.20"},                                  // 4
    {"172.18.94.80", "172.18.94.70", "172.18.94.10", "172.18.94.20", "172.18.94.40"},                  // 5
    {"172.18.94.80", "172.18.94.70", "172.18.94.10", "172.18.94.20", "172.18.94.40", "172.18.94.30"},  // 6
};

static std::string rdmaPathRecv = "/sys/class/infiniband/mlx5_0/ports/1/counters/port_rcv_data";
static std::string rdmaPathXmit = "/sys/class/infiniband/mlx5_0/ports/1/counters/port_xmit_data";
using NodeID = uint64_t;
// -------------------------------------------------------------------------------------
constexpr uint64_t PAGEID_BITS_NODEID = 8;
constexpr uint64_t NODEID_MASK = (~uint64_t(0) >> 8);
struct PID {
   // -------------------------------------------------------------------------------------
   uint64_t id = 0;
   PID() = default;
   explicit PID(uint64_t pid_id):id(pid_id){};
   constexpr PID(uint64_t owner, uint64_t id) : id(((owner << ((sizeof(uint64_t) * 8) - PAGEID_BITS_NODEID))) | id){};
   NodeID getOwner() { return NodeID(id >> ((sizeof(uint64_t) * 8 - PAGEID_BITS_NODEID))); }
   uint64_t plainPID() { return (id & NODEID_MASK) ; }
   operator uint64_t(){ return id; }
   inline PID& operator=(const uint64_t& other){
      id = other;
      return *this;
   }
   friend bool operator==(const PID& lhs, const PID& rhs)  { return (lhs.id == rhs.id); }
   friend bool operator!=(const PID& lhs, const PID& rhs)  { return (lhs.id != rhs.id); }
   friend bool operator>=(const PID& lhs, const PID& rhs)  { return (lhs.id >= rhs.id); }
   friend bool operator<=(const PID& lhs, const PID& rhs)  { return (lhs.id <= rhs.id); }
   friend bool operator<(const PID& lhs, const PID& rhs)  { return (lhs.id < rhs.id); }
   friend bool operator>(const PID& lhs, const PID& rhs)  { return (lhs.id > rhs.id); }
   // -------------------------------------------------------------------------------------
};



constexpr NodeID EMPTY_NODEID (~uint64_t(0));
constexpr PID EMPTY_PID ((~uint8_t(0)), (~uint64_t(0)));
constexpr uint64_t EMPTY_PVERSION ((~uint64_t(0)));
constexpr uint64_t EMPTY_EPOCH ((~uint64_t(0)));
// -------------------------------------------------------------------------------------
// helper functions
// -------------------------------------------------------------------------------------

struct Helper{
static unsigned long nextPowerTwo(unsigned long v)
{
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    return v;

}

static bool powerOfTwo(u64 n)
{
   return n && (!(n & (n-1)));
}
};

// -------------------------------------------------------------------------------------
// Catalog
// -------------------------------------------------------------------------------------
constexpr NodeID CATALOG_OWNER {0};
constexpr PID CATALOG_PID (CATALOG_OWNER,0);

