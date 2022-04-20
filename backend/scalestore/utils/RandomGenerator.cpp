#include "RandomGenerator.hpp"
#include <random>
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace utils
{
static std::atomic<uint64_t> mt_counter = 0;
// -------------------------------------------------------------------------------------
MersenneTwister::MersenneTwister(uint64_t seed) : mti(NN + 1)
{
   static thread_local std::random_device rd;
   init((seed ^ (mt_counter++)) ^ rd());
}
// -------------------------------------------------------------------------------------
void MersenneTwister::init(uint64_t seed)
{
   mt[0] = seed;
   for (mti = 1; mti < NN; mti++)
      mt[mti] = (6364136223846793005ULL * (mt[mti - 1] ^ (mt[mti - 1] >> 62)) + mti);
}
// -------------------------------------------------------------------------------------
uint64_t MersenneTwister::rnd()
{
   int i;
   uint64_t x;
   static uint64_t mag01[2] = {0ULL, MATRIX_A};

   if (mti >= NN) { /* generate NN words at one time */

      for (i = 0; i < NN - MM; i++) {
         x = (mt[i] & UM) | (mt[i + 1] & LM);
         mt[i] = mt[i + MM] ^ (x >> 1) ^ mag01[(int)(x & 1ULL)];
      }
      for (; i < NN - 1; i++) {
         x = (mt[i] & UM) | (mt[i + 1] & LM);
         mt[i] = mt[i + (MM - NN)] ^ (x >> 1) ^ mag01[(int)(x & 1ULL)];
      }
      x = (mt[NN - 1] & UM) | (mt[0] & LM);
      mt[NN - 1] = mt[MM - 1] ^ (x >> 1) ^ mag01[(int)(x & 1ULL)];

      mti = 0;
   }

   x = mt[mti++];

   x ^= (x >> 29) & 0x5555555555555555ULL;
   x ^= (x << 17) & 0x71D67FFFEDA60000ULL;
   x ^= (x << 37) & 0xFFF7EEE000000000ULL;
   x ^= (x >> 43);

   return x;
}
// -------------------------------------------------------------------------------------
Xorshift64star::Xorshift64star(uint64_t seed_){
   static thread_local std::random_device rd;
   seed = (seed_ ^ (mt_counter++)) ^ rd();
}
// -------------------------------------------------------------------------------------
uint64_t Xorshift64star::rnd(){  
      uint64_t x = seed; /* state nicht mit 0 initialisieren */
      x ^= x >> 12;      // a
      x ^= x << 25;      // b
      x ^= x >> 27;      // c
      seed = x;
      return x * 0x2545F4914F6CDD1D;
} 

// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace scalestore
