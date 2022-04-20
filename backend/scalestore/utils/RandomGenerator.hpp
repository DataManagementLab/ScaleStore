#pragma once
// -------------------------------------------------------------------------------------
#include "Defs.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <random>
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace utils
{
class MersenneTwister
{
  private:
   static const int NN = 312;
   static const int MM = 156;
   static const uint64_t MATRIX_A = 0xB5026F5AA96619E9ULL;
   static const uint64_t UM = 0xFFFFFFFF80000000ULL;
   static const uint64_t LM = 0x7FFFFFFFULL;
   uint64_t mt[NN];
   int mti;
   void init(uint64_t seed);

  public:
   MersenneTwister(uint64_t seed = 19650218ULL);
   uint64_t rnd();
};

class Xorshift64star
{
  private:
   uint64_t seed;
  public:
   Xorshift64star(uint64_t seed = 19650218ULL);
   uint64_t rnd();
};
}  // namespace utils
}  // namespace scalestore
// -------------------------------------------------------------------------------------
static thread_local scalestore::utils::MersenneTwister mt_generator;
static thread_local std::mt19937 random_generator;
static thread_local scalestore::utils::Xorshift64star fast_generator; 
// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace utils
{
// -------------------------------------------------------------------------------------
class RandomGenerator
{
  public:
   // ATTENTION: open interval [min, max)
   static uint64_t getRandU64(uint64_t min, uint64_t max)
   {
      uint64_t rand = min + (mt_generator.rnd() % (max - min));
      ensure(rand < max);
      ensure(rand >= min);
      return rand;
   }

   static uint64_t getRandU64Fast(){
      return fast_generator.rnd();
   }
   
   // ATTENTION: open interval [min, max)
   // ATTENTION: power two
   static uint64_t getRandU64PowerTwo(uint64_t maxPowerTwo)
   {
      uint64_t rand = (mt_generator.rnd() & (maxPowerTwo-1));
      return rand;
   }
   static uint64_t getRandU64() { return mt_generator.rnd(); }
   static uint64_t getRandU64STD(uint64_t min, uint64_t max)
   {
      std::uniform_int_distribution<uint64_t> distribution(min, max - 1);
      return distribution(random_generator);
   }

   template <typename T>
   static inline T getRand(T min, T max)
   {
      uint64_t rand = getRandU64(min, max);
      return static_cast<T>(rand);      
   }

   
   static void getRandString(uint8_t* dst, u64 size)
   {
      for (u64 t_i = 0; t_i < size; t_i++) {
         dst[t_i] = getRand(48, 123);
      }

   }  // namespace utils
};
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace scalestore
   // -------------------------------------------------------------------------------------
