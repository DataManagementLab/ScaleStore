#include "FNVHash.hpp"
#include "Defs.hpp"
// #include "ZipfGenerator.hpp"
#include "ZipfRejectionInversion.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace utils
{

class ScrambledZipfGenerator
{
  public:
   u64 min, max, n;
   double theta;
   std::random_device rd;
   std::mt19937 gen;
   zipf_distribution<> zipf_generator;
   // 10000000000ul
   // [min, max)
   ScrambledZipfGenerator(u64 min, u64 max, double theta) : min(min), max(max), n(max - min), gen(rd()), zipf_generator((max - min) * 2, theta) {
   }
   u64 rand();
   u64 rand(u64 offset);
};

// class ScrambledZipfGenerator
// {
//   public:
//    u64 min, max, n;
//    double theta;
//    ZipfGenerator zipf_generator;
//    // 10000000000ul
//    // [min, max)
//    ScrambledZipfGenerator(u64 min, u64 max, double theta) : min(min), max(max), n(max - min), zipf_generator((max - min) * 2, theta) {
//    }
//    u64 rand();
//    u64 rand(u64 offset);
// };
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace scalestore
