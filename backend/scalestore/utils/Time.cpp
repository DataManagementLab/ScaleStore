#include "Time.hpp"
// -------------------------------------------------------------------------------------

namespace scalestore
{
namespace utils
{
uint64_t getTimePoint()
{

      using namespace std::chrono;
      auto now = system_clock::now();
      auto now_micros = time_point_cast<microseconds>(now);
      auto value = now_micros.time_since_epoch();
      return value.count();
}
}  // utils
}  // namespace utils
