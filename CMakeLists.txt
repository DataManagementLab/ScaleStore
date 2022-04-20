# ---------------------------------------------------------------------------
# ScaleStore DB
# ---------------------------------------------------------------------------

project(scalestoredb)
cmake_minimum_required(VERSION 3.7)

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -g")

if (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  add_compile_options(-Wall -Wextra -Werror -mavx2 -mcx16 -m64) # -fno-elide-constructors no need for now
else()
  add_compile_options(-Wall -Wextra -march=native)
endif()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
  message("${Red}!ATTENTION: debug mode enabled! ${ColourReset}")
else()
  message("${BoldGreen}Release mode: ${ColourReset} " ${CMAKE_BUILD_TYPE})
endif (CMAKE_BUILD_TYPE STREQUAL "Debug")


find_package(Threads REQUIRED)
set(THREADS_PREFER_PTHREAD_FLAG ON)

# ---------------------------------------------------------------------------
# Includes
# ---------------------------------------------------------------------------
include("${CMAKE_SOURCE_DIR}/shared-headers/local.cmake")
include("${CMAKE_SOURCE_DIR}/libs/gflags.cmake")
# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

add_subdirectory("backend")

# ---------------------------------------------------------------------------
# Executable
# ---------------------------------------------------------------------------

add_subdirectory("frontend")
