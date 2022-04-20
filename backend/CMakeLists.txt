# ---------------------------------------------------------------------------
# Scalestore
# ---------------------------------------------------------------------------
project(scalestore)

# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------
file(GLOB_RECURSE SCALESTORE_CC **.cpp **/**.cpp **.hpp **/**.hpp)
if (NOT UNIX)
    message(SEND_ERROR "unsupported platform")
endif ()

# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------
add_library(scalestore STATIC ${SCALESTORE_CC})

OPTION(SANI "Compile scalestore with sanitizers" OFF)
IF(SANI)
  if (CMAKE_BUILD_TYPE MATCHES Debug)
    message("Compiling with Sanitizers")
    target_compile_options(scalestore PUBLIC -fsanitize=address)
    target_link_libraries(scalestore asan)
    # target_compile_options(scalestore PUBLIC -fsanitize=undefined)
    # target_link_libraries(scalestore ubsan)
  endif ()
ENDIF(SANI)

target_link_libraries(scalestore gflags Threads::Threads atomic numa rdmacm ibverbs aio) 
target_include_directories(scalestore PUBLIC ${SHARED_INCLUDE_DIRECTORY})
target_include_directories(scalestore PRIVATE ${CMAKE_CURRENT_LIST_DIR})
# ---------------------------------------------------------------------------
set(SCALESTORE_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR})
set_property(TARGET scalestore APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${SCALESTORE_INCLUDE_DIR})
