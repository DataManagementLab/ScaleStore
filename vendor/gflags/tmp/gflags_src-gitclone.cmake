
if(NOT "/home/tobias/git/scalestore/vendor/gflags/src/gflags_src-stamp/gflags_src-gitinfo.txt" IS_NEWER_THAN "/home/tobias/git/scalestore/vendor/gflags/src/gflags_src-stamp/gflags_src-gitclone-lastrun.txt")
  message(STATUS "Avoiding repeated git clone, stamp file is up to date: '/home/tobias/git/scalestore/vendor/gflags/src/gflags_src-stamp/gflags_src-gitclone-lastrun.txt'")
  return()
endif()

execute_process(
  COMMAND ${CMAKE_COMMAND} -E remove_directory "/home/tobias/git/scalestore/vendor/gflags/src/gflags_src"
  RESULT_VARIABLE error_code
  )
if(error_code)
  message(FATAL_ERROR "Failed to remove directory: '/home/tobias/git/scalestore/vendor/gflags/src/gflags_src'")
endif()

# try the clone 3 times in case there is an odd git clone issue
set(error_code 1)
set(number_of_tries 0)
while(error_code AND number_of_tries LESS 3)
  execute_process(
    COMMAND "/usr/bin/git"  clone --no-checkout "https://github.com/gflags/gflags.git" "gflags_src"
    WORKING_DIRECTORY "/home/tobias/git/scalestore/vendor/gflags/src"
    RESULT_VARIABLE error_code
    )
  math(EXPR number_of_tries "${number_of_tries} + 1")
endwhile()
if(number_of_tries GREATER 1)
  message(STATUS "Had to git clone more than once:
          ${number_of_tries} times.")
endif()
if(error_code)
  message(FATAL_ERROR "Failed to clone repository: 'https://github.com/gflags/gflags.git'")
endif()

execute_process(
  COMMAND "/usr/bin/git"  checkout f8a0efe03aa69b3336d8e228b37d4ccb17324b88 --
  WORKING_DIRECTORY "/home/tobias/git/scalestore/vendor/gflags/src/gflags_src"
  RESULT_VARIABLE error_code
  )
if(error_code)
  message(FATAL_ERROR "Failed to checkout tag: 'f8a0efe03aa69b3336d8e228b37d4ccb17324b88'")
endif()

set(init_submodules TRUE)
if(init_submodules)
  execute_process(
    COMMAND "/usr/bin/git"  submodule update --recursive --init 
    WORKING_DIRECTORY "/home/tobias/git/scalestore/vendor/gflags/src/gflags_src"
    RESULT_VARIABLE error_code
    )
endif()
if(error_code)
  message(FATAL_ERROR "Failed to update submodules in: '/home/tobias/git/scalestore/vendor/gflags/src/gflags_src'")
endif()

# Complete success, update the script-last-run stamp file:
#
execute_process(
  COMMAND ${CMAKE_COMMAND} -E copy
    "/home/tobias/git/scalestore/vendor/gflags/src/gflags_src-stamp/gflags_src-gitinfo.txt"
    "/home/tobias/git/scalestore/vendor/gflags/src/gflags_src-stamp/gflags_src-gitclone-lastrun.txt"
  RESULT_VARIABLE error_code
  )
if(error_code)
  message(FATAL_ERROR "Failed to copy script-last-run stamp file: '/home/tobias/git/scalestore/vendor/gflags/src/gflags_src-stamp/gflags_src-gitclone-lastrun.txt'")
endif()

