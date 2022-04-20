FIND_PATH(IBVERBS_INCLUDE_DIR infiniband/verbs.h
  PATHS
  $ENV{IBVERBS_HOME}
  NO_DEFAULT_PATH
    PATH_SUFFIXES include
)

FIND_PATH(IBVERBS_INCLUDE_DIR infiniband/verbs.h
  PATHS
  /usr/local/include
  /usr/include
  /sw/include # Fink
  /opt/local/include # DarwinPorts
  /opt/csw/include # Blastwave
  /opt/include
)

FIND_LIBRARY(IBVERBS_LIBRARY 
  NAMES ibverbs
  PATHS $ENV{IBVERBS_HOME}
    NO_DEFAULT_PATH
    PATH_SUFFIXES lib64 lib
)

FIND_LIBRARY(IBVERBS_LIBRARY 
  NAMES ibverbs
  PATHS
    /usr/local
    /usr
    /sw
    /opt/local
    /opt/csw
    /opt
    /usr/freeware
    PATH_SUFFIXES lib64 lib
)
SET(IBVERBS_FOUND FALSE)
IF(IBVERBS_LIBRARY AND IBVERBS_INCLUDE_DIR)
  SET(IBVERBS_FOUND TRUE)
ENDIF(IBVERBS_LIBRARY AND IBVERBS_INCLUDE_DIR)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(IBVerbs DEFAULT_MSG IBVERBS_LIBRARY IBVERBS_INCLUDE_DIR)

mark_as_advanced(IBVERBS_INCLUDE_DIR IBVERBS_LIBRARIES)
