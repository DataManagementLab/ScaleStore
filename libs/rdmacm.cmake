FIND_PATH(RDMACM_INCLUDE_DIR rdma/rdma_verbs.h
  PATHS
  $ENV{RDMACM_HOME}
  NO_DEFAULT_PATH
    PATH_SUFFIXES include
)

FIND_PATH(RDMACM_INCLUDE_DIR rdma/rdma_verbs.h
  PATHS
  /usr/local/include
  /usr/include
  /sw/include # Fink
  /opt/local/include # DarwinPorts
  /opt/csw/include # Blastwave
  /opt/include
)

FIND_LIBRARY(RDMACM_LIBRARY 
  NAMES rdmacm
  PATHS $ENV{RDMACM_HOME}
    NO_DEFAULT_PATH
    PATH_SUFFIXES lib64 lib
)

FIND_LIBRARY(RDMACM_LIBRARY 
  NAMES rdmacm
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
SET(RDMACM_FOUND FALSE)
IF(RDMACM_LIBRARY AND RDMACM_INCLUDE_DIR)
  SET(RDMACM_FOUND TRUE)
ENDIF(RDMACM_LIBRARY AND RDMACM_INCLUDE_DIR)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RdmaCm DEFAULT_MSG RDMACM_LIBRARY RDMACM_INCLUDE_DIR)

mark_as_advanced(RDMACM_INCLUDE_DIR RDMACM_LIBRARIES)
