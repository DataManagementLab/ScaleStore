add_executable(frontend frontend.cpp)
add_dependencies(frontend scalestore)
target_link_libraries(frontend scalestore numa)

add_executable(OLAP OLAP.cpp)
add_dependencies(OLAP scalestore)
target_link_libraries(OLAP scalestore numa)

add_executable(ycsb ycsb/ycsb.cpp)
add_dependencies(ycsb scalestore)
target_link_libraries(ycsb scalestore numa)

add_executable(ycsb_latency ycsb/ycsb_latency.cpp)
add_dependencies(ycsb_latency scalestore)
target_link_libraries(ycsb_latency scalestore numa)

add_executable(zipf ycsb/zipf_distribution.cpp)
add_dependencies(zipf scalestore)
target_link_libraries(zipf scalestore numa)


add_executable(ycsb_in_memory_tree ycsb/ycsb_in_memory_tree.cpp)
add_dependencies(ycsb_in_memory_tree scalestore)
target_link_libraries(ycsb_in_memory_tree scalestore numa)

add_executable(ycsb_nam ycsb/ycsb_nam.cpp)
add_dependencies(ycsb_nam scalestore)
target_link_libraries(ycsb_nam scalestore numa)

add_executable(ycsb_workload_change ycsb/ycsb_workload_change.cpp)
add_dependencies(ycsb_workload_change scalestore)
target_link_libraries(ycsb_workload_change scalestore numa)

add_executable(bm_hashtable benchmarks/hashTable.cpp)
add_dependencies(bm_hashtable scalestore)
target_link_libraries(bm_hashtable scalestore numa)

add_executable(bm_sampling benchmarks/samplingSpeed.cpp)
add_dependencies(bm_sampling scalestore)
target_link_libraries(bm_sampling scalestore numa)

add_executable(bm_ssd_benchmark benchmarks/ssd_benchmark.cpp)
add_dependencies(bm_ssd_benchmark scalestore)
target_link_libraries(bm_ssd_benchmark scalestore numa)

add_executable(bm_pageprovider benchmarks/pageProvider.cpp)
add_dependencies(bm_pageprovider scalestore)
target_link_libraries(bm_pageprovider scalestore numa)

add_executable(bm_random benchmarks/random.cpp)
add_dependencies(bm_random scalestore)
target_link_libraries(bm_random scalestore numa)

add_executable(consistencyCheck consistency/BTreeCheck.cpp)
add_dependencies(consistencyCheck scalestore)
target_link_libraries(consistencyCheck scalestore numa)

add_executable(contention consistency/Contention.cpp)
add_dependencies(contention scalestore)
target_link_libraries(contention scalestore numa)


add_executable(btreeTest tests/BTreeTest.cpp)
add_dependencies(btreeTest scalestore)
target_link_libraries(btreeTest scalestore numa)

add_executable(tpcc tpcc/tpcc.cpp)
add_dependencies(tpcc scalestore)
target_link_libraries(tpcc scalestore numa)


