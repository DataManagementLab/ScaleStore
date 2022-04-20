#experiment to see adaptive partitioning in action
# changes partitioning every 30 seconds in a round robin fashion
import config
from distexprunner import *

NUMBER_NODES = 6
# NUMBER_NODES = 3
parameter_grid = ParameterGrid(
    dramGB=[50],
    numberNodes= [NUMBER_NODES],
    zipf = [0],
    # zipf= range(0,100,30),
    readRatio=[50,95,100],      # A, B, C workloads
    # readRatio=[100],      # A, B, C workloads
    fillDegree= [80],   # from 5 to X percent filled per node
    pp=[2],
    fp=[1],
)


@reg_exp(servers=config.server_list[:NUMBER_NODES])
def compile(servers):
    servers.cd("/home/tziegler/scalestore/build")
    cmake_cmd = f'cmake -DSANI=OFF  -DCMAKE_BUILD_TYPE=Release ..'
    procs = [s.run_cmd(cmake_cmd) for s in servers]
    assert(all(p.wait() == 0 for p in procs))

    make_cmd = f'make -j'
    procs = [s.run_cmd(make_cmd) for s in servers]
    assert(all(p.wait() == 0 for p in procs))
    

PAGE_SIZE = 8192
YCSB_TUPLE_SIZE = 128 + 8
@reg_exp(servers=config.server_list[:NUMBER_NODES], params=parameter_grid, raise_on_rc=False)
def ycsbBenchmark(servers, dramGB, numberNodes, zipf, readRatio, fillDegree ,pp,fp):
    servers.cd("/home/tziegler/scalestore/build/frontend")
    
    sizeBytes = dramGB * 1024 * 1024 * 1024
    numTuples = int(((int(sizeBytes * (fillDegree / 100)  / YCSB_TUPLE_SIZE)) * numberNodes) / 2)

    cmds = []

    for i in range(0, numberNodes):
        cmd = f'numactl --membind=0 --cpunodebind=0 ./ycsb_workload_change -worker=20 -dramGB={dramGB} -nodes={numberNodes} -messageHandlerThreads=4   -ownIp={servers[i].ibIp} -pageProviderThreads={pp} -coolingPercentage=10 -freePercentage={fp} -csvFile=ycsb_workload_change_new_eviction.csv -YCSB_read_ratio={readRatio} -YCSB_run_for_seconds=40 -YCSB_tuple_count={numTuples} -YCSB_zipf_factor={zipf} -tag={fillDegree}'
        cmds += [servers[i].run_cmd(cmd)]
    
    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART
