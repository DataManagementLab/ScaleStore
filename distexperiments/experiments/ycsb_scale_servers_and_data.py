import config
from distexprunner import *


parameter_grid = ParameterGrid(
    dramGB=[50],
    numberNodes= range(1,7),
    zipf= [0],
    readRatio=[50,95,100],      # A, B, C workloads
    fillDegree= [80],   # from 5 to 90 percent filled per node
    pp=[2],
    fp=[1],
    partitioned= ["YCSB_partitioned", "noYCSB_partitioned"],
)


@reg_exp(servers=config.server_list)
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
@reg_exp(servers=config.server_list, params=parameter_grid, raise_on_rc=False)
def ycsbBenchmark(servers, dramGB, numberNodes, zipf, readRatio, fillDegree,pp,fp, partitioned):
    servers.cd("/home/tziegler/scalestore/build/frontend")
    
    sizeBytes = dramGB * 1024 * 1024 * 1024
    numTuples = int(((int(sizeBytes * (fillDegree / 100)  / YCSB_TUPLE_SIZE)) * numberNodes) / 2)

    cmds = []

    for i in range(0, numberNodes):
        cmd = f'numactl --membind=0 --cpunodebind=0 ./ycsb -worker=20 -dramGB={dramGB} -nodes={numberNodes} -messageHandlerThreads=4   -ownIp={servers[i].ibIp} -pageProviderThreads={pp} -coolingPercentage=10 -freePercentage={fp} -csvFile=ycsb_scale_server_and_data.csv -YCSB_read_ratio={readRatio} -YCSB_run_for_seconds=30 -YCSB_tuple_count={numTuples} -YCSB_zipf_factor={zipf} -{partitioned} -tag={partitioned} -evictCoolestEpochs=0.5'
        cmds += [servers[i].run_cmd(cmd)]
    
    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART
