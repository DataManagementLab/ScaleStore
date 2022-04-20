import config
from distexprunner import *


parameter_grid = ParameterGrid(
    # dramGB=[150],
    dramGB=[150],
    dramGBCompute =[10],
    # numberNodes= range(2,6),
    numberNodes= [5],
    numberStorage =[2],
    zipf= [0],
    readRatio=[100],      # A, B, C workloads
    # readRatio=[100],      # A, B, C workloads 
    initialFillDegree = [100], # 90 percentage on 2 nodes and the we scale compute and bw keep data fixed 
    pp=[4],
    fp=[1],
    RUNS=range(1,5),
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
def ycsbBenchmark(servers, dramGB, dramGBCompute, numberNodes, numberStorage,zipf, readRatio, initialFillDegree ,pp,fp,RUNS):
    servers.cd("/home/tziegler/scalestore/build/frontend")
    
    sizeBytes = dramGB * 1024 * 1024 * 1024
    numTuples = int(((int(sizeBytes * (initialFillDegree / 100)  / YCSB_TUPLE_SIZE)) / 2))

    cmds = []

    for i in range(0, numberNodes):
        if i < numberStorage:
            cmd = f'numactl --membind=0 --cpunodebind=0 ./ycsb_nam -worker=20 -dramGB={dramGB} -nodes={numberNodes} -messageHandlerThreads=4  -ownIp={servers[i].ibIp} -pageProviderThreads={pp} -coolingPercentage=10 -freePercentage={fp} -csvFile=ycsb_nam_scale_compute_{RUNS}.csv -YCSB_read_ratio={readRatio} -YCSB_run_for_seconds=30 -YCSB_tuple_count={numTuples} -YCSB_zipf_factor={zipf} -evictCoolestEpochs=0.5 --ssd_path={servers[i].ssdPath} --ssd_gib=2000 -NAM_storage_nodes={numberStorage}'
            cmds += [servers[i].run_cmd(cmd)]
        else:
            cmd = f'numactl --membind=0 --cpunodebind=0 ./ycsb_nam -worker=20 -dramGB={dramGBCompute} -nodes={numberNodes} -messageHandlerThreads=4   -ownIp={servers[i].ibIp} -pageProviderThreads={pp} -coolingPercentage=10 -freePercentage={fp} -csvFile=ycsb_nam_scale_compute_{RUNS}.csv -YCSB_read_ratio={readRatio} -YCSB_run_for_seconds=30 -YCSB_tuple_count={numTuples} -YCSB_zipf_factor={zipf} -evictCoolestEpochs=0.5 --ssd_path={servers[i].ssdPath} --ssd_gib=2000 -NAM_storage_nodes={numberStorage}'
            cmds += [servers[i].run_cmd(cmd)]
    
    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART
