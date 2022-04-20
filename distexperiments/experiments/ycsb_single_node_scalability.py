import config
from distexprunner import *

NUMBER_NODES = 1

parameter_grid = ParameterGrid(
    dramGB=[150],
    numberNodes= [NUMBER_NODES],
    zipf= [0],
    readRatio=[100],      # A, B, C workloads
    pp=[2],
    fp=[1],
    threads=range(1,21),
    partitioned= ["noYCSB_partitioned"]
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
def ycsbBenchmark(servers, dramGB, numberNodes, zipf, readRatio, pp,fp,threads,partitioned):
    servers.cd("/home/tziegler/scalestore/build/frontend")

    cmds = []
    for i in range(0,len(servers)):
        print(i)
        cmd = f'blkdiscard {servers[i].ssdPath}'
        cmds += [servers[i].run_cmd(cmd)]

    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART    

    numTuples = 100000000

    z = float(zipf)/100
    cmds = []

            
    for i in range(0, numberNodes):
        cmd = f'numactl --membind=0 --cpunodebind=0 ./ycsb -worker={threads} -dramGB={dramGB} -nodes={numberNodes} -messageHandlerThreads=4   -ownIp={servers[i].ibIp} -pageProviderThreads={pp} -coolingPercentage=10 -freePercentage={fp} -csvFile=ycsb_single_node_scalability.csv -YCSB_read_ratio={readRatio} -YCSB_run_for_seconds=20 -YCSB_tuple_count={numTuples} -YCSB_zipf_factor={z} -tag=NO_DELEGATE -evictCoolestEpochs=0.5 --ssd_path={servers[i].ssdPath} --ssd_gib=2000 -YCSB_warm_up -{partitioned} -tag={partitioned}'
        cmds += [servers[i].run_cmd(cmd)]
    
    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART
    
