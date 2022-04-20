import config
from distexprunner import *

NUMBER_NODES = 4

parameter_grid = ParameterGrid(
    dramGB=[100],
    numberNodes= [NUMBER_NODES],
    zipf= [0],
    # readRatio=[50,95,100],      # A, B, C workloads
    # fillDegree=[150, 125, 100, 90, 80, 70, 60, 50, 40, 30, 20, 10],
    fillDegree=[10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 125, 150],
    probSSD=[100],
    pp=[2],
    fp=[1],
    partitioned= ["noYCSB_partitioned"],
    RUNS=[1]
)


@reg_exp(servers=config.server_list[:NUMBER_NODES])
def compile(servers):
    servers.cd("/home/tziegler/scalestore/build")
    cmake_cmd = f'cmake -D CMAKE_C_COMPILER=gcc-10 -D CMAKE_CXX_COMPILER=g++-10 -DCMAKE_BUILD_TYPE=Release ..'
    procs = [s.run_cmd(cmake_cmd) for s in servers]
    assert(all(p.wait() == 0 for p in procs))

    make_cmd = f'make -j'
    procs = [s.run_cmd(make_cmd) for s in servers]
    assert(all(p.wait() == 0 for p in procs))
    

PAGE_SIZE = 8192
YCSB_TUPLE_SIZE = 128 + 8
@reg_exp(servers=config.server_list[:NUMBER_NODES], params=parameter_grid, raise_on_rc=True, max_restarts=1)
# def ycsbBenchmark(servers, dramGB, numberNodes, zipf, readRatio, fillDegree,probSSD, pp,fp,partitioned):
def ycsbBenchmark(servers, dramGB, numberNodes, zipf,fillDegree,probSSD, pp,fp,partitioned,RUNS):    
    servers.cd("/home/tziegler/scalestore/build/frontend")

    cmds = []
    for i in range(0,len(servers)):
        print(i)
        cmd = f'blkdiscard {servers[i].ssdPath}'
        cmds += [servers[i].run_cmd(cmd)]

    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART    
    
    sizeBytes = dramGB * 1024 * 1024 * 1024
    numTuples = int(((int(sizeBytes * (fillDegree / 100)  / YCSB_TUPLE_SIZE)) * numberNodes) / 2)

    z = float(zipf)/100
    cmds = []

            
    for i in range(0, numberNodes):
        cmd = f'numactl --membind=0 --cpunodebind=0 ./ycsb -worker=20 -dramGB={dramGB} -nodes={numberNodes} -messageHandlerThreads=4   -ownIp={servers[i].ibIp} -pageProviderThreads={pp} -coolingPercentage=10 -freePercentage={fp} -csvFile=ycsb_data_scalability_new_hashtable.csv -YCSB_run_for_seconds=20 -YCSB_tuple_count={numTuples} -YCSB_zipf_factor={z} -tag=NO_DELEGATE -evictCoolestEpochs=0.5 --ssd_path={servers[i].ssdPath} --ssd_gib=2000 -YCSB_warm_up -prob_SSD={probSSD} -YCSB_record_latency -YCSB_all_workloads -{partitioned} -tag={partitioned}'

        cmds += [servers[i].run_cmd(cmd)]
    
    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART
    
