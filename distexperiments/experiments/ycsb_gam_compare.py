import config
from distexprunner import *


parameter_grid = ParameterGrid(
    dramGB=[30],
    # numberNodes= range(1,5),
    # numberNodes= [2,4],
    numberNodes= [1],
    zipf= [0],
    # readRatio=[50,95,100],      # A, B, C workloads
    pp=[2],
    fp=[1],
    threads=[1,20],
    # threads=[1],
    partitioned= ["noYCSB_partitioned"],
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
def ycsbBenchmark(servers, dramGB, numberNodes, zipf,pp,fp,threads, partitioned):
    servers.cd("/home/tziegler/scalestore/build/frontend")

    
    numTuples = int(50000000/numberNodes)

    cmds = []
    # mh = numberNodes-1
    mh=1
    for i in range(0, numberNodes):
        cmd = f'numactl --membind=0 --cpunodebind=0 ./ycsb -worker={threads} -dramGB={dramGB} -nodes={numberNodes} -messageHandlerThreads={mh}  -ownIp={servers[i].ibIp} -pageProviderThreads={pp} -coolingPercentage=10 -freePercentage={fp} -csvFile=ycsb_gam_comparision_wo_value.csv -YCSB_run_for_seconds=30 -YCSB_tuple_count={numTuples} -YCSB_zipf_factor={zipf} -{partitioned} -tag={partitioned} -evictCoolestEpochs=0.5 --ssd_path={servers[i].ssdPath} --ssd_gib=2000 --YCSB_all_workloads --YCSB_warm_up'
        cmds += [servers[i].run_cmd(cmd)]
    
    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART
