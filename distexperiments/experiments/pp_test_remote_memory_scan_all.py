#experiment to see adaptive partitioning in action
# changes partitioning every 30 seconds in a round robin fashion
import config
from distexprunner import *

NUMBER_NODES = 5
parameter_grid = ParameterGrid(
    dramGB=[50],
    numberNodes= [NUMBER_NODES],
    fillDegree=[90],   # from 5 to 90 percent filled per node
    # fillDegree= range(10,100,20),   # from 5 to 90 percent filled per node
    seconds=[60],
    pp=[4],
    fp=[5],
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
def ycsbBenchmark(servers, dramGB, numberNodes,fillDegree,seconds,pp,fp):
    servers.cd("/home/tziegler/scalestore/build/frontend")
     
    cmds = []

    fd = fillDegree / 100
    for i in range(0, numberNodes):
        cmd = f'numactl --membind=0 --cpunodebind=0 ./pp_test_scan_perf_all -worker=20 -dramGB={dramGB} -nodes={numberNodes} -messageHandlerThreads=4   -ownIp={servers[i].ibIp} -pageProviderThreads={pp} -coolingPercentage=10 -freePercentage={fp} -csvFile=pp_test_scan_remote_memory_all_optimized.csv -fill_degree={fd} -run_for_seconds={seconds}'
        cmds += [servers[i].run_cmd(cmd)]
    
    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART

