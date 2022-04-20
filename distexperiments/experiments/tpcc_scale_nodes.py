import config
from distexprunner import *

NUMBER_NODES = 4

parameter_grid = ParameterGrid(
    dramGB=[100],
    numberNodes= [1,2,3,4],
    probSSD=[100],
    whc=[20],
    locality=["","-tpcc_warehouse_affinity","-tpcc_warehouse_locality"],
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
    

@reg_exp(servers=config.server_list, params=parameter_grid, raise_on_rc=True, max_restarts=1)
def tpccBenchmark(servers, dramGB, numberNodes,probSSD, whc, locality ,RUNS):    
    servers.cd("/home/tziegler/scalestore/build/frontend")

    cmds = []
    for i in range(0, numberNodes):
        print(i)
        cmd = f'blkdiscard {servers[i].ssdPath}'
        cmds += [servers[i].run_cmd(cmd)]

    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART    

    warehouses = whc*numberNodes;
    cmds = []
    for i in range(0, numberNodes):
        cmd = f'numactl --membind=0 --cpunodebind=0 ./tpcc -worker=20 -dramGB={dramGB} -nodes={numberNodes} -messageHandlerThreads=4   -ownIp={servers[i].ibIp} -csvFile=tpcc_nodes_scalability.csv --ssd_path={servers[i].ssdPath} --ssd_gib=2000 -prob_SSD={probSSD} -tpcc_warehouse_count={warehouses} -TPCC_run_for_seconds=30 {locality}'

        cmds += [servers[i].run_cmd(cmd)]
    
    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART
    
