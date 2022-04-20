import config
from distexprunner import *


parameter_grid = ParameterGrid(
    dramGB=[150],
    factor_mem_ssd=[10],
    # numberNodes= range(1,6),
    numberNodes= [5],
    pp=[2],
    fp=[1],
    workers = range(1,25),
    partitioned= ["OLAP_partitioned", "noOLAP_partitioned"],
    # partitioned= ["OLAP_partitioned"],    
    # partitioned= ["noOLAP_partitioned"],
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

    

@reg_exp(servers=config.server_list, params=parameter_grid, raise_on_rc=False)
def ycsbBenchmark(servers, dramGB, factor_mem_ssd, numberNodes ,pp,fp,workers, partitioned):
    servers.cd("/home/tziegler/scalestore/build/frontend")

    cmds = []
    for i in range(0,numberNodes):
        print(i)
        cmd = f'blkdiscard {servers[i].ssdPath}'
        cmds += [servers[i].run_cmd(cmd)]
        
    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART
    
    cmds = []
    for i in range(0, numberNodes):
        cmd = f'numactl --membind=0 --cpunodebind=0 ./OLAP -worker={workers} -dramGB={dramGB} -nodes={numberNodes} -messageHandlerThreads=2   -ownIp={servers[i].ibIp} -pageProviderThreads={pp} -coolingPercentage=10 -freePercentage={fp} -csvFile=olap_scale_server.csv  -OLAP_run_for_seconds=30 --ssd_path={servers[i].ssdPath} --ssd_gib=2000 --OLAP_factor_mem_ssd={factor_mem_ssd} -{partitioned} -tag={partitioned} -evictCoolestEpochs=0.5'
        cmds += [servers[i].run_cmd(cmd)]
    
    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART
