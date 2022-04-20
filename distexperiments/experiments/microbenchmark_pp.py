import config
from distexprunner import *

NUMBER_NODES = 4

parameter_grid = ParameterGrid(
    dramGB=[50],
    # numberNodes= range(2,5),
    numberNodes=[4],
    initialFillDegree = [180], # 90 percentage on 2 nodes and the we scale compute and bw keep data fixed 
    pp=[4],
    fp=[1],
    partitioned= ["noYCSB_partitioned"],
    evict_ssd = ["noevict_to_ssd"],
    pp_batch = [16,32,64,128,256,512,1024,2048],
    rdma_batch = [16,32,64,128,256,512,1024,2048],
)


def set_benchmark_config(servers, pp_batch, rdma_batch):

    rdma_batch_size_cmd = f"sed -i '15 s/constexpr uint64_t batchSize.*/constexpr uint64_t batchSize = {rdma_batch};/' ~/scalestore/backend/scalestore/storage/buffermanager/PageProvider.hpp"
    procs = [s.run_cmd(rdma_batch_size_cmd) for s in servers]
    assert(all(p.wait() == 0 for p in procs))

    pp_batch_size_cmd = f"sed -i '16 s/constexpr uint64_t pp_batch_size.*/constexpr uint64_t pp_batch_size = {pp_batch};/' ~/scalestore/backend/scalestore/storage/buffermanager/PageProvider.hpp"
    procs = [s.run_cmd(pp_batch_size_cmd) for s in servers]
    assert(all(p.wait() == 0 for p in procs))
        

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
def ycsbBenchmark(servers, dramGB, numberNodes, initialFillDegree ,pp,fp, partitioned, evict_ssd,pp_batch,rdma_batch):

    if pp_batch < rdma_batch:
        return
    
    set_benchmark_config(servers,pp_batch,rdma_batch)
    compile(servers)

    servers.cd("/home/tziegler/scalestore/build/frontend")
    sizeBytes = dramGB * 1024 * 1024 * 1024
    numTuples = int(((int(sizeBytes * (initialFillDegree / 100)  / YCSB_TUPLE_SIZE)) / 2))

    cmds = []

    for i in range(0, numberNodes):
        cmd = f'numactl --membind=0 --cpunodebind=0 ./ycsb -worker=20 -dramGB={dramGB} -nodes={numberNodes} -messageHandlerThreads=4   -ownIp={servers[i].ibIp} -pageProviderThreads={pp} -coolingPercentage=10 -freePercentage={fp} -csvFile=microbenchmark_pp.csv -YCSB_run_for_seconds=30 -YCSB_tuple_count={numTuples} -{partitioned} -{evict_ssd} -tag=rdma_{rdma_batch}_pp_{pp_batch} -evictCoolestEpochs=0.99  -ssd_path={servers[i].ssdPath} -ssd_gib=2000 -YCSB_all_workloads '
        cmds += [servers[i].run_cmd(cmd)]
    
    if not all(cmd.wait() == 0 for cmd in cmds):
        return Action.RESTART
