import config
from distexprunner import *



server_list = config.server_list[0, ]


@reg_exp(servers=server_list)
def environment_variables(servers):
    for s in servers:
        s.run_cmd('env', env={'OMP_NUM_THREADS': 8}).wait()

