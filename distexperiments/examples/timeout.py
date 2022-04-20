import config
from distexprunner import *


server_list = config.server_list[0, ]


@reg_exp(servers=server_list, raise_on_rc=False, max_restarts=3)
def timeout(servers):
    rc = servers[0].run_cmd('sleep infinity', timeout=1).wait()
    if rc == ReturnCode.TIMEOUT:
        return Action.RESTART