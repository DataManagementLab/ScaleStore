import config
from distexprunner import *


server_list = config.server_list[0, ]


@reg_exp(servers=server_list, max_restarts=3, raise_on_rc=False)
def restart(servers):
    for s in servers:
        cmd = s.run_cmd(f'date && sleep 0.1 && exit 1', stdout=Console(fmt=f'{s.id}: %s'))
        if cmd.wait() != 0:
            return Action.RESTART