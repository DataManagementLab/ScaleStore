import config
from distexprunner import *



server_list = ServerList(
    config.server_list[0],
    working_directory='/tmp'
)


@reg_exp(servers=server_list)
def cwd(servers):
    for s in servers:
        s.run_cmd('ls && pwd', stdout=Console(fmt=f'{s.id}: %s')).wait()

    servers.cd('/')
    for s in servers:
        s.run_cmd('ls && pwd', stdout=Console(fmt=f'{s.id}: %s')).wait()

    for s in servers:
        s.cd('/home')
        s.run_cmd('ls && pwd', stdout=Console(fmt=f'{s.id}: %s')).wait()