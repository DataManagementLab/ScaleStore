import config
from distexprunner import *



# pre-select by lambda function
@reg_exp(servers=config.server_list[lambda s: hasattr(s, 'id')])
def serverlist_ops(servers):
    # select by index
    s = servers[0]
    s.run_cmd(f'echo "{s.id}"', stdout=Console()).wait()

    # select by id
    s = servers['node02']
    s.run_cmd(f'echo "{s.id}"', stdout=Console()).wait()

    # select whole list
    for s in servers:
        s.run_cmd(f'echo "{s.id}"', stdout=Console()).wait()

    # select by lambda
    for s in servers[lambda s: s.id >= 'node03']:
        s.run_cmd(f'echo "{s.id}"', stdout=Console()).wait()

    # select by slice
    for s in servers[1:3]:
        s.run_cmd(f'echo "{s.id}"', stdout=Console()).wait()

    # select reverse order
    for s in servers[::-1]:
        s.run_cmd(f'echo "{s.id}"', stdout=Console()).wait()

    # select even entries
    for s in servers[::2]:
        s.run_cmd(f'echo "{s.id}"', stdout=Console()).wait()

    # select by tuple-list, a combination of all
    for s in servers['node04', 2, lambda s: s.id=='node01']:
        s.run_cmd(f'echo "{s.id}"', stdout=Console()).wait()
