import config
from distexprunner import *


server_list = config.server_list[0, ]


import json
class String:
    def __init__(self):
        self.__s = []

    def __call__(self, line):
        self.__s.append(line)

    def __str__(self):
        return ''.join(self.__s)

    def json(self):
        return json.loads(str(self))


class HighLoad(Exception):
    pass


@reg_exp(servers=server_list)
def cpu_load(servers, cpu_limit=None, node_limit=None):

    output = String()
    rc = servers[0].run_cmd(f'mpstat -P ALL -N ALL -o JSON 1 1', stdout=output).wait()
    assert(rc == 0)
    
    stat = output.json()['sysstat']['hosts'][0]['statistics'][0]
    load = {
        'cpu': dict(map(lambda x: (x['cpu'], 100-x['idle']), stat['cpu-load'])),
        'node': dict(map(lambda x: (x['node'], 100-x['idle']), stat['node-load']))
    }
    for name, vals in load.items():
        log(f'{name}:')
        for idx, util in vals.items():
            log(f'\t{idx}: {util:0.2f}')
            

    if isinstance(cpu_limit, (int, float)):
        for cpu, util in load['cpu'].items():
            if util > cpu_limit:
                raise HighLoad(f'too high load for cpu {repr(cpu)}: {util:0.2f}% > {cpu_limit}')
    elif callable(cpu_limit):
        for cpu, util in load['cpu'].items():
            if not cpu_limit(cpu, util):
                raise HighLoad(f'too high load for cpu {repr(cpu)}: {util:0.2f}%  cpu_limit()=False')

    if isinstance(node_limit, (int, float)):
        for node, util in load['node'].items():
            if util > node_limit:
                raise HighLoad(f'too high load for node {repr(node)}: {util:0.2f}% > {node_limit}')
    elif callable(node_limit):
        for node, util in load['node'].items():
            if not node_limit(node, util):
                raise HighLoad(f'too high load for node {repr(node)}: {util:0.2f}%  node_limit()=False')

# node_limit=10
# cpu_limit=lambda c, u: u < 10