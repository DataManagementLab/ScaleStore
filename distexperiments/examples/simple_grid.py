import config
from distexprunner import *



server_list = config.server_list[0, ]
parameter_grid = ParameterGrid(
    a=range(1, 5),
    b=[2, 4],
    to_file=[True, False],
    computed=ComputedParam(lambda to_file: [1] if to_file else 2)
)


@reg_exp(servers=server_list, params=parameter_grid)
def simple_grid(servers, a, b, to_file, computed):
    for s in servers:
        stdout = File('simple_grid.log', append=True)
        if not to_file:
            stdout = [stdout, Console(fmt=f'{s.id}: %s')]

        s.run_cmd(f'echo {a} {b} {computed}', stdout=stdout).wait()


@reg_exp(servers=ServerList())
def only_local(servers):
    File('simple_grid.log', append=False)('empty\n')
