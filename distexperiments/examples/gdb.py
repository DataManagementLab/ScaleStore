import config
from distexprunner import *


ENABLED = False


@reg_exp(servers=config.server_list[0, ])
def gdb(servers):
    if not ENABLED:
        log('Skipping, not enabled')
        return
    s = servers[0]

    code = r"""
    #include <stdio.h>
    #include <unistd.h>

    int main(void) {
        int i = 42;
        int *p = NULL;
        *p = i;
        return 0;
    }
    """
    exe = 'unbuffered'

    cmd = s.run_cmd(f'gcc -g -xc - -o {exe}')
    cmd.stdin(code, close=True)
    cmd.wait()

    controller = StdinController()

    output = File('gdb.log', flush=True)
    cmd = s.run_cmd(f'{GDB} ./{exe}', stdout=output, stderr=output, stdin=controller)

    controller.wait()

    cmd.wait()

    s.run_cmd(f'rm -f {exe}').wait()