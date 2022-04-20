import config
from distexprunner import *



@reg_exp(servers=config.server_list, raise_on_rc=True)
def buffered_stdout(servers):
    s = servers[0]

    code = r"""
    #include <stdio.h>
    #include <unistd.h>

    int main(void) {
        for (int i = 0; i < 10; i++) {
            printf("%d\n", i);
            usleep(1000000);
        }
        return 0;
    }
    """
    exe = 'unbuffered'

    cmd = s.run_cmd(f'gcc -xc - -o {exe}')
    cmd.stdin(code, close=True)
    cmd.wait()

    s.run_cmd(f'./{exe}', stdout=Console()).wait()
    s.run_cmd(f'rm -f {exe}').wait()