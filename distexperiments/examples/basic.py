import config
from distexprunner import *


server_list = config.server_list[0, ]

@reg_exp(servers=server_list)
def bash_for(servers):
    for s in servers[:1]:
        cmd = s.run_cmd('for i in {1..10}; do echo $i; sleep 0.1; done')
        # time.sleep(3) would block event loop processing
        sleep(3)
        cmd.wait()



@reg_exp(servers=server_list, raise_on_rc=False)
def kill_yes(servers):
    for s in servers[:1]:
        yes_cmd = s.run_cmd('yes > /dev/null')
        sleep(3)
        yes_cmd.kill()


@reg_exp(servers=server_list)
def read_stdin(servers):
    cmd = servers['node01'].run_cmd('read p && echo $p', stdout=Console(fmt='node01: %s'))
    cmd.stdin('hello\n')
    cmd.wait()


@reg_exp(servers=server_list)
def many_trees(servers):
    cmds = [servers[0].run_cmd('tree || ls') for _ in range(20)]
    assert(all(cmd.wait() == 0 for cmd in cmds))


@reg_exp(servers=server_list, raise_on_rc=False)
def exit_code(servers):
    cmds = [servers[0].run_cmd(f'exit {i}') for i in range(5)]
    assert(not all(cmd.wait() == 0 for cmd in cmds))