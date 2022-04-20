import config
from distexprunner import *



class PackageList:
    def __init__(self):
        self.packages = {}
        
    def __call__(self, line):
        name, version = line.strip().split('|', 1)
        self.packages[name] = version

    def __repr__(self):
        items = ', '.join(f'{name}={version}' for name, version in self.packages.items())
        return f'<{self.__class__.__name__} [{items}]>'



@reg_exp(servers=ServerList())
def check_packages(servers):
    package_lists = IterClassGen(PackageList)
    apps = ['gcc', 'cmake', 'python3.7']
    cmd = f"dpkg-query -W -f='${{Package}}|${{Version}}\n' {' '.join(apps)}"

    procs = [s.run_cmd(cmd, stdout=next(package_lists)) for s in servers]
    [p.wait() for p in procs]

    for package_list in package_lists:
        assert(len(package_list.packages) == len(apps))


