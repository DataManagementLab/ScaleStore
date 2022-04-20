from distexprunner import ServerList, Server


SERVER_PORT = 20005


server_list = ServerList(
    # fill in
    Server('node08', 'c08.lab.dm.informatik.tu-darmstadt.de', SERVER_PORT, ibIp='172.18.94.80', ssdPath="/dev/md0"),
    Server('node07', 'c07.lab.dm.informatik.tu-darmstadt.de', SERVER_PORT, ibIp='172.18.94.70', ssdPath="/dev/md0 "),
    # Server('node03', 'c03.lab.dm.informatik.tu-darmstadt.de', SERVER_PORT, ibIp='172.18.94.30', ssdPath="/dev/md0"),
    Server('node02', 'c02.lab.dm.informatik.tu-darmstadt.de', SERVER_PORT, ibIp='172.18.94.20', ssdPath="/dev/md0"),
    # Server('node05', 'c05.lab.dm.informatik.tu-darmstadt.de', SERVER_PORT, ibIp='172.18.94.50', ssdPath="/dev/md0"),
    Server('node01', 'c01.lab.dm.informatik.tu-darmstadt.de', SERVER_PORT, ibIp='172.18.94.10', ssdPath="/dev/md0"),
    Server('node04', 'c04.lab.dm.informatik.tu-darmstadt.de', SERVER_PORT, ibIp='172.18.94.40', ssdPath="/dev/md127"),
)
