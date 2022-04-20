from distexprunner import ServerList, Server


SERVER_PORT = 20000


server_list = ServerList(
    Server('node01', '127.0.0.1', SERVER_PORT),
    Server('node02', '127.0.0.1', SERVER_PORT),
    Server('node03', '127.0.0.1', SERVER_PORT),
    Server('node04', '127.0.0.1', SERVER_PORT),
    Server('node05', '127.0.0.1', SERVER_PORT),
    #Server('node0x', '192.168.94.2x', SERVER_PORT),
)
