import os
import signal
from multiprocessing import Process
from typing import List

from dsm.epaxos.net.impl.generic.cli import replica_client, replica_server
from dsm.epaxos.replica.quorum.ev import ReplicaAddress
from dsm.epaxos.net.impl.udp.client import UDPReplicaClient
from dsm.epaxos.net.impl.udp.server import UDPReplicaServer
# from dsm.epaxos.net.impl.zeromq.client import ZMQReplicaClient
# from dsm.epaxos.net.impl.zeromq.server import ZMQReplicaServer

replicas = {
    i: ReplicaAddress(f'tcp://0.0.0.0:{60000 + i}', f'tcp://0.0.0.0:{61000+i}') for i in range(1, 6)
}

clients = list(range(100, 105))


def main():
    # server_cls, client_cls = ZMQReplicaServer, ZMQReplicaClient
    server_cls, client_cls = UDPReplicaServer, UDPReplicaClient

    ress = []  # type: List[Process]
    for replica_id in replicas.keys():
        res = Process(target=replica_server, args=(server_cls, 0, replica_id, replicas),
                      name=f'dsm-replica-{replica_id}')
        ress.append(res)
    for client_id in clients:
        res = Process(target=replica_client, args=(client_cls, client_id, replicas), name=f'dsm-client-{client_id}')
        ress.append(res)
    for res in ress:
        res.start()
    try:
        for res in ress:
            res.join()
    except:
        for res in ress:
            os.kill(res.pid, signal.SIGTERM)
    finally:
        for res in ress:
            res.join()


if __name__ == '__main__':
    main()
