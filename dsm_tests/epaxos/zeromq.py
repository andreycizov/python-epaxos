import os
import signal
from multiprocessing import Process
from typing import List

from dsm.epaxos.network.impl.zeromq.impl import ReplicaAddress, replica_client, replica_server

replicas = {
    1: ReplicaAddress('tcp://0.0.0.0:60001'),
    2: ReplicaAddress('tcp://0.0.0.0:60002'),
    3: ReplicaAddress('tcp://0.0.0.0:60003'),
    4: ReplicaAddress('tcp://0.0.0.0:60004'),
    5: ReplicaAddress('tcp://0.0.0.0:60005'),
}

clients = list(range(100, 110))


def main():
    ress = []  # type: List[Process]
    for replica_id in replicas.keys():
        res = Process(target=replica_server, args=(0, replica_id, replicas), name=f'dsm-replica-{replica_id}')
        ress.append(res)
    for client_id in clients:
        res = Process(target=replica_client, args=(client_id, replicas), name=f'dsm-client-{client_id}')
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
