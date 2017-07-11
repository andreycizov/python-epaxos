from multiprocessing.pool import Pool

from dsm.epaxos.network.zeromq import replica_server, ReplicaAddress, replica_client

replicas = {
    1: ReplicaAddress('tcp://0.0.0.0:50001', 'tcp://0.0.0.0:60001'),
    2: ReplicaAddress('tcp://0.0.0.0:50002', 'tcp://0.0.0.0:60002'),
    3: ReplicaAddress('tcp://0.0.0.0:50003', 'tcp://0.0.0.0:60003'),
}

clients = [
    103,
    104
]


def main():
    with Pool(len(replicas) + len(clients)) as pool:
        ress = []
        for replica_id in replicas.keys():
            res = pool.apply_async(replica_server, (0, replica_id, replicas))
            ress.append(res)
        for client_id in clients:
            res = pool.apply_async(replica_client, (client_id, replicas))
            ress.append(res)
        try:
            pass
        finally:
            for res in ress:
                res.get()


if __name__ == '__main__':
    main()
