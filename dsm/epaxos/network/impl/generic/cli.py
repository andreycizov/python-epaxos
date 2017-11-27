import cProfile
import logging
import random
import signal
import time
from collections import deque
from typing import Dict, ClassVar

import sys

import numpy as np

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.network.impl.generic.client import ReplicaClient
from dsm.epaxos.network.impl.generic.server import ReplicaAddress, ReplicaServer

logger = logging.getLogger(__name__)


def cli_logger(level=logging.NOTSET):
    logger = logging.getLogger()

    if logger.hasHandlers():
        return logger

    logger.setLevel(level)

    ch = logging.StreamHandler(sys.stderr)
    format = logging.Formatter("[%(asctime)s][%(levelname)s][%(name)s]\t%(message)s")
    ch.setFormatter(format)
    ch.setLevel(logging.NOTSET)
    logger.addHandler(ch)

    return logger


def replica_server(cls: ClassVar[ReplicaServer], epoch: int, replica_id: int, replicas: Dict[int, ReplicaAddress]):
    profile = True
    if profile:
        pr = cProfile.Profile()
        pr.enable()

    # print('Calibrating profiler')
    # for i in range(5):
    #     print(pr.calibrate(10000))
    def receive_signal(*args):
        import sys
        logger.info('Writing results')
        if profile:
            pr.disable()
            pr.dump_stats(f'{replica_id}.profile')
        sys.exit()

    signal.signal(signal.SIGTERM, receive_signal)

    try:
        cli_logger()
        with cls(epoch, replica_id, replicas) as server:
            server.run()

    except:
        logger.exception(f'Server {replica_id}')
    finally:
        if profile:
            pr.disable()
            pr.dump_stats(f'{replica_id}.profile')


def replica_client(cls: ClassVar[ReplicaClient], peer_id: int, replicas: Dict[int, ReplicaAddress]):
    try:
        cli_logger()

        TOTAL = 20000
        EACH = 200
        OP_CP = 1000
        LAT_BUF = 10

        latencies_mat = np.zeros(TOTAL + TOTAL // EACH)

        with cls(peer_id, replicas) as client:
            time.sleep(0.5)

            latencies = deque()

            for i in range(TOTAL):
                lat, _ = client.request(AbstractCommand(random.randint(1, 1000000)))
                latencies.append(lat)
                latencies_mat[i] = lat
                # time.sleep(1.)
                # print(lat)
                if i % OP_CP == 0 and i > 0 and peer_id == 100:
                    lat, _ = client.request(AbstractCommand(0))
                    latencies.append(lat)
                    latencies_mat[i] = lat

                if i % EACH == 0:
                    # print(latencies)
                    logger.info(f'Client `{peer_id}` DONE {i + 1} LAT_AVG={sum(latencies) / len(latencies)}')

                if len(latencies) > LAT_BUF:
                    latencies.popleft()
            logger.info(f'Client `{peer_id}` DONE')
        np.save(f'latencies-{peer_id}.npy', latencies_mat)
    except:
        logger.exception(f'Client {peer_id}')
