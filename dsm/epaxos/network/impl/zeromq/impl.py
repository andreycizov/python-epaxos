import cProfile
import logging
import random
import signal
import time
from collections import deque
from typing import Dict, Tuple

import zmq

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.network.impl.generic.client import ReplicaClient
from dsm.epaxos.network.impl.generic.server import ReplicaAddress, ReplicaServer
from dsm.epaxos.network.impl.zeromq.mapping import ZMQClientSendChannel, ZMQReplicaReceiveChannel, \
    ZMQReplicaSendChannel, deserialize
from dsm.epaxos.network.peer import Channel

from dsm.epaxos.network.impl.zeromq.util import cli_logger

logger = logging.getLogger(__name__)


class ZMQReplicaServer(ReplicaServer):
    def init(self, replica_id: int) -> Tuple[Channel, Channel]:
        self.context = zmq.Context(len(self.peer_addr) - 1, shadow=False)

        socket = self.context.socket(zmq.ROUTER)
        socket.bind(self.peer_addr[replica_id].replica_addr)
        socket.setsockopt(zmq.IDENTITY, str(replica_id).encode())
        socket.setsockopt(zmq.ROUTER_HANDOVER, 1)
        socket.setsockopt(zmq.RCVBUF, 2 ** 20)
        socket.setsockopt(zmq.SNDBUF, 2 ** 20)
        socket.sndhwm = 1000000
        socket.rcvhwm = 1000000
        socket.setsockopt(zmq.LINGER, 0)
        socket.hwm = 50
        # socket.setsockopt(zmq.HWM, 20)

        self.poller = zmq.Poller()
        self.poller.register(socket, zmq.POLLIN)
        self.socket = socket

        # logger.info(f'Replica `{replica_id}` connecting.')
        for peer_id, addr in self.peer_addr.items():
            if peer_id == replica_id:
                continue

            self.socket.connect(addr.replica_addr)

        return ZMQReplicaSendChannel(self), ZMQReplicaReceiveChannel(self)

    def poll(self, min_wait):
        poll_result = self.poller.poll(min_wait * 1000.)

        return self.socket in dict(poll_result)

    def send(self):
        return self.channel_send.send_packets()

    def recv(self):
        rcvd = 0
        while True:
            try:
                replica_request = self.socket.recv_multipart(flags=zmq.NOBLOCK)[-1]

                self.channel_receive.receive_packet(replica_request)
                rcvd += 1
            except zmq.ZMQError:
                break
        return rcvd

    def close(self):
        self.socket.close()
        self.context.term()


class ZMQReplicaClient(ReplicaClient):
    def __init__(
        self,
        *args
    ):
        super().__init__(*args)

    def init(self, peer_id: int) -> Channel:
        self.poller = zmq.Poller()

        self.context = zmq.Context()

        socket = self.context.socket(zmq.DEALER)
        socket.setsockopt(zmq.IDENTITY, str(peer_id).encode())
        socket.linger = 0

        self.socket = socket
        self.poller.register(self.socket, zmq.POLLIN)

        self._replica_id = None

        return ZMQClientSendChannel(self)

    @property
    def leader_id(self):
        return self._replica_id

    def connect(self, replica_id=None):
        if self.leader_id is None:
            # replica_id = random.choice(list(self.peer_addr.keys()))
            replica_id = list(self.peer_addr.keys())[self.peer_id % len(self.peer_addr)]
            self._replica_id = replica_id
        else:
            self.socket.disconnect(self.peer_addr[self.leader_id].replica_addr)

        self.socket.connect(self.peer_addr[replica_id].replica_addr)

    def poll(self, max_wait) -> bool:
        poll_result = dict(self.poller.poll(max_wait * 1000.))
        return self.socket in poll_result

    def send(self, command: AbstractCommand):
        self.channel.client_request(self.leader_id, command)

    def recv(self):
        payload, = self.socket.recv_multipart()

        return deserialize(payload)

    def close(self):
        self.socket.disconnect()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def replica_server(epoch: int, replica_id: int, replicas: Dict[int, ReplicaAddress]):
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
        with ZMQReplicaServer(epoch, replica_id, replicas) as server:
            server.run()

    except:
        logger.exception(f'Server {replica_id}')
    finally:
        if profile:
            pr.disable()
            pr.dump_stats(f'{replica_id}.profile')


def replica_client(peer_id: int, replicas: Dict[int, ReplicaAddress]):
    try:
        cli_logger()

        with ZMQReplicaClient(peer_id, replicas) as client:
            time.sleep(0.5)

            latencies = deque()

            for i in range(20000):
                lat, _ = client.request(AbstractCommand(random.randint(1, 1000000)))
                latencies.append(lat)
                # time.sleep(1.)
                # print(lat)
                if i % 200 == 0:
                    # print(latencies)
                    logger.info(f'Client `{peer_id}` DONE {i + 1} LAT_AVG={sum(latencies) / len(latencies)}')

                if len(latencies) > 200:
                    latencies.popleft()
            logger.info(f'Client `{peer_id}` DONE')
    except:
        logger.exception(f'Client {peer_id}')
