import logging
import random
import time
from datetime import datetime, timedelta
from itertools import groupby
from typing import Dict, NamedTuple

import zmq
from zmq import Context

from dsm.epaxos.command.deps.default import DefaultDepsStore
from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.store import InstanceStore
from dsm.epaxos.network.zeromq.mapping import ReplicaChannel, route_packet, deserialize
from dsm.epaxos.network.zeromq.util import cli_logger
from dsm.epaxos.replica.replica import Replica
from dsm.epaxos.replica.state import ReplicaState
from dsm.epaxos.timeout.store import TimeoutStore

logger = logging.getLogger(__name__)


class ReplicaAddress(NamedTuple):
    replica_addr: str


class ReplicaServer:
    def __init__(
        self,
        context: Context,
        epoch: int,
        replica_id: int,
        peer_addr: Dict[int, ReplicaAddress],
    ):
        self.poller = zmq.Poller()
        self.peer_addr = peer_addr

        replica_socket = context.socket(zmq.ROUTER)
        replica_socket.bind(peer_addr[replica_id].replica_addr)
        replica_socket.setsockopt_string(zmq.IDENTITY, str(replica_id))
        replica_socket.setsockopt(zmq.ROUTER_HANDOVER, 1)

        self.poller.register(replica_socket, zmq.POLLIN)
        self.replica_socket = replica_socket

        channel = ReplicaChannel(replica_id, self.replica_socket)
        state = ReplicaState(channel, epoch, replica_id, set(peer_addr.keys()), set(peer_addr.keys()), True, timedelta(milliseconds=150))
        deps_store = DefaultDepsStore()
        timeout_store = TimeoutStore(state)
        store = InstanceStore(state, deps_store, timeout_store)

        self.state = state
        self.replica = Replica(state, store)

    def connect(self):
        logger.info(f'Replica `{self.state.replica_id}` connecting.')
        for peer_id, addr in self.peer_addr.items():
            if peer_id == self.state.replica_id:
                continue

            self.replica_socket.connect(addr.replica_addr)

    def main(self):
        logger.info(f'Replica `{self.state.replica_id}` started.')

        time_start = datetime.now()
        while True:
            min_wait = self.replica.minimum_wait()
            poll_result = self.poller.poll(min_wait * 1000. if min_wait else 1.)

            sockets = dict(poll_result)

            replica_recv = self.replica_socket in sockets

            if replica_recv:
                while True:
                    try:
                        replica_request = self.replica_socket.recv_multipart(flags=zmq.NOBLOCK)[-1]

                        packet = deserialize(replica_request)

                        route_packet(self.replica, packet)
                    except zmq.ZMQError:
                        break

            self.replica.check_timeouts()

            time_now = datetime.now()

            if time_now - time_start > timedelta(minutes=0.5):
                time_start = datetime.now()

                print(self.replica, {y: len(list(x)) for y, x in groupby(sorted([v.type for k, v in self.replica.store.instances.items()]))}, self.replica.store.last_committed)


class ReplicaClient:
    def __init__(
        self,
        context: Context,
        peer_id: int,
        peer_addr: Dict[int, ReplicaAddress],

    ):
        self.peer_id = peer_id
        self.peer_addr = peer_addr
        self.replica_id = None
        self.poller = zmq.Poller()

        socket = context.socket(zmq.DEALER)
        socket.setsockopt_string(zmq.IDENTITY, str(peer_id))

        self.socket = socket
        self.poller.register(self.socket, zmq.POLLIN)

        self.channel = None

    def connect(self, replica_id=None):
        if replica_id is None:
            replica_id = random.choice(list(self.peer_addr.keys()))

        if self.replica_id:
            self.socket.disconnect(self.peer_addr[self.replica_id].replica_addr)

        self.replica_id = replica_id
        self.channel = ReplicaChannel(self.peer_id, self.socket)

        self.socket.connect(self.peer_addr[replica_id].replica_addr)

    def request(self, command: AbstractCommand):
        assert self.replica_id is not None

        TIMEOUT = 1000

        self.channel.client_request(self.replica_id, command)

        start = datetime.now()

        while True:

            poll_result = dict(self.poller.poll(TIMEOUT))

            if self.socket in poll_result:
                payload, = self.socket.recv_multipart()

                rtn = deserialize(payload)
                # logger.info(f'Client `{self.peer_id}` -> {self.replica_id} Send={command} Recv={rtn.payload}')

                end = datetime.now()
                latency = (end - start).total_seconds()
                return latency, rtn
            else:
                # logger.info(f'Client `{self.peer_id}` -> {self.replica_id} RetrySend={command}')
                self.channel.client_request(self.replica_id, command)


def replica_server(epoch: int, replica_id: int, replicas: Dict[int, ReplicaAddress]):
    try:
        cli_logger()
        context = zmq.Context.instance()
        rs = ReplicaServer(context, epoch, replica_id, replicas)
        rs.connect()
        rs.main()
    except:
        logger.exception(f'Server {replica_id}')


def replica_client(peer_id: int, replicas: Dict[int, ReplicaAddress]):
    try:
        cli_logger()
        context = zmq.Context.instance()
        rc = ReplicaClient(context, peer_id, replicas)
        rc.connect()

        time.sleep(0.5)

        latencies = []



        for i in range(20000):
            lat, _ = rc.request(AbstractCommand(random.randint(1, 1000000)))
            latencies.append(lat)
            if i % 200 == 0:
                # print(latencies)
                logger.info(f'Client `{peer_id}` DONE {i + 1}')
        logger.info(f'Client `{peer_id}` DONE')
    except:
        logger.exception(f'Client {peer_id}')
