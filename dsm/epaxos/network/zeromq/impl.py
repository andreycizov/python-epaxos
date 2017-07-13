import cProfile
import logging
import random
import signal
import time
from collections import deque
from datetime import datetime, timedelta
from itertools import groupby
from typing import Dict, NamedTuple

import zmq
from zmq import Context

from dsm.epaxos.command.deps.default import DefaultDepsStore
from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.store import InstanceStore
from dsm.epaxos.network.zeromq.mapping import deserialize, ZMQReplicaReceiveChannel, ZMQReplicaSendChannel, \
    ZMQClientSendChannel
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

        socket = context.socket(zmq.ROUTER)
        socket.bind(peer_addr[replica_id].replica_addr)
        socket.setsockopt(zmq.IDENTITY, str(replica_id).encode())
        socket.setsockopt(zmq.ROUTER_HANDOVER, 1)
        socket.setsockopt(zmq.RCVBUF, 2 ** 20)
        socket.setsockopt(zmq.SNDBUF, 2 ** 20)
        socket.sndhwm = 1000000
        socket.rcvhwm = 1000000
        socket.setsockopt(zmq.LINGER, 0)
        socket.hwm = 50
        # socket.setsockopt(zmq.HWM, 20)

        self.poller.register(socket, zmq.POLLIN)
        self.socket = socket

        self.channel_receive = ZMQReplicaReceiveChannel(self)
        self.channel_send = ZMQReplicaSendChannel(self)

        state = ReplicaState(
            self.channel_send,
            epoch, replica_id,
            set(peer_addr.keys()),
            set(peer_addr.keys()),
            True,
            5
        )

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

            self.socket.connect(addr.replica_addr)

    def main(self):
        logger.info(f'Replica `{self.state.replica_id}` started.')

        last_tick_time = datetime.now()
        poll_delta = 0.
        last_tick = self.state.ticks

        pkts_rcvd = 0
        pkts_sent = 0

        while True:
            min_wait = self.replica.check_timeouts_minimum_wait()
            min_wait_poll = max(0, self.state.seconds_per_tick - poll_delta)

            if min_wait:
                min_wait = min(min_wait, min_wait_poll)
            else:
                min_wait = min_wait_poll

            poll_result = self.poller.poll(min_wait * 1000.)
            poll_delta = (datetime.now() - last_tick_time).total_seconds()

            if poll_delta > self.state.seconds_per_tick:
                self.replica.tick()
                self.replica.check_timeouts()
                last_tick_time = last_tick_time + timedelta(seconds=self.state.seconds_per_tick)

            pkts_sent += self.channel_send.send_packets()

            sockets = dict(poll_result)

            if self.socket in sockets:
                rcvd = 0
                while True:
                    try:
                        replica_request = self.socket.recv_multipart(flags=zmq.NOBLOCK)[-1]

                        self.channel_receive.receive_packet(replica_request)
                        rcvd += 1
                    except zmq.ZMQError:
                        break

                pkts_rcvd += rcvd

                if rcvd:
                    self.replica.execute_pending()
            else:
                pass

            pkts_sent += self.channel_send.send_packets()

            if self.state.ticks != last_tick and self.state.ticks % (self.state.jiffies * 30) == 0:
                last_tick = self.state.ticks
                print(
                    datetime.now(),
                    self.state.ticks,
                    self.state.seconds_per_tick,
                    self.state.replica_id,
                    sorted((y.name, len(list(x))) for y, x in
                           groupby(sorted([v.type for k, v in self.replica.store.instances.items()]))),
                    # sorted((k, v) for k, v in self.replica.store.executed_cut.items())
                    sorted([(k, v) for k, v in self.state.packet_counts.items()])
                )


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
        socket.setsockopt(zmq.IDENTITY, str(peer_id).encode())

        self.socket = socket
        self.poller.register(self.socket, zmq.POLLIN)

        self.channel = None

    def connect(self, replica_id=None):
        if replica_id is None:
            # replica_id = random.choice(list(self.peer_addr.keys()))
            replica_id = list(self.peer_addr.keys())[self.peer_id % len(self.peer_addr)]

        if self.replica_id:
            self.socket.disconnect(self.peer_addr[self.replica_id].replica_addr)

        self.replica_id = replica_id
        self.channel = ZMQClientSendChannel(self)

        self.socket.connect(self.peer_addr[replica_id].replica_addr)

    def request(self, command: AbstractCommand):
        assert self.replica_id is not None

        TIMEOUT = 10000

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
        context = zmq.Context(len(replicas), shadow=False)
        rs = ReplicaServer(context, epoch, replica_id, replicas)
        rs.connect()

        rs.main()

    except:
        logger.exception(f'Server {replica_id}')
    finally:
        if profile:
            pr.disable()
            pr.dump_stats(f'{replica_id}.profile')


def replica_client(peer_id: int, replicas: Dict[int, ReplicaAddress]):
    try:
        cli_logger()
        context = zmq.Context()
        rc = ReplicaClient(context, peer_id, replicas)
        rc.connect()

        time.sleep(0.5)

        latencies = deque()

        for i in range(20000):
            lat, _ = rc.request(AbstractCommand(random.randint(1, 1000000)))
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
