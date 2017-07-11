import pickle
import random
from typing import List, Dict, NamedTuple

import zlib
import sys

import logging

import time
import zmq
from zmq import Socket, Context

from dsm.epaxos.command.deps.default import DefaultDepsStore
from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Slot, StateType, Ballot
from dsm.epaxos.instance.store import InstanceStore
from dsm.epaxos.network.packet import Packet, Payload, ClientResponse, PrepareResponseNack, PrepareResponseAck, \
    AcceptResponseNack, AcceptResponseAck, PreAcceptResponseAck, PreAcceptResponseNack, ClientRequest, PrepareRequest, \
    CommitRequest, AcceptRequest, PreAcceptRequest
from dsm.epaxos.network.peer import Channel
from dsm.epaxos.replica.replica import Replica
from dsm.epaxos.replica.state import ReplicaState
from dsm.epaxos.timeout.store import TimeoutStore

logger = logging.getLogger(__name__)


def serialize(packet: Packet, protocol: int):
    p = pickle.dumps(packet, protocol)
    return zlib.compress(p)


def deserialize(body: bytes):
    z = zlib.decompress(body)
    return pickle.loads(z)


class ReplicaChannel(Channel):
    def __init__(self, peer_id, peer_socket: Socket):
        self.peer_id = peer_id
        self.peer_socket = peer_socket
        self.protocol = -1
        super().__init__()

    def send(self, peer: int, payload: Payload):
        # logger.info(f'Send `{self.peer_id}` -> `{peer}` : {payload}')
        packet = Packet(self.peer_id, peer, str(payload.__class__.__name__), payload)

        self.peer_socket.send_multipart([str(peer).encode(), serialize(packet, self.protocol)])

    def pre_accept_request(
        self,
        peer: int,
        slot: Slot, ballot: Ballot,
        command: AbstractCommand, seq: int, deps: List[Slot]
    ):
        self.send(peer, PreAcceptRequest(slot, ballot, command, seq, deps))

    def accept_request(
        self,
        peer: int,
        slot: Slot, ballot: Ballot,
        command: AbstractCommand, seq: int, deps: List[Slot]
    ):
        self.send(peer, AcceptRequest(slot, ballot, command, seq, deps))

    def commit_request(
        self,
        peer: int,
        slot: Slot, ballot: Ballot,
        command: AbstractCommand, seq: int, deps: List[Slot]
    ):
        self.send(peer, CommitRequest(slot, ballot, command, seq, deps))

    def prepare_request(
        self,
        peer: int,
        slot: Slot, ballot: Ballot
    ):
        self.send(peer, PrepareRequest(slot, ballot))

    def client_request(
        self,
        client_peer: int,
        command: AbstractCommand
    ):
        self.send(client_peer, ClientRequest(command))

    def pre_accept_response_ack(
        self,
        peer: int,
        slot: Slot, ballot: Ballot,
        seq: int, deps: List[Slot]
    ):
        self.send(peer, PreAcceptResponseAck(slot, ballot, seq, deps))

    def pre_accept_response_nack(
        self,
        peer: int,
        slot: Slot
    ):
        self.send(peer, PreAcceptResponseNack(slot))

    def accept_response_ack(
        self,
        peer: int,
        slot: Slot, ballot: Ballot
    ):
        self.send(peer, AcceptResponseAck(slot, ballot))

    def accept_response_nack(
        self,
        peer: int,
        slot: Slot
    ):
        self.send(peer, AcceptResponseNack(slot))

    def prepare_response_ack(
        self,
        peer: int,
        slot: Slot, ballot: Ballot,
        command: AbstractCommand, seq: int, deps: List[Slot],
        state: StateType
    ):
        self.send(peer, PrepareResponseAck(slot, ballot, command, seq, deps, state))

    def prepare_response_nack(
        self,
        peer: int,
        slot: Slot
    ):
        self.send(peer, PrepareResponseNack(slot))

    def client_response(
        self,
        client_peer: int,
        command: AbstractCommand
    ):
        self.send(client_peer, ClientResponse(command))


class ReplicaAddress(NamedTuple):
    replica_addr: str
    client_addr: str


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
        state = ReplicaState(channel, epoch, replica_id, set(peer_addr.keys()), set(peer_addr.keys()), True)
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
        while True:
            min_wait = self.replica.minimum_wait()
            poll_result = self.poller.poll(min_wait * 1000. if min_wait and min_wait > 0.03 else None)

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


def route_packet(replica: Replica, packet: Packet):
    p = packet.payload
    # logger.warning(f'Recv `{replica.state.replica_id}`: {packet}')
    if isinstance(p, ClientRequest):
        replica.client_request(packet.origin, p.command)
    elif isinstance(p, PreAcceptRequest):
        replica.pre_accept_request(packet.origin, p.slot, p.ballot, p.command, p.seq, p.deps)
    elif isinstance(p, PreAcceptResponseAck):
        replica.pre_accept_response_ack(packet.origin, p.slot, p.ballot, p.seq, p.deps)
    elif isinstance(p, PreAcceptResponseNack):
        replica.pre_accept_response_nack(packet.origin, p.slot)
    elif isinstance(p, AcceptRequest):
        replica.accept_request(packet.origin, p.slot, p.ballot, p.command, p.seq, p.deps)
    elif isinstance(p, AcceptResponseAck):
        replica.accept_response_ack(packet.origin, p.slot, p.ballot)
    elif isinstance(p, AcceptResponseNack):
        replica.accept_response_nack(packet.origin, p.slot)
    elif isinstance(p, CommitRequest):
        replica.commit_request(packet.origin, p.slot, p.ballot, p.seq, p.command, p.deps)
    elif isinstance(p, PrepareRequest):
        replica.prepare_request(packet.origin, p.slot, p.ballot)
    elif isinstance(p, PrepareResponseAck):
        replica.prepare_response_ack(packet.origin, p.slot, p.ballot, p.command, p.seq, p.deps, p.state)
    elif isinstance(p, PrepareResponseNack):
        replica.prepare_response_nack(packet.origin, p.slot)
    else:
        logger.error(f'XXX Replica `{replica.state.replica_id}`: {packet}')


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

        while True:

            poll_result = dict(self.poller.poll(TIMEOUT))

            if self.socket in poll_result:
                payload, = self.socket.recv_multipart()

                rtn = deserialize(payload)
                logger.info(f'Client `{self.peer_id}` -> {self.replica_id} Send={command} Recv={rtn.payload}')
                return rtn
            else:
                logger.info(f'Client `{self.peer_id}` -> {self.replica_id} RetrySend={command}')
                self.channel.client_request(self.replica_id, command)


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

        for i in range(50):
            if i % 25 == 0:
                rc.connect()
            rc.request(AbstractCommand(1000))
        logger.info(f'Client `{peer_id}` DONE')
    except:
        logger.exception(f'Client {peer_id}')
