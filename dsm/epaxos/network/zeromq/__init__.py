import pickle
from typing import List, Dict, NamedTuple

import zlib

import zmq
from zmq import Socket, Context

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Slot, StateType, Ballot
from dsm.epaxos.network.packet import Packet, Payload, ClientResponse, PrepareResponseNack, PrepareResponseAck, \
    AcceptResponseNack, AcceptResponseAck, PreAcceptResponseAck, PreAcceptResponseNack, ClientRequest, PrepareRequest, \
    CommitRequest, AcceptRequest, PreAcceptRequest
from dsm.epaxos.network.peer import Channel
from dsm.epaxos.replica.replica import Replica


def serialize(packet: Packet, protocol: int):
    p = pickle.dumps(packet, protocol)
    return zlib.compress(p)


def deserialize(body: bytes):
    z = zlib.decompress(body)
    return pickle.loads(z)


class ReplicaChannel(Channel):
    def __init__(self, replica_id, peer_socket: Socket):
        self.replica_id = replica_id
        self.peer_socket = peer_socket
        self.protocol = -1
        super().__init__()

    def send(self, peer: int, payload: Payload):
        packet = Packet(self.replica_id, peer, str(payload.__class__.__name__), payload)

        self.peer_socket.send_multipart([str(peer), serialize(packet, self.protocol)])

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
        replica_id: int,
        peer_addr: Dict[int, ReplicaAddress],
    ):
        self.poller = zmq.Poller()

        # TODO: dealer is strictly Round-Robin.
        peer_socket = context.socket(zmq.ROUTER)
        peer_socket.setsockopt_string(zmq.IDENTITY, str(replica_id))
        # peer_socket.identity = str(replica_id)

        for peer_id, addr in peer_addr.items():
            if peer_id == replica_id:
                continue

            peer_socket.connect(addr.replica_addr)

        replica_socket = context.socket(zmq.ROUTER)
        replica_socket.bind(peer_addr[replica_id].replica_addr)

        client_socket = context.socket(zmq.ROUTER)
        client_socket.bind(peer_addr[replica_id].client_addr)

        self.poller.register(replica_socket)
        self.poller.register(client_socket)

        self.peer_socket = peer_socket
        self.client_socket = client_socket
        self.replica_socket = replica_socket
        self.replica = None  # type: Replica

        # TODO: now issue is that clients are not supposed to work this way!

    def main(self):
        while True:
            to_wait = self.replica.minimum_wait()
            wait_seconds = to_wait.total_seconds() if to_wait else None
            sockets = dict(self.poller.poll(wait_seconds))

            if self.client_socket in sockets:
                client_id, _, client_request = self.client_socket.recv_multipart()

                print(client_request)

            if self.replica_socket in sockets:
                _, _, replica_request = self.replica_socket.recv_multipart()

                print(replica_request)


def replica_server(replica_id: int, replicas: Dict[int, ReplicaAddress]):
    context = zmq.Context.instance()
    ReplicaServer(context, replica_id, replicas).main()


class Client:
    pass
