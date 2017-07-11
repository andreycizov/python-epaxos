import pickle
import random
from typing import List

import logging

import zlib
from zmq import Socket

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Slot, Ballot, StateType
from dsm.epaxos.network.packet import Payload, Packet, PreAcceptRequest, AcceptRequest, CommitRequest, PrepareRequest, \
    ClientRequest, PreAcceptResponseAck, PreAcceptResponseNack, AcceptResponseAck, AcceptResponseNack, \
    PrepareResponseAck, PrepareResponseNack, ClientResponse
from dsm.epaxos.network.peer import Channel
from dsm.epaxos.replica.replica import Replica

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


def route_packet(replica: Replica, packet: Packet):
    p = packet.payload
    # logger.warning(f'Recv `{replica.state.replica_id}`: {packet}')
    if isinstance(p, ClientRequest):
        replica.client_request(packet.origin, p.command)
    else:
        if random.random() > 0.95:
            return

        if isinstance(p, PreAcceptRequest):
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
