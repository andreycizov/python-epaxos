import random
from typing import List

import logging

from dsm.epaxos.command.state import Command
from dsm.epaxos.instance.state import Ballot, Slot, StateType
from dsm.epaxos.network.packet import Packet, ClientRequest, PreAcceptRequest, PreAcceptResponseAck, \
    PreAcceptResponseNack, AcceptRequest, AcceptResponseAck, AcceptResponseNack, CommitRequest, PrepareRequest, \
    PrepareResponseAck, PrepareResponseNack, ClientResponse, Payload, DivergedResponse
from dsm.epaxos.network.peer import Channel, DirectInterface
from dsm.epaxos.replica.replica import Replica
from dsm.epaxos.replica.state import ReplicaState

logger = logging.getLogger(__name__)


class ReplicaReceiveChannel(Channel):
    @property
    def replica(self) -> Replica:
        raise NotImplementedError('')

    def receive_packet(self, body):
        raise NotImplementedError()

    def receive(self, packet: Packet):
        self.replica.state.packet_counts[packet.type] += 1

        p = packet.payload
        if random.random() > 0.97:
            return
        MAP = {
            ClientRequest: ('leader', 'client_request', lambda p: (packet.origin, p.command)),
            PreAcceptRequest: ('acceptor', 'pre_accept_request',
                               lambda p: (packet.origin, p.slot, p.ballot, p.command, p.seq, p.deps)),
            PreAcceptResponseAck: (
                'leader', 'pre_accept_response_ack', lambda p: (packet.origin, p.slot, p.ballot, p.seq, p.deps)),
            PreAcceptResponseNack: ('leader', 'pre_accept_response_nack', lambda p: (packet.origin, p.slot)),
            AcceptRequest: (
                'acceptor', 'accept_request', lambda p: (packet.origin, p.slot, p.ballot, p.command, p.seq, p.deps)),
            AcceptResponseAck: ('leader', 'accept_response_ack', lambda p: (packet.origin, p.slot, p.ballot)),
            AcceptResponseNack: ('leader', 'accept_response_nack', lambda p: (packet.origin, p.slot)),
            CommitRequest: (
                'acceptor', 'commit_request', lambda p: (packet.origin, p.slot, p.ballot, p.seq, p.command, p.deps)),
            PrepareRequest: ('acceptor', 'prepare_request', lambda p: (packet.origin, p.slot, p.ballot)),
            PrepareResponseAck: (
                'leader', 'prepare_response_ack', lambda p: (packet.origin, p.slot, p.ballot, p.command, p.seq, p.deps,
                                                             p.state)),
            DivergedResponse: ('leader', 'diverged_response', lambda p: (packet.origin)),
            PrepareResponseNack: ('leader', 'prepare_response_nack', lambda p: (packet.origin, p.slot)),
        }

        mapped = MAP.get(p.__class__)

        if mapped:
            dest, call, lam = mapped

            dest = getattr(self.replica, dest)

            if isinstance(dest, DirectInterface):
                dest.packet(packet.origin, p)
                return

            call = getattr(dest, call)

            call(*lam(p))
        else:
            logger.error(f'XXX Replica `{self.replica.state.self_id}`: {packet}')


class ReplicaSendChannel(Channel):
    @property
    def peer_id(self) -> ReplicaState:
        raise NotImplementedError()

    def send_packet(self, packet: Packet):
        raise NotImplementedError()

    def send(self, peer: int, payload: Payload):
        self.send_packet(Packet(self.peer_id, peer, str(payload.__class__.__name__), payload))

    def pre_accept_request(
        self,
        peer: int,
        slot: Slot, ballot: Ballot,
        command: Command, seq: int, deps: List[Slot]
    ):
        self.send(peer, PreAcceptRequest(slot, ballot, command, seq, deps))

    def accept_request(
        self,
        peer: int,
        slot: Slot, ballot: Ballot,
        command: Command, seq: int, deps: List[Slot]
    ):
        self.send(peer, AcceptRequest(slot, ballot, command, seq, deps))

    def commit_request(
        self,
        peer: int,
        slot: Slot, ballot: Ballot,
        command: Command, seq: int, deps: List[Slot]
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
        command: Command
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
        command: Command, seq: int, deps: List[Slot],
        state: StateType
    ):
        self.send(peer, PrepareResponseAck(slot, ballot, command, seq, deps, state))

    def diverged_response(self,
                          peer: int):
        self.send(peer, DivergedResponse(Slot(0, 0)))

    def prepare_response_nack(
        self,
        peer: int,
        slot: Slot
    ):
        self.send(peer, PrepareResponseNack(slot))

    def client_response(
        self,
        client_peer: int,
        command: Command
    ):
        self.send(client_peer, ClientResponse(command))
