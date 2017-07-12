import random
from typing import List

import logging

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Ballot, Slot, StateType
from dsm.epaxos.network.packet import Packet, ClientRequest, PreAcceptRequest, PreAcceptResponseAck, \
    PreAcceptResponseNack, AcceptRequest, AcceptResponseAck, AcceptResponseNack, CommitRequest, PrepareRequest, \
    PrepareResponseAck, PrepareResponseNack, ClientResponse, Payload
from dsm.epaxos.network.peer import Channel
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
        p = packet.payload
        if isinstance(p, ClientRequest):
            self.replica.leader.client_request(packet.origin, p.command)
        else:
            if random.random() > 0.99:
                return

            if isinstance(p, PreAcceptRequest):
                self.replica.acceptor.pre_accept_request(packet.origin, p.slot, p.ballot, p.command, p.seq, p.deps)
            elif isinstance(p, PreAcceptResponseAck):
                self.replica.leader.pre_accept_response_ack(packet.origin, p.slot, p.ballot, p.seq, p.deps)
            elif isinstance(p, PreAcceptResponseNack):
                self.replica.leader.pre_accept_response_nack(packet.origin, p.slot)
            elif isinstance(p, AcceptRequest):
                self.replica.acceptor.accept_request(packet.origin, p.slot, p.ballot, p.command, p.seq, p.deps)
            elif isinstance(p, AcceptResponseAck):
                self.replica.leader.accept_response_ack(packet.origin, p.slot, p.ballot)
            elif isinstance(p, AcceptResponseNack):
                self.replica.leader.accept_response_nack(packet.origin, p.slot)
            elif isinstance(p, CommitRequest):
                self.replica.acceptor.commit_request(packet.origin, p.slot, p.ballot, p.seq, p.command, p.deps)
            elif isinstance(p, PrepareRequest):
                self.replica.acceptor.prepare_request(packet.origin, p.slot, p.ballot)
            elif isinstance(p, PrepareResponseAck):
                self.replica.leader.prepare_response_ack(packet.origin, p.slot, p.ballot, p.command, p.seq, p.deps,
                                                         p.state)
            elif isinstance(p, PrepareResponseNack):
                self.replica.leader.prepare_response_nack(packet.origin, p.slot)
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
