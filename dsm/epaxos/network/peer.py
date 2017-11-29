from typing import List

from dsm.epaxos.command.state import Command
from dsm.epaxos.instance.state import StateType
from dsm.epaxos.instance.new_state import Slot, Ballot
from dsm.epaxos.network.packet import Payload


class AcceptorInterface:
    def pre_accept_request(
        self,
        peer: int,
        slot: Slot, ballot: Ballot,
        command: Command, seq: int, deps: List[Slot]
    ):
        raise NotImplementedError()

    def accept_request(
        self,
        peer: int,
        slot: Slot, ballot: Ballot,
        command: Command, seq: int, deps: List[Slot]
    ):
        raise NotImplementedError()

    def commit_request(
        self,
        peer: int,
        slot: Slot, ballot: Ballot,
        command: Command, seq: int, deps: List[Slot]
    ):
        raise NotImplementedError()

    def prepare_request(
        self,
        peer: int,
        slot: Slot, ballot: Ballot
    ):
        raise NotImplementedError()


class LeaderInterface:
    def client_request(
        self,
        client_peer: int,
        command: Command
    ):
        raise NotImplementedError()

    def pre_accept_response_ack(
        self,
        peer: int,
        slot: Slot, ballot: Ballot,
        seq: int, deps: List[Slot]
    ):
        raise NotImplementedError()

    def pre_accept_response_nack(
        self,
        peer: int,
        slot: Slot
    ):
        raise NotImplementedError()

    def accept_response_ack(
        self,
        peer: int,
        slot: Slot, ballot: Ballot
    ):
        raise NotImplementedError()

    def accept_response_nack(
        self,
        peer: int,
        slot: Slot
    ):
        raise NotImplementedError()

    def prepare_response_ack(
        self,
        peer: int,
        slot: Slot, ballot: Ballot,
        command: Command, seq: int, deps: List[Slot],
        state: StateType
    ):
        raise NotImplementedError()

    def diverged_response(self,
                          peer: int):
        raise NotImplementedError()

    def prepare_response_nack(
        self,
        peer: int,
        slot: Slot
    ):
        raise NotImplementedError()


class DirectInterface:
    def packet(self, peer: int, payload: Payload):
        pass


class ClientInterface:
    def client_response(
        self,
        client_peer: int,
        command: Command
    ):
        raise NotImplementedError()


class Channel(AcceptorInterface, ClientInterface, LeaderInterface, DirectInterface):
    pass
