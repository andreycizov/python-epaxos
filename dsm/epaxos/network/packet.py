from typing import NamedTuple, List

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Slot, Ballot, StateType


class Payload:
    pass


class Packet(NamedTuple):
    origin: int
    destination: int
    type: str
    payload: Payload


class ClientRequest(NamedTuple, Payload):
    command: AbstractCommand


class ClientResponse(NamedTuple, Payload):
    command: AbstractCommand


class PreAcceptRequest(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot
    command: AbstractCommand
    seq: int
    deps: List[Slot]


class PreAcceptResponseAck(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot
    seq: int
    deps: List[Slot]


class PreAcceptResponseNack(NamedTuple, Payload):
    slot: Slot


class AcceptRequest(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot
    command: AbstractCommand
    seq: int
    deps: List[Slot]


class AcceptResponseAck(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot


class AcceptResponseNack(NamedTuple, Payload):
    slot: Slot


class CommitRequest(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot
    command: AbstractCommand
    seq: int
    deps: List[Slot]


class PrepareRequest(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot


class PrepareResponseAck(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot
    command: AbstractCommand
    seq: int
    deps: List[Slot]
    state: StateType


class PrepareResponseNack(NamedTuple, Payload):
    slot: Slot


PACKETS = [
    ClientRequest,
    ClientResponse,

    PreAcceptRequest,
    PreAcceptResponseAck,
    PreAcceptResponseNack,

    AcceptRequest,
    AcceptResponseAck,
    AcceptResponseNack,

    CommitRequest,

    PrepareRequest,
    PrepareResponseAck,
    PrepareResponseNack
]
