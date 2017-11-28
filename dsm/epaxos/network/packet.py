from typing import List, NamedTuple

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Ballot, Slot, StateType, InstanceState, STATE_TYPES_MAP
from dsm.epaxos.network.serializer import _deserialize, _serialize


class Payload:
    pass


class Packet(NamedTuple):
    origin: int
    destination: int
    type: str
    payload: Payload

    @classmethod
    def serialize(cls, obj: 'Packet'):
        return {
            'o': obj.origin,
            'd': obj.destination,
            't': obj.type,
            'p': _serialize(obj.payload)
        }

    @classmethod
    def deserialize(cls, json):
        return cls(json['o'], json['d'], json['t'], _deserialize(TYPE_TO_PACKET[json['t']], json['p']))


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


class PreAccept(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot
    payload: Payload
    seq: int
    deps: List[Slot]


class PreAcceptResponseAck(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot
    seq: int
    deps: List[Slot]
    deps_comm_mask: List[bool]


class PreAcceptResponseNack(NamedTuple, Payload):
    slot: Slot
    reason: str


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

    @property
    def inst(self) -> InstanceState:
        if self.state in [StateType.Prepared]:
            return STATE_TYPES_MAP[self.state](self.slot, self.ballot)
        else:
            return STATE_TYPES_MAP[self.state](self.slot, self.ballot, self.command, self.seq, self.deps)


class PrepareResponseAckEmpty(NamedTuple, Payload):
    slot: Slot


class PrepareResponseNack(NamedTuple, Payload):
    slot: Slot


class DivergedResponse(NamedTuple, Payload, ):
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
    PrepareResponseNack,

    DivergedResponse,
]

TYPE_TO_PACKET = {v.__name__: v for v in PACKETS}

SLOTTED = [
    PreAcceptRequest,
    PreAcceptResponseAck,
    PreAcceptResponseNack,

    AcceptRequest,
    AcceptResponseAck,
    AcceptResponseNack,

    CommitRequest,
    PrepareRequest,
    PrepareResponseAck,
    DivergedResponse,
    PrepareResponseNack,
]
