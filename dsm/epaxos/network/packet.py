from typing import List, NamedTuple, Optional

from dsm.epaxos.command.state import Command
from dsm.epaxos.instance.state import InstanceState, STATE_TYPES_MAP, StateType
from dsm.epaxos.instance.new_state import Slot, Ballot
from dsm.serializer import T_des, T_ser


class Payload:
    pass


class Packet(NamedTuple):
    origin: int
    destination: int
    type: str
    payload: Payload

    @classmethod
    def serializer(cls, sub_ser: T_ser):
        def ser(obj: 'Packet'):
            return [
                obj.origin,
                obj.destination,
                obj.type,
                sub_ser(obj.payload.__class__)(obj.payload)
            ]

        return ser

    @classmethod
    def deserializer(cls, sub_deser: T_des):
        def deser(json):
            o, d, t, p = json
            return cls(o, d, t, sub_deser(TYPE_TO_PACKET[t])(p))

        return deser


class ClientRequest(NamedTuple, Payload):
    command: Command


class ClientResponse(NamedTuple, Payload):
    command: Optional[Command]


class PreAcceptRequest(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot
    command: Optional[Command]
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
    ballot: Ballot
    reason: str


class TentativePreAcceptRequest(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot
    command: Optional[Command]
    seq: int
    deps: List[Slot]


class TentativePreAcceptResponseAck(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot


class TentativePreAcceptResponseNack(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot
    conflict: Slot
    replica_id: int
    status: StateType


class AcceptRequest(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot
    command: Optional[Command]
    seq: int
    deps: List[Slot]


class AcceptResponseAck(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot


class AcceptResponseNack(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot


class CommitRequest(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot
    command: Optional[Command]
    seq: int
    deps: List[Slot]


class PrepareRequest(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot


class PrepareResponseAck(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot
    command: Optional[Command]
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
    ballot: Ballot


class PrepareResponseNack(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot


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
