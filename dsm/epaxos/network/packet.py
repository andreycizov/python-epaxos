from typing import List, NamedTuple, Optional

from dsm.epaxos.command.state import Command
from dsm.epaxos.instance.state import Ballot, Slot, StateType, InstanceState, STATE_TYPES_MAP


class Payload:
    pass


class Packet(NamedTuple):
    origin: int
    destination: int
    type: str
    payload: Payload

    @classmethod
    def serializer(cls, sub_ser):
        def ser(obj: 'Packet'):
            return {
                'o': obj.origin,
                'd': obj.destination,
                't': obj.type,
                'p': sub_ser(obj.payload)
            }
        return ser

    @classmethod
    def deserializer(cls, sub_deser):
        def deser(json):
            return cls(json['o'], json['d'], json['t'], sub_deser(TYPE_TO_PACKET[json['t']], json['p']))
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
    command: Optional[Command]
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
