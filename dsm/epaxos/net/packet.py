from typing import List, NamedTuple, Optional

from dsm.epaxos.cmd.state import Command
from dsm.epaxos.inst.state import Slot, Ballot, Stage
from dsm.serializer import T_des, T_ser


class Payload:
    pass


PeerID = int
ClientID = PeerID


class Packet(NamedTuple):
    origin: PeerID
    destination: PeerID
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


class ClientIdent(NamedTuple):
    pass


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
    status: Stage


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
    state: Stage


class PrepareResponseAckEmpty(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot


class PrepareResponseNack(NamedTuple, Payload):
    slot: Slot
    ballot: Ballot


class DivergedResponse(NamedTuple, Payload, ):
    slot: Slot


PACKET_CLIENT = (
    ClientRequest,
    ClientResponse,
    ClientIdent,
)

PACKET_ACCEPTOR = (
    PreAcceptRequest,

    AcceptRequest,

    CommitRequest,

    PrepareRequest,

)

PACKET_LEADER = (
    PreAcceptResponseAck,
    PreAcceptResponseNack,

    AcceptResponseAck,
    AcceptResponseNack,

    PrepareResponseAck,
    PrepareResponseNack,
)

PACKET_ALL = (
    DivergedResponse,
)

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
