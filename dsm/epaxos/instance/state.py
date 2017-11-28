from copy import copy
from typing import List, NamedTuple, Any

from enum import IntEnum

from dsm.epaxos.command.state import AbstractCommand, Noop


class Slot(NamedTuple):
    replica_id: int
    instance_id: int

    def ballot_initial(self, epoch):
        return Ballot(epoch, 0, self.replica_id)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.replica_id},{self.instance_id})'

    @classmethod
    def serialize(cls, obj: 'Slot'):
        return [obj.replica_id, obj.instance_id]

    @classmethod
    def deserialize(cls, json):
        return cls(*json)


class Ballot(NamedTuple):
    epoch: int
    b: int
    replica_id: int

    def next(self):
        return Ballot(self.epoch, self.b + 1, self.replica_id)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.epoch},{self.b},{self.replica_id})'

    @classmethod
    def serialize(cls, obj: 'Ballot'):
        return [obj.epoch, obj.b, obj.replica_id]

    @classmethod
    def deserialize(cls, json):
        return cls(*json)


class Stage(IntEnum):
    Prepared = 0
    PreAccepted = 1
    Accepted = 2
    Committed = 4


class Payload(NamedTuple):
    body: Any


class Status(NamedTuple):
    payload: Payload
    stage: Stage
    seq: int
    deps: List[Slot]


class StateType(IntEnum):
    # We do not have a command for this state yet.
    Prepared = 0
    PreAccepted = 1
    Accepted = 2
    Committed = 4


class InstanceState:
    type = None  # type: StateType

    def __init__(self, slot: Slot, ballot: Ballot):
        self.slot = slot
        self.ballot = ballot

    def __repr__(self):
        return f'{self.__class__.__name__}({self.slot}, {self.ballot})'

    def with_ballot(self, ballot):
        r = copy(self)
        r.ballot = ballot
        return r


class PreparedState(InstanceState):
    type = StateType.Prepared


class PostPreparedState(InstanceState):
    type = None  # type: StateType

    def __init__(self, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int, deps: List[Slot]):
        self.command = command
        self.seq = seq
        self.deps = deps
        super().__init__(slot, ballot)

    def update(self, seq, deps, command=None):
        r = copy(self)
        r.seq = seq
        r.deps = deps
        if command:
            r.command = command
        return r

    def promote(self, type) -> 'PostPreparedState':
        return STATE_TYPES_MAP[type](self.slot, self.ballot, self.command, self.seq, self.deps)

    @classmethod
    def noop(cls, slot, ballot):
        return cls(slot, ballot, Noop, 0, [])


class PreAcceptedState(PostPreparedState):
    type = StateType.PreAccepted


class AcceptedState(PostPreparedState):
    type = StateType.Accepted


class CommittedState(PostPreparedState):
    type = StateType.Committed


STATE_TYPES = [
    PreparedState,
    PreAcceptedState,
    AcceptedState,
    CommittedState,
]

STATE_TYPES_MAP = {x.type: x for x in STATE_TYPES}
