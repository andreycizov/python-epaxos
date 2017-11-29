from copy import copy
from enum import IntEnum
from typing import List

from dsm.epaxos.command.state import Command
from dsm.epaxos.instance.new_state import Ballot, Slot


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
        return f'{self.type.name}({self.slot}, {self.ballot})'

    def with_ballot(self, ballot):
        r = copy(self)
        r.ballot = ballot
        return r


class PreparedState(InstanceState):
    type = StateType.Prepared


class PostPreparedState(InstanceState):
    type = None  # type: StateType

    def __init__(self, slot: Slot, ballot: Ballot, command: Command, seq: int, deps: List[Slot]):
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

    def __repr__(self):
        return super().__repr__() + f'({self.command},{self.seq},{self.deps})'

    def promote(self, type) -> 'PostPreparedState':
        return STATE_TYPES_MAP[type](self.slot, self.ballot, self.command, self.seq, self.deps)

    @classmethod
    def noop(cls, slot, ballot):
        return cls(slot, ballot, None, 0, [])


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
