from typing import List, NamedTuple

from enum import IntEnum

from dsm.epaxos.command.state import AbstractCommand, Noop


class Slot(NamedTuple):
    replica_id: int
    instance_id: int

    def ballot_initial(self, epoch):
        return Ballot(epoch, 0, self.replica_id)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.replica_id},{self.instance_id})'


class Ballot(NamedTuple):
    epoch: int
    b: int
    replica_id: int

    def next(self):
        return Ballot(self.epoch, self.b + 1, self.replica_id)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.epoch},{self.b},{self.replica_id})'


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


class PreparedState(InstanceState):
    type = StateType.Prepared


class PostPreparedState(InstanceState):
    type = None  # type: StateType

    def __init__(self, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int, deps: List[Slot]):
        self.command = command
        self.seq = seq
        self.deps = deps
        super().__init__(slot, ballot)


class PreAcceptedState(PostPreparedState):
    type = StateType.PreAccepted


class AcceptedState(PostPreparedState):
    type = StateType.Accepted


class CommittedState(PostPreparedState):
    type = StateType.Committed
