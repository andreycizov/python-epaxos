from typing import List, NamedTuple

from enum import IntEnum

from dsm.epaxos.command.state import AbstractCommand, Noop


class State(IntEnum):
    PreAccepted = 1
    Accepted = 2
    Committed = 4


class Slot(NamedTuple):
    replica_id: int
    instance_id: int

    def ballot(self, epoch):
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


class Instance:
    def __init__(self, ballot: Ballot, command: AbstractCommand, seq: int, deps: List[Slot], state: State):
        self.ballot = ballot
        self.command = command
        self.seq = seq
        self.deps = deps
        self.state = state

    def set_ballot(self, ballot: Ballot):
        self.ballot = ballot

    def set_ballot_next(self):
        self.ballot = self.ballot.next()

    def set_state(self, state: State):
        assert state >= self.state
        self.state = state

        if self.state == State.Committed:
            raise NotImplementedError('Notify all of the instances that depend on this instance that there is a new committed instance')

    def set_deps(self, seq: int, deps: List[Slot]):
        # TODO: every time we set deps, we check if we know about all of the specific slots in here.
        # TODO: if we don't - we then create an instance with an empty command and an initial ballot number for
        # TODO: our current epoch (so that we could then start ExplicitPrepare for it).
        raise NotImplementedError('')
        self.seq = seq
        self.deps = deps

    def set_command(self, command: AbstractCommand):
        self.command = command

    def set_noop(self):
        self.command = Noop
        self.set_deps(0, [])

    def __repr__(self):
        return f'{self.__class__.__name__}({self.ballot}, {self.command}, {self.seq}, {self.deps}, {self.phase})'
