from typing import Set, SupportsInt

from enum import IntEnum

from dsm.epaxos.command.state import AbstractCommand


class State(IntEnum):
    PreAccepted = 1
    Accepted = 2
    Committed = 4


class Slot:
    def __init__(self, replica_id, instance_id):
        self.replica_id = replica_id
        self.instance_id = instance_id

    def ballot(self, epoch):
        return Ballot(epoch, 0, self.replica_id)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.replica_id},{self.instance_id})'

    def tuple(self):
        return self.replica_id, self.instance_id

    def __lt__(self, other: 'Slot'):
        return self.tuple() < other.tuple()


class Ballot:
    def __init__(self, epoch, b, leader_id):
        self.epoch = epoch
        self.b = b
        self.leader_id = leader_id

    def next(self):
        return Ballot(self.epoch, self.b + 1, self.leader_id)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.epoch},{self.b},{self.leader_id})'

    def tuple(self):
        return self.epoch, self.b, self.leader_id

    def __lt__(self, other: 'Ballot'):
        return self.tuple() < other.tuple()


class Instance:
    def __init__(self, ballot: Ballot, command: AbstractCommand, seq: SupportsInt, deps: Set[Slot], state: State):
        self.ballot = ballot
        self.command = command
        self.seq = seq
        self.deps = deps
        self.state = state

    def __repr__(self):
        return f'{self.__class__.__name__}({self.ballot}, {self.command}, {self.seq}, {self.deps}, {self.state})'
