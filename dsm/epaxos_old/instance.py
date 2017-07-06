from typing import Set, List, SupportsInt

from dsm.epaxos.packet import PreAcceptAckReply, AcceptAckReply
from dsm.epaxos.state import Ballot, Command, Slot


class InstanceState:
    def __repr__(self):
        return f'{self.__class__.__name__}'


class PreAcceptedState(InstanceState):
    pass


class PreAcceptedLeaderState(InstanceState):
    def __init__(self, client_id):
        self.client_id = client_id
        self.replies = []  # type: List[PreAcceptAckReply]


class AcceptedState(InstanceState):
    pass


class AcceptedLeaderState(InstanceState):
    def __init__(self, client_id):
        self.client_id = client_id
        self.replies = []  # type: List[AcceptAckReply]


class CommittedState(InstanceState):
    pass


class Instance:
    def __init__(self, ballot: Ballot, command: Command, seq: SupportsInt, deps: Set[Slot], state: InstanceState):
        self.ballot = ballot
        self.command = command
        self.seq = seq
        self.deps = deps
        self.state = state

    def __repr__(self):
        return f'Instance({self.ballot}, {self.command}, {self.seq}, {self.deps}, {self.state})'
