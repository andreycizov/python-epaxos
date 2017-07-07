from typing import SupportsInt, Set

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Slot, Ballot
from dsm.epaxos.message.abstract import Message
from dsm.epaxos.message.state import Vote
from dsm.epaxos.message.types import PayloadType


class PreAcceptRequest(Message):
    def __init__(self, vote: Vote, command: AbstractCommand, seq: SupportsInt, deps: Set[Slot]):
        self.vote = vote
        self.command = command
        self.seq = seq
        self.deps = deps


class PreAcceptResponse(Message):
    def __init__(self, slot: Slot):
        self.slot = slot


class PreAcceptAckResponse(PreAcceptResponse):
    def __init__(self, slot: Slot, ballot: Ballot, seq: SupportsInt, deps: Set[Slot]):
        super().__init__(slot)
        self.ballot = ballot
        self.seq = seq
        self.deps = deps


class PreAcceptNackResponse(PreAcceptResponse):
    pass
