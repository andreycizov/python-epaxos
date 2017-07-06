from typing import SupportsInt, Set

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Slot
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
    def __init__(self, vote: Vote):
        self.vote = vote


class PreAcceptAckResponse(PreAcceptResponse):
    def __init__(self, vote: Vote, seq: SupportsInt, deps: Set[Slot]):
        super().__init__(vote)
        self.seq = seq
        self.deps = deps


class PreAcceptNackResponse(PreAcceptResponse):
    pass
