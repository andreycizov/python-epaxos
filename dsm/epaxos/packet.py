from typing import Set, SupportsInt

from dsm.epaxos.state import Command, Slot, Ballot


class Packet:
    def __repr__(self):
        return f'{self.__class__.__name__}'


class ClientRequest(Packet):
    def __init__(self, command: Command):
        self.command = command

    def __repr__(self):
        return f'{self.__class__.__name__}({self.command})'


class ClientResponse(Packet):
    def __init__(self, command: Command):
        self.command = command

    def __repr__(self):
        return f'{self.__class__.__name__}({self.command})'


class NackReply(Packet):
    def __init__(self, slot):
        self.slot = slot

    def __repr__(self):
        return f'{self.__class__.__name__}({self.slot})'


class WithBallot:
    def __init__(self, slot: Slot, ballot: Ballot):
        self.slot = slot
        self.ballot = ballot

    def __repr__(self):
        return f'{self.__class__.__name__}({self.slot}, {self.ballot})'


class WithCommand:
    def __init__(self, command: Command, seq: SupportsInt, deps: Set[Slot]):
        self.command = command
        self.seq = seq
        self.deps = deps

    def __repr__(self):
        return f'{self.__class__.__name__}({self.slot}, {self.ballot}, {self.command}, {self.seq}, {self.deps})'


class WithState:
    def __init__(self, state):
        self.state = state


class BallotPacket(Packet, WithBallot):
    def __init__(self, slot: Slot, ballot: Ballot):
        WithBallot.__init__(self, slot, ballot)


class BallotCommandPacket(BallotPacket, WithCommand):
    def __init__(self, slot: Slot, ballot: Ballot, command: Command, seq: SupportsInt, deps: Set[Slot]):
        BallotPacket.__init__(self, slot, ballot)
        WithCommand.__init__(self, command, seq, deps)


class BallotCommandStatePacket(BallotCommandPacket, WithState):
    def __init__(self, slot: Slot, ballot: Ballot, command: Command, seq: SupportsInt, deps: Set[Slot], state):
        BallotCommandPacket.__init__(self, slot, ballot, command, seq, deps)
        WithState.__init__(self, state)


class PreAcceptRequest(BallotCommandPacket):
    pass


class PreAcceptReply:
    pass


class PreAcceptAckReply(BallotCommandPacket, PreAcceptReply):
    pass


class PreAcceptNackReply(BallotPacket, PreAcceptReply):
    pass


class AcceptRequest(BallotCommandPacket):
    pass


class AcceptReply:
    pass


class AcceptAckReply(BallotCommandPacket, AcceptReply):
    pass


class AcceptNackReply(BallotPacket, AcceptReply):
    pass


class CommitRequest(BallotCommandPacket):
    pass


class PrepareRequest(BallotPacket):
    pass


class PrepareReply(BallotCommandStatePacket):
    pass