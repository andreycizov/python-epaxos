from typing import Optional, SupportsInt, Set

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Slot, Ballot


class Channel:
    def __init__(self):
        pass

    def pre_accept_request(self, peer: SupportsInt, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: SupportsInt,
                           deps: Set[Slot]):
        pass

    def prepare_request(self, peer: SupportsInt, slot: Slot, ballot: Ballot):
        pass

class Peer:
    def __init__(self, ident: Optional[SupportsInt]):
        self.ident = ident

    def pre_accept_request(self, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: SupportsInt,
                           deps: Set[Slot]):
        pass

    def prepare_request(self, slot: Slot, ballot: Ballot):
        pass
