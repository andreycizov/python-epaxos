from typing import Optional, List

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Slot, Ballot


class Channel:
    def __init__(self):
        pass

    def pre_accept_request(self, peer: int, slot: Slot, ballot: Ballot, command: AbstractCommand,
                           seq: int,
                           deps: List[Slot]):
        pass

    def accept_request(self, peer: int, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int,
                       deps: List[Slot]):
        pass

    def commit_request(self, peer: int, slot: Slot, ballot: Ballot, seq: int, command: AbstractCommand,
                       deps: List[Slot]):
        pass

    def prepare_request(self, peer: int, slot: Slot, ballot: Ballot):
        pass

    def client_response(self, client_peer: int, command: AbstractCommand):
        pass


class Peer:
    def __init__(self, ident: Optional[int]):
        self.ident = ident

    def pre_accept_request(self, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int,
                           deps: List[Slot]):
        pass

    def prepare_request(self, slot: Slot, ballot: Ballot):
        pass
