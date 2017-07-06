from typing import Optional, SupportsInt

from dsm.epaxos.message.abstract import Message


class Peer:
    def __init__(self, ident: Optional[SupportsInt]):
        self.ident = ident

    def send(self, message: Message):
        pass