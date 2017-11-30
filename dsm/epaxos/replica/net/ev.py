from typing import NamedTuple, Tuple, Any

from dsm.epaxos.net import packet


class Send(NamedTuple):
    dest: int
    payload: packet.Payload

    def __repr__(self):
        return f'Send({self.dest}, {self.payload})'


class Receive(NamedTuple):
    type: Tuple[Any, ...]

    @classmethod
    def any(cls, *events):
        return Receive(events)

    @classmethod
    def from_waiting(cls, waiting_for, payload):
        return tuple(payload if payload.__class__ == x else None for x in waiting_for)

    def __repr__(self):
        rs = ','.join(f'{x.__name__}' for x in self.type)
        return f'Receive({rs})'