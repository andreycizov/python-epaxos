from typing import NamedTuple

from dsm.epaxos.cmd.state import Command
from dsm.epaxos.inst.state import Slot


class LeaderStart(NamedTuple):
    command: Command


class LeaderStop(NamedTuple):
    slot: Slot
    reason: str = 'NONE'


class LeaderExplicitPrepare(NamedTuple):
    slot: Slot
    reason: str