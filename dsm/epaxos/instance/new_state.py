from enum import IntEnum
from typing import NamedTuple, List

from dsm.epaxos.command.state import Command
from dsm.epaxos.instance.state import Slot


class Stage(IntEnum):
    Prepared = 0
    PreAccepted = 1
    Accepted = 2
    Committed = 4
    Executed = 5


class Status(NamedTuple):
    command: Command
    stage: Stage
    seq: int
    deps: List[Slot]


# updateAttributes(slot, status)