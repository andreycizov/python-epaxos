from typing import NamedTuple, Dict

from dsm.epaxos.cmd.state import CommandID
from dsm.epaxos.inst.state import Slot
from dsm.epaxos.inst.store import InstanceStoreState


class Load(NamedTuple):
    slot: Slot

    def __repr__(self):
        return f'Load({self.slot})'


class LoadCommandSlot(NamedTuple):
    id: CommandID


class Store(NamedTuple):
    slot: Slot
    inst: InstanceStoreState

    def __repr__(self):
        return f'Store({self.slot},{self.inst})'


class InstanceState(NamedTuple):
    slot: Slot
    inst: InstanceStoreState


class CheckpointEvent(NamedTuple):
    slot: Slot
    at: Dict[int, Slot]
