from typing import NamedTuple

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
    state: InstanceStoreState

    def __repr__(self):
        return f'Store({self.slot},{self.state})'


class SlotState(NamedTuple):
    slot: Slot
    inst: InstanceStoreState