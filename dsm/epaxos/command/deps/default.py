from typing import NamedTuple, List

from dsm.epaxos.command.deps.store import AbstractDepsStore
from dsm.epaxos.command.state import AbstractCommand, Noop
from dsm.epaxos.instance.state import Slot, InstanceState, PostPreparedState


class DefaultDepsStoreState(NamedTuple):
    slot: Slot
    seq: int


class DefaultDepsStore(AbstractDepsStore):
    def __init__(self):
        super().__init__()
        self.last_ident_slot = {}  # type: Dict[int, DefaultDepsStoreState]

    def _key(self, command: AbstractCommand):
        return command.ident // 1000

    def update(self, slot: Slot, old_inst: InstanceState, new_inst: InstanceState):
        if isinstance(new_inst, PostPreparedState):
            key = self._key(new_inst.command)

            if key not in self.last_ident_slot or self.last_ident_slot[key].slot == slot or self.last_ident_slot[
                key].seq < new_inst.seq:
                self.last_ident_slot[key] = DefaultDepsStoreState(slot, new_inst.seq)

    def query(self, slot: Slot, command: AbstractCommand) -> List[Slot]:
        if command == Noop:
            return []

        key = self._key(command)

        if key not in self.last_ident_slot:
            return []

        dep_slot, _ = self.last_ident_slot[key]

        if dep_slot == slot:
            return []
        else:
            return sorted([dep_slot])