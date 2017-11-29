from itertools import groupby
from typing import NamedTuple, List, Dict, Optional

from dsm.epaxos.command.deps.store import AbstractDepsStore
from dsm.epaxos.command.state import Command, Checkpoint, Mutator
from dsm.epaxos.instance.state import Slot, InstanceState, PostPreparedState
from dsm.epaxos.instance.store import last


class DefaultDepsStoreState(NamedTuple):
    slot: Slot
    seq: int


class DefaultDepsStore(AbstractDepsStore):
    def __init__(self):
        super().__init__()
        self.last_ident_slot = {}  # type: Dict[int, DefaultDepsStoreState]
        self.last_cp = None  # type: Optional[DefaultDepsStoreState]
        self.last_cp_deps = None

    def _key(self, command: Optional[Command]):
        # TODO: we would like to keep track of commands that have been seen, but not executed yet; then arrange their dependencies accordingly.

        # TODO: here, if the command gets into a second PreAccept phase (After ExplicitPrepare) -> it will then obtain an incorrect dependency on a future slot.

        p = command.payload

        if isinstance(p, Checkpoint):
            return -1
        elif isinstance(p, Mutator):
            return p.key // 5

    def update(self, slot: Slot, old_inst: InstanceState, new_inst: InstanceState):
        if isinstance(new_inst, PostPreparedState):
            cmd = new_inst.command

            if cmd is None:
                return

            key = self._key(cmd)

            if isinstance(cmd.payload, Checkpoint):
                if self.last_cp is None or self.last_cp.slot == slot or self.last_cp.seq < new_inst.seq:
                    self.last_cp = DefaultDepsStoreState(slot, new_inst.seq)

                if self.last_cp is None or self.last_cp.slot != slot:
                    self.last_ident_slot = {}
                return

            if key not in self.last_ident_slot or self.last_ident_slot[key].slot == slot or self.last_ident_slot[
                key].seq < new_inst.seq:
                self.last_ident_slot[key] = DefaultDepsStoreState(slot, new_inst.seq)

    def query(self, slot: Slot, command: Optional[Command]) -> List[Slot]:
        if command is None:
            return []

        key = self._key(command)

        if isinstance(command.payload, Checkpoint):
            last_cp_dep = [] if self.last_cp is None else [self.last_cp]

            its = [max(y, key=lambda x: x.slot) for _, y in groupby(sorted(self.last_ident_slot.values(), key=lambda x: x.slot), key=lambda x: x.slot.replica_id)]
            deps = sorted(its + last_cp_dep, key=lambda x: x.slot)

            return sorted(set([x.slot for x in deps]))

        if key not in self.last_ident_slot:
            if self.last_cp:
                return [self.last_cp.slot]
            else:
                return []

        dep_slot, _ = self.last_ident_slot[key]

        if dep_slot == slot:
            return []
        else:
            return sorted([dep_slot])
