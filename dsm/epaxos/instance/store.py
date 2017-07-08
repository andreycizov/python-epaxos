from typing import Dict, List

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Slot, Instance, Ballot, State


class InstanceStore:
    def __init__(self):
        self.instances_deps = {}
        self.instances = {}  # type: Dict[Slot, Instance]

    def __contains__(self, item: Slot):
        return item in self.instances

    def __getitem__(self, item: Slot):
        return self.instances[item]

    def create(self, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int, deps: List[Slot]):
        self.instances[slot] = Instance(ballot, command, seq, deps, State.PreAccepted)

    def update_deps(self, slot: Slot, add_seq: int = 0, add_deps: List[Slot] = []):
        # TODO: what if one of our deps does not exist in our history ?

        deps = self.dependencies(slot)
        deps = sorted(set(add_deps + deps))

        seq = max((self[x].seq for x in deps), default=0) + 1
        seq = max([seq, add_seq])

        self[slot].set_deps(seq, deps)

    def dependencies(self, slot: Slot):
        """
        Currently we assume that the dependencies between commands may only be transitional
        """
        return sorted(
            inst_slot
            for inst_slot, v in self.instances.items()
            if (self[slot].command.ident // 1000) == (v.command.ident // 1000) and inst_slot != slot
        )
