from typing import Dict, SupportsInt, Set

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Slot, Instance, Ballot, State


class InstanceStore:
    def __init__(self):
        self.instances = {}  # type: Dict[Slot, Instance]

    def __getitem__(self, item: Slot):
        return self.instances[item]

    def create(self, slot: Slot, ballot: Ballot, command, seq: SupportsInt, deps: Set[Slot]):
        self.instances[slot] = Instance(ballot, command, seq, deps, State.PreAccepted)

    # def __setitem__(self, key: Slot, value: Instance):
    #     self.instances[key] = value

    def interferences(self, slot: Slot, command: AbstractCommand):
        """
        Currently we assume that the dependencies between commands may only be transitional
        """
        return {inst_slot for inst_slot, v in self.instances.items() if
                (command.ident // 1000) == (v.command.ident // 1000) and (slot < inst_slot)}
