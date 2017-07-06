from typing import Dict

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Slot, Instance


class InstanceStore:
    def __init__(self):
        self.instances = {}  # type: Dict[Slot, Instance]

    def __getitem__(self, item: Slot):
        return self.instances[item]

    def __setitem__(self, key: Slot, value: Instance):
        self.instances[key] = value

    def interferences(self, slot: Slot, command: AbstractCommand):
        """
        Currently we assume that the dependencies between commands may only be transitional
        """
        return {inst_slot for inst_slot, v in self.instances.items() if
                (command.ident // 1000) == (v.command.ident // 1000) and (slot < inst_slot)}
