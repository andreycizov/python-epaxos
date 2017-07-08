from typing import List

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Slot, Ballot, State
from dsm.epaxos.instance.store import InstanceStore
from dsm.epaxos.network.peer import AcceptorInterface
from dsm.epaxos.replica.abstract import Behaviour
from dsm.epaxos.replica.state import ReplicaState


class Acceptor(Behaviour, AcceptorInterface):
    def __init__(
        self,
        state: ReplicaState,
        store: InstanceStore,
    ):
        super().__init__(state, store)

    def _check_if_known(self, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int, deps: List[Slot],
                        state: State):
        if slot not in self.store:
            self.store.create(slot, ballot, command, seq, deps)
            self.store[slot].set_state(state)
        elif ballot < self.store[slot].ballot:
            return True
        else:
            inst = self.store[slot]

            inst.set_command(command)
            inst.set_ballot(ballot)
            inst.set_deps(seq, deps)
            inst.set_state(state)

    def pre_accept_request(
        self, peer: int, slot: Slot, ballot: Ballot, command: AbstractCommand,
        seq: int,
        deps: List[Slot]):

        #

        if slot not in self.store:
            self.store.create(slot, ballot, command, 0, [])

        raise NotImplementedError()

    def accept_request(self, peer: int, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int,
                       deps: List[Slot]):
        raise NotImplementedError()

    def commit_request(self, peer: int, slot: Slot, ballot: Ballot, seq: int, command: AbstractCommand,
                       deps: List[Slot]):
        raise NotImplementedError()

    def prepare_request(self, peer: int, slot: Slot, ballot: Ballot):
        pass

    def execute(self):
        # TODO: While executing, we may find some instances that we depend upon but which are still not committed
        # TODO: or which may still not exist at all in our banks - we
        pass