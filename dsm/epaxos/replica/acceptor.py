from typing import List

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Slot, Ballot, StateType
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

    def _check_if_known(self, slot: Slot, ballot: Ballot):
        inst = self.store[slot]

        if inst.ballot > ballot:
            return None
        else:
            return inst

    def pre_accept_request(
        self, peer: int, slot: Slot, ballot: Ballot, command: AbstractCommand,
        seq: int,
        deps: List[Slot]):

        inst = self._check_if_known(slot, ballot)

        if inst is None:
            self.state.channel.pre_accept_response_nack(peer, slot)
        else:
            inst = self.store.pre_accept(slot, ballot, command, seq, deps)
            self.state.channel.pre_accept_response_ack(peer, slot, inst.ballot, inst.seq, inst.deps)

    def accept_request(self, peer: int, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int,
                       deps: List[Slot]):
        inst = self._check_if_known(slot, ballot)

        if inst is None:
            self.state.channel.acc

    def commit_request(self, peer: int, slot: Slot, ballot: Ballot, seq: int, command: AbstractCommand,
                       deps: List[Slot]):
        raise NotImplementedError()

    def prepare_request(self, peer: int, slot: Slot, ballot: Ballot):
        pass

    def execute(self):
        # TODO: While executing, we may find some instances that we depend upon but which are still not committed
        # TODO: or which may still not exist at all in our banks - we
        pass