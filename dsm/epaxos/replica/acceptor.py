from typing import List

import logging

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Slot, Ballot, StateType, CommittedState
from dsm.epaxos.instance.store import InstanceStore
from dsm.epaxos.network.peer import AcceptorInterface
from dsm.epaxos.replica.abstract import Behaviour
from dsm.epaxos.replica.leader import Leader
from dsm.epaxos.replica.state import ReplicaState

logger = logging.getLogger(__name__)


class Acceptor(Behaviour, AcceptorInterface):
    def __init__(
        self,
        state: ReplicaState,
        store: InstanceStore,
        leader: Leader,
    ):
        super().__init__(state, store)
        self.leader = leader

    def _check_if_known(self, slot: Slot, ballot: Ballot, disallow_empty=False):
        inst = self.store[slot]

        if inst.ballot > ballot or (disallow_empty and inst.type == StateType.Prepared):
            return None
        else:
            return inst

    def pre_accept_request(
        self, peer: int, slot: Slot, ballot: Ballot, command: AbstractCommand,
        seq: int,
        deps: List[Slot]):

        inst = self._check_if_known(slot, ballot)

        if inst is None or inst.type > StateType.PreAccepted:
            self.state.channel.pre_accept_response_nack(peer, slot)
        else:
            # Responding as an Acceptor should cancel our Leadership State (we will receive packets out of order)
            self.leader._stop_leadership(slot)
            inst = self.store.pre_accept(slot, ballot, command, seq, deps)
            self.state.channel.pre_accept_response_ack(peer, slot, inst.ballot, inst.seq, inst.deps)

    def accept_request(self, peer: int, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int,
                       deps: List[Slot]):
        inst = self._check_if_known(slot, ballot)

        if inst is None or inst.type > StateType.Accepted:
            self.state.channel.accept_response_nack(peer, slot)
        else:
            self.leader._stop_leadership(slot)
            inst = self.store.accept(slot, ballot, command, seq, deps)
            self.state.channel.accept_response_ack(peer, slot, inst.ballot)

    def commit_request(self, peer: int, slot: Slot, ballot: Ballot, seq: int, command: AbstractCommand,
                       deps: List[Slot]):
        inst = self._check_if_known(slot, ballot)

        # We check for the StateType here, since if were required to Commit after having already committed -
        # then the leader of ExplicitPrepare phase has missed our reply.

        if inst is None or inst.type > StateType.Committed:
            pass
            # logger.warning(
            #     f'Acceptor `{self.state.replica_id}` Slot `{slot}` Ballot {ballot} < {self.store[slot].ballot}')
        else:
            self.leader._stop_leadership(slot)
            self.store.commit(slot, ballot, command, seq, deps)

    def prepare_request(self, peer: int, slot: Slot, ballot: Ballot):
        inst = self._check_if_known(slot, ballot, True)

        if inst is None or inst.ballot >= ballot:
            self.state.channel.prepare_response_nack(peer, slot)
        else:
            self.leader._stop_leadership(slot)
            self.state.channel.prepare_response_ack(peer, slot, inst.ballot, inst.command, inst.seq, inst.deps,
                                                    inst.type)
            # self.store.increase_ballot(slot, ballot)

    def check_timeouts_minimum_wait(self):
        return self.store.timeout_store.minimum_wait()

    def check_timeouts(self):
        for slot in self.store.timeout_store.query():
            self.leader.begin_explicit_prepare(slot)
