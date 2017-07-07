from typing import Dict, List, SupportsInt, Set, Tuple

import logging
from enum import Enum

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Instance, Slot, State, Ballot
from dsm.epaxos.instance.store import InstanceStore
from dsm.epaxos.replica.abstract import Behaviour
from dsm.epaxos.replica.state import ReplicaState

logger = logging.getLogger(__name__)


class LeaderInstanceState(Enum):
    PreAccept = 1

    ExplicitPrepare = 10


class LeaderInstance:
    def __init__(self, state: LeaderInstancePayload, peer_client: SupportsInt, allow_fast=True):
        self.state = state
        self.peer_client = peer_client
        self.allow_fast = allow_fast


class LeaderInstancePayload:
    name = None  # type: LeaderInstanceState


class PreAcceptLeaderInstance(LeaderInstancePayload):
    name = LeaderInstanceState.PreAccept

    def __init__(self):
        self.replies = []  # type: List[Tuple[SupportsInt, SupportsInt, Set[Slot]]]


class ExplicitPrepareLeaderInstance(LeaderInstancePayload):
    name = LeaderInstanceState.ExplicitPrepare

    def __init__(self):
        self.replies = []  # type: List[Tuple[SupportsInt, Ballot, AbstractCommand, SupportsInt, Set[Slot]]]


class Leader(Behaviour):
    def __init__(
        self,
        state: ReplicaState,
        store: InstanceStore,
    ):
        super().__init__(state, store)
        self.leading = {}  # type: Dict[Slot, LeaderInstance]
        self.next_instance_id = 0

    def __contains__(self, item):
        return item in self.leading

    def __getitem__(self, item: Slot):
        return self.leading[item]

    def __setitem__(self, key: Slot, value: LeaderInstance):
        self.leading[key] = value

    def __delitem__(self, key: Slot):
        del self.leading[key]

    @property
    def peers_fast(self):
        return {x for x in self.state.quorum_fast if x != self.state.peer}

    @property
    def peers_full(self):
        return {x for x in self.state.quorum_full if x != self.state.peer}

    @property
    def quorum_n(self):
        return int(len(self.peers_fast) / 2) + 1

    def create_request(self, client_peer: SupportsInt, command: AbstractCommand):
        slot = Slot(self.state.ident, self.next_instance_id)
        self.next_instance_id += 1
        self.begin_pre_accept(client_peer, slot, command)

    def begin_pre_accept(self, client_peer: SupportsInt, slot: Slot, command: AbstractCommand):
        deps = self.store.interferences(slot, command)
        seq = max({0} | {self.store[x].seq for x in deps}) + 1

        # TODO: we need to provide checks here (?)
        inst = Instance(slot.ballot(self.state.epoch), command, seq, deps, State.PreAccepted)

        self.store[slot] = inst
        self[slot] = LeaderInstance(PreAcceptLeaderInstance(), client_peer)

        for peer in self.peers_fast:
            # Timeouts on PreAcceptRequest are guaranteed to be handled by the Replica part of the algorithm
            self.state.channel.pre_accept_request(peer, slot, inst.ballot, inst.command, inst.seq, inst.deps)

    def pre_accept_response_check(self, slot: Slot):
        if self[slot].state.name != LeaderInstanceState.PreAccept:
            logger.warning(
                f'Replica {self.state.peer} pre_accept_reply invalid leading state {self[slot]}')
            return True

    def pre_accept_response_ack(self, peer: SupportsInt, slot: Slot, seq: SupportsInt, deps: Set[Slot]):
        if self.pre_accept_response_check(slot):
            return

        l_inst = self[slot].state  # type: PreAcceptLeaderInstance
        l_inst.replies.append((peer, seq, deps))

        if len(l_inst.replies) + 1 >= self.quorum_n:
            pass

    def pre_accept_response_nack(self, peer: SupportsInt, slot: Slot):
        if self.pre_accept_response_check(slot):
            return

        self.begin_explicit_prepare(slot)

    def begin_explicit_prepare(self, slot: Slot):
        client_peer = None if slot not in self else self[slot].peer_client
        self[slot] = LeaderInstance(ExplicitPrepareLeaderInstance(), client_peer)
        self.store[slot].ballot = self.store[slot].ballot.next()

        inst = self.store[slot]
        l_inst = self[slot].state  # type: ExplicitPrepareLeaderInstance

        # Fake sending a PrepareRequest to ourselves
        l_inst.replies.append((self.state.peer, inst.ballot, inst.command, inst.seq, inst.deps))

        for peer in self.peers_full:
            self.state.channel.prepare_request(peer, slot, inst.ballot)

    def prepare_response_check(self, slot):
        if self[slot].state.name != LeaderInstanceState.ExplicitPrepare:
            logger.warning(
                f'Replica {self.state.peer} prepare_response_check invalid leading state {self[slot]}')
            return True

    def prepare_response(self, peer: SupportsInt, slot: Slot, ballot: Ballot, command: AbstractCommand,
                         seq: SupportsInt, deps: Set[Slot]):
        if self.prepare_response_check(slot):
            return

        l_inst = self[slot].state  # type: ExplicitPrepareLeaderInstance
        l_inst.replies.append((peer, ballot, command, seq, deps))

        if len(l_inst.replies) >= self.quorum_n:
            max_ballot = max()

