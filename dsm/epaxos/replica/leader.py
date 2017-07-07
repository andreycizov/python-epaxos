from typing import Dict, List, NamedTuple, Optional

import logging
from enum import Enum
from functools import reduce

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Slot, State, Ballot
from dsm.epaxos.instance.store import InstanceStore
from dsm.epaxos.replica.abstract import Behaviour
from dsm.epaxos.replica.state import ReplicaState

logger = logging.getLogger(__name__)


class LeaderInstanceState(Enum):
    Initial = 0
    PreAccept = 1

    ExplicitPrepare = 10


class LeaderInstance:
    def __init__(self, peer_client: Optional[int] = None, allow_fast=True):
        self.state = LeaderInstancePayload()
        self.peer_client = peer_client
        self.allow_fast = allow_fast

    def set_state(self, state: LeaderInstancePayload):
        self.state = state


class LeaderInstancePayload:
    name = LeaderInstanceState.Initial


class PreAcceptReply(NamedTuple):
    peer: int
    seq: int
    deps: List[Slot]


class PreAcceptLeaderInstance(LeaderInstancePayload):
    name = LeaderInstanceState.PreAccept

    def __init__(self):
        self.replies = []  # type: List[PreAcceptReply]


class PrepareReply(NamedTuple):
    peer: int
    ballot: Ballot
    command: AbstractCommand
    seq: int
    deps: List[Slot]
    state: State


class ExplicitPrepareLeaderInstance(LeaderInstancePayload):
    name = LeaderInstanceState.ExplicitPrepare

    def __init__(self):
        self.replies = []  # type: List[PrepareReply]
        self.replies_nack = 0


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

    def _response_slot_state_check(self, slot: Slot, required_state: LeaderInstanceState):
        if slot not in self:
            logger.warning(
                f'Leader `{self.state.peer}` Slot `{slot}` is not leading at this replica')
            return True

        if self[slot].state.name != required_state:
            logger.warning(
                f'Leader `{self.state.peer}` Slot `{slot}` invalid leading state `{self[slot].state.name}` (REQUIRED: `{required_state}`)')
            return True

    def client_request(self, client_peer: int, command: AbstractCommand):
        # Slot does not access other slots at all (apart from it's dependency checks)

        slot = Slot(self.state.ident, self.next_instance_id)
        self.next_instance_id += 1

        deps = self.store.dependencies(slot, command)
        seq = max({0} | {self.store[x].seq for x in deps}) + 1

        self.store.create(slot, slot.ballot(self.state.epoch), seq, deps)

        self[slot] = LeaderInstance(client_peer)
        self.begin_pre_accept(slot)

    def begin_pre_accept(self, slot: Slot):
        self[slot].set_state(PreAcceptLeaderInstance())

        inst = self.store[slot]

        for peer in self.peers_fast:
            # Timeouts on PreAcceptRequest are guaranteed to be handled by the Acceptor part of the algorithm
            self.state.channel.pre_accept_request(peer, slot, inst.ballot, inst.command, inst.seq, inst.deps)

    def finalise_pre_accept(self, slot: Slot, replies: List[PreAcceptReply]):
        if self[slot].allow_fast and (
                reduce(lambda a, b: a == b, (x.deps for x in replies), self.store[slot].deps) and
                reduce(lambda a, b: a == b, (x.seq for x in replies), self.store[slot].seq)
        ):
            self.begin_commit(slot)
        else:
            seq = max({x.seq for x in replies})
            deps = reduce(lambda a, b: a | b, (x.deps for x in replies), set())

            self.store[slot].set_deps(seq, deps)

            self.begin_accept(slot)

    def begin_accept(self, slot: Slot):
        inst = self.store[slot]
        inst.set_state(State.Accepted)

        for peer in self.peers_full:
            self.state.channel.accept_request(peer, slot, inst.ballot, inst.command, inst.seq, inst.deps)

    def finalise_accept(self, slot: Slot):
        peer = self[slot].peer_client
        if peer:
            self.state.channel.client_response(peer, self.store[slot].command, slot)

        self.begin_commit(slot)

    def begin_commit(self, slot: Slot):
        inst = self.store[slot]
        inst.set_state(State.Committed)

        for peer in self.peers_full:
            self.state.channel.commit_request(peer, slot, inst.ballot, inst.command, inst.seq, inst.deps)

        self.finalise_commit(slot)

    def finalise_commit(self, slot: Slot):
        # We are no longer leading this request
        del self[slot]

    def begin_explicit_prepare(self, slot: Slot):
        self[slot].set_state(ExplicitPrepareLeaderInstance())
        self.store[slot].set_ballot_next()

        for peer in self.peers_full:
            self.state.channel.prepare_request(peer, slot, self.store[slot].ballot)

        # Fake sending a PrepareRequest to ourselves
        inst = self.store[slot]
        l_inst = self[slot].state  # type: ExplicitPrepareLeaderInstance
        l_inst.replies.append(PrepareReply(self.state.peer, inst.ballot, inst.command, inst.seq, inst.deps, inst.state))

    def finalise_explicit_prepare(self, slot: Slot, replies: List[PrepareReply]):
        # TODO: ballot does not allow for a comparison operation, as of yet.
        max_ballot = max(x.ballot for x in replies)
        replies = [x for x in replies if x.ballot == max_ballot]

        max_state = max(x.state for x in replies)

        if any(x.state == State.Committed for x in replies):
            # TODO: we would like to set (command, seq, deps here).
            self.begin_commit(slot)
        elif any(x.state == State.Accepted for x in replies):
            # TODO: we would like to set (command, seq, deps here).
            self.begin_accept(slot)
        elif len([x for x in replies if x.state == State.PreAccepted]) + 1 >= self.quorum_n:
            # TODO: we would like to fix the conditions here.
            # TODO: we would like to set (command, seq, deps here).
            self.begin_accept(slot)
        elif any(x.state == State.PreAccepted for x in replies):
            # TODO: we would like to set (command, seq, deps here).
            self[slot].allow_fast = False
            self.begin_pre_accept(slot)
        else:
            self[slot].allow_fast = False
            self.store[slot].set_noop()
            self.begin_pre_accept(slot)

    def pre_accept_response_ack(self, peer: int, slot: Slot, ballot: Ballot, seq: int, deps: List[Slot]):
        if self._response_slot_state_check(slot, LeaderInstanceState.PreAccept):
            return

        if self.store[slot].ballot < ballot:
            self.pre_accept_response_nack(peer, slot)

        l_inst = self[slot].state  # type: PreAcceptLeaderInstance
        l_inst.replies.append(PreAcceptReply(peer, seq, deps))

        if len(l_inst.replies) + 1 >= self.quorum_n:
            self.finalise_pre_accept(slot, l_inst.replies)

    def pre_accept_response_nack(self, peer: int, slot: Slot):
        if self._response_slot_state_check(slot, LeaderInstanceState.PreAccept):
            return

        self.begin_explicit_prepare(slot)

    def prepare_response_ack(self, peer: int, slot: Slot, ballot: Ballot, command: AbstractCommand,
                             seq: int, deps: List[Slot], state: State):
        if self._response_slot_state_check(slot, LeaderInstanceState.ExplicitPrepare):
            return

        l_inst = self[slot].state  # type: ExplicitPrepareLeaderInstance
        l_inst.replies.append(PrepareReply(peer, ballot, command, seq, deps, state))

        if len(l_inst.replies) >= self.quorum_n:
            self.finalise_explicit_prepare(slot, l_inst.replies)

    def prepare_response_nack(self, peer: int, slot: Slot):
        l_inst = self[slot].state  # type: ExplicitPrepareLeaderInstance
        l_inst.replies_nack += 1

        if l_inst.replies_nack >= self.quorum_n:
            self.begin_explicit_prepare(slot)
