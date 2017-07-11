from itertools import groupby
from typing import Dict, List, NamedTuple, Optional, Tuple

import logging
from enum import Enum
from functools import reduce

from copy import copy

from dsm.epaxos.command.state import AbstractCommand, Noop
from dsm.epaxos.instance.state import Slot, StateType, Ballot, PostPreparedState, PreparedState, PreAcceptedState
from dsm.epaxos.instance.store import InstanceStore
from dsm.epaxos.network.peer import LeaderInterface
from dsm.epaxos.replica.abstract import Behaviour
from dsm.epaxos.replica.state import ReplicaState

logger = logging.getLogger(__name__)


class LeaderStateType(Enum):
    Initial = 0
    PreAccept = 1
    Accept = 2

    ExplicitPrepare = 10


class LeaderStatePhase:
    name = LeaderStateType.Initial


class LeaderState:
    def __init__(self, peer_client: Optional[int] = None, allow_fast=True):
        self.phase = LeaderStatePhase()
        self.peer_client = peer_client
        self.allow_fast = allow_fast

    def set_state(self, state: LeaderStatePhase):
        self.phase = state


class PreAcceptReply(NamedTuple):
    peer: int
    seq: int
    deps: List[Slot]


class PreAcceptLeaderPhase(LeaderStatePhase):
    name = LeaderStateType.PreAccept

    def __init__(self):
        self.replies = []  # type: List[PreAcceptReply]


class AcceptReply(NamedTuple):
    peer: int


class AcceptLeaderPhase(LeaderStatePhase):
    name = LeaderStateType.Accept

    def __init__(self):
        self.replies = []  # type: List[AcceptReply]


class ExplicitPrepareReply(NamedTuple):
    peer: int
    ballot: Ballot
    command: AbstractCommand
    seq: int
    deps: List[Slot]
    state: StateType


class ExplicitPrepareLeaderPhase(LeaderStatePhase):
    name = LeaderStateType.ExplicitPrepare

    def __init__(self):
        self.replies = []  # type: List[ExplicitPrepareReply]
        self.replies_nack = 0


class Leader(Behaviour, LeaderInterface):
    def __init__(
        self,
        state: ReplicaState,
        store: InstanceStore,
    ):
        super().__init__(state, store)
        self.leading = {}  # type: Dict[Slot, LeaderState]
        self.next_instance_id = 0

    def __contains__(self, item):
        return item in self.leading

    def __getitem__(self, item: Slot):
        return self.leading[item]

    def __setitem__(self, key: Slot, value: LeaderState):
        self.leading[key] = value

    def __delitem__(self, key: Slot):
        del self.leading[key]

    @property
    def peers_fast(self):
        return {x for x in self.state.quorum_fast if x != self.state.replica_id}

    @property
    def peers_full(self):
        return {x for x in self.state.quorum_full if x != self.state.replica_id}

    @property
    def quorum_f(self):
        return int(len(self.peers_fast) / 2)

    @property
    def quorum_slow(self):
        return self.quorum_f + 1

    @property
    def quorum_fast(self):
        return self.quorum_f * 2

    def _response_slot_state_check(self, slot: Slot, required_state: LeaderStateType):
        if slot not in self:
            logger.warning(
                f'Leader `{self.state.replica_id}` Slot `{slot}` is not leading <anymore> at this replica')
            return True

        if self[slot].phase.name != required_state:
            logger.warning(
                f'Leader `{self.state.replica_id}` Slot `{slot}` invalid leading phase `{self[slot].phase.name}` (REQUIRED: `{required_state}`)')
            return True

    def _start_leadership(self, slot: Slot, state: LeaderStatePhase, client_peer: Optional[int] = None):
        if slot not in self:
            self[slot] = LeaderState(client_peer)
        self[slot].set_state(state)

    def _stop_leadership(self, slot: Slot):
        del self[slot]

    def client_request(self, client_peer: int, command: AbstractCommand):
        # Slot does not access other slots at all (apart from it's dependency checks)

        slot = Slot(self.state.replica_id, self.next_instance_id)
        self.next_instance_id += 1

        self.begin_pre_accept(slot, command, client_peer)

    def begin_pre_accept(self, slot: Slot, command: Optional[AbstractCommand] = None, client_peer: Optional[int] = None):
        self._start_leadership(slot, PreAcceptLeaderPhase(), client_peer)

        inst = self.store[slot]

        if command:
            self.store.pre_accept(slot, inst.ballot, command)
        else:
            assert isinstance(inst, PostPreparedState)
            self.store.pre_accept(slot, inst.ballot, inst.command)

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
            deps = sorted(reduce(lambda a, b: a + b, (x.deps for x in replies), list()))

            inst = copy(self.store[slot])

            assert isinstance(inst, PreAcceptedState), repr(inst)

            inst.seq = seq
            inst.deps = deps

            self.store[slot] = inst
            self.begin_accept(slot)

    def begin_accept(self, slot: Slot):
        self._start_leadership(slot, AcceptLeaderPhase())

        inst = self.store[slot]

        self.store.accept(slot, inst.ballot, inst.command, inst.seq, inst.deps)

        for peer in self.peers_full:
            self.state.channel.accept_request(peer, slot, inst.ballot, inst.command, inst.seq, inst.deps)

    def finalise_accept(self, slot: Slot):
        peer = self[slot].peer_client
        if peer:
            self.state.channel.client_response(peer, self.store[slot].command, slot)

        self.begin_commit(slot)

    def begin_commit(self, slot: Slot):
        inst = self.store[slot]

        self.store.commit(slot, inst.ballot, inst.command, inst.seq, inst.deps)

        client_peer = self[slot].peer_client
        if client_peer:
            self.state.channel.client_response(client_peer, self.store[slot].command)

        for peer in self.peers_full:
            self.state.channel.commit_request(peer, slot, inst.ballot, inst.command, inst.seq, inst.deps)

        self.finalise_commit(slot)

    def finalise_commit(self, slot: Slot):
        self._stop_leadership(slot)

    def begin_explicit_prepare(self, slot: Slot):
        self._start_leadership(slot, ExplicitPrepareLeaderPhase())

        self.store.increase_ballot(slot)

        for peer in self.peers_full:
            self.state.channel.prepare_request(peer, slot, self.store[slot].ballot)

        # Fake sending a PrepareRequest to ourselves
        inst = self.store[slot]

        if isinstance(inst, PostPreparedState):
            phase = self[slot].phase  # type: ExplicitPrepareLeaderPhase
            phase.replies.append(
                ExplicitPrepareReply(self.state.replica_id, inst.ballot, inst.command, inst.seq, inst.deps, inst.type))

    def finalise_explicit_prepare(self, slot: Slot, replies: List[ExplicitPrepareReply]):
        max_state = max((x.state for x in replies), default=StateType.Prepared)
        max_ballot = max((x.ballot for x in replies))

        own_inst = self.store[slot]

        if max_state == StateType.Committed:
            reply = [x for x in replies if x.state == StateType.Committed][0]
            self.store.commit(slot, own_inst.ballot, reply.command, reply.seq, reply.deps)
            self.begin_commit(slot)
        elif max_state == StateType.Accepted:
            reply = [x for x in replies if x.state == StateType.Accepted][0]
            self.store.accept(slot, own_inst.ballot, reply.command, reply.seq, reply.deps)
            self.begin_accept(slot)
        elif max_state == StateType.PreAccepted:
            def key(x: ExplicitPrepareReply):
                return x.command, x.seq, x.deps

            selected = sorted(replies, key=key)
            selected = groupby(selected, key=key)
            selected = [
                (x, list(y))
                for x, y in selected
            ]  # type: List[Tuple[Tuple[AbstractCommand, int, List[int]], List[ExplicitPrepareReply]]]
            selected = [
                y
                for x, y in selected
                if len(y) >= self.quorum_slow - 1 and
                   not any(z.peer == self.store[slot].slot.replica_id for z in y)
            ]

            if len(selected):
                reply = selected[0][0]

                self.store.pre_accept(slot, own_inst.ballot, reply.command, reply.seq, reply.deps)
                self.begin_accept(slot)
            else:
                reply = [x for x in replies if x.state == StateType.PreAccepted][0]
                self.store.pre_accept(slot, own_inst.ballot, reply.command, reply.seq, reply.deps)
                self[slot].allow_fast = False
                self.begin_pre_accept(slot)
        else:
            self[slot].allow_fast = False
            self.begin_pre_accept(slot, Noop)

    def pre_accept_response_ack(self, peer: int, slot: Slot, ballot: Ballot, seq: int, deps: List[Slot]):
        if self._response_slot_state_check(slot, LeaderStateType.PreAccept):
            return

        if self.store[slot].ballot < ballot:
            self.pre_accept_response_nack(peer, slot)
        else:
            phase = self[slot].phase  # type: PreAcceptLeaderPhase
            phase.replies.append(PreAcceptReply(peer, seq, deps))

            if len(phase.replies) + 1 >= self.quorum_fast:
                self.finalise_pre_accept(slot, phase.replies)

    def pre_accept_response_nack(self, peer: int, slot: Slot):
        if self._response_slot_state_check(slot, LeaderStateType.PreAccept):
            return

        self.begin_explicit_prepare(slot)

    def accept_response_ack(self, peer: int, slot: Slot, ballot: Ballot):
        if self._response_slot_state_check(slot, LeaderStateType.Accept):
            return

        if self.store[slot].ballot < ballot:
            self.accept_response_nack(peer, slot)
        else:
            phase = self[slot].phase  # type: AcceptLeaderPhase
            phase.replies.append(AcceptReply(peer))

            if len(phase.replies) >= self.quorum_f:
                self.begin_commit(slot)

    def accept_response_nack(self, peer: int, slot: Slot):
        if self._response_slot_state_check(slot, LeaderStateType.Accept):
            return

        self.begin_explicit_prepare(slot)

    def prepare_response_ack(self, peer: int, slot: Slot, ballot: Ballot, command: AbstractCommand,
                             seq: int, deps: List[Slot], state: StateType):
        if self._response_slot_state_check(slot, LeaderStateType.ExplicitPrepare):
            return

        phase = self[slot].phase  # type: ExplicitPrepareLeaderPhase
        phase.replies.append(ExplicitPrepareReply(peer, ballot, command, seq, deps, state))

        if len(phase.replies) >= self.quorum_slow:
            self.finalise_explicit_prepare(slot, phase.replies)

    def prepare_response_nack(self, peer: int, slot: Slot):
        if self._response_slot_state_check(slot, LeaderStateType.ExplicitPrepare):
            return

        phase = self[slot].phase  # type: ExplicitPrepareLeaderPhase
        phase.replies_nack += 1

        if phase.replies_nack >= self.quorum_slow:
            self.begin_explicit_prepare(slot)
