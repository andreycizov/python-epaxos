from itertools import groupby
from typing import Dict, List, NamedTuple, Optional, Tuple

import logging
from enum import Enum
from functools import reduce

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Slot, State, Ballot
from dsm.epaxos.instance.store import InstanceStore
from dsm.epaxos.network.peer import LeaderInterface
from dsm.epaxos.replica.abstract import Behaviour
from dsm.epaxos.replica.state import ReplicaState

logger = logging.getLogger(__name__)


class LeaderInstanceState(Enum):
    Initial = 0
    PreAccept = 1

    ExplicitPrepare = 10


class LeaderState:
    def __init__(self, peer_client: Optional[int] = None, allow_fast=True):
        self.phase = LeaderStatePhase()
        self.peer_client = peer_client
        self.allow_fast = allow_fast

    def set_state(self, state: LeaderStatePhase):
        self.phase = state


class LeaderStatePhase:
    name = LeaderInstanceState.Initial


class PreAcceptReply(NamedTuple):
    peer: int
    seq: int
    deps: List[Slot]


class PreAcceptLeaderPhase(LeaderStatePhase):
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


class ExplicitPrepareLeaderPhase(LeaderStatePhase):
    name = LeaderInstanceState.ExplicitPrepare

    def __init__(self):
        self.replies = []  # type: List[PrepareReply]
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
        return {x for x in self.state.quorum_fast if x != self.state.peer}

    @property
    def peers_full(self):
        return {x for x in self.state.quorum_full if x != self.state.peer}

    @property
    def quorum_f(self):
        return int(len(self.peers_fast) / 2)

    @property
    def quorum_slow(self):
        return self.quorum_f + 1

    @property
    def quorum_fast(self):
        return self.quorum_f * 2

    def _response_slot_state_check(self, slot: Slot, required_state: LeaderInstanceState):
        if slot not in self:
            logger.warning(
                f'Leader `{self.phase.replica_id}` Slot `{slot}` is not leading at this replica')
            return True

        if self[slot].phase.name != required_state:
            logger.warning(
                f'Leader `{self.phase.replica_id}` Slot `{slot}` invalid leading phase `{self[slot].phase.name}` (REQUIRED: `{required_state}`)')
            return True

    def _start_leadership(self, slot: Slot, state: LeaderStatePhase, client_peer: Optional[int] = None):
        if slot not in self:
            self[slot] = LeaderState(client_peer)
        self[slot].set_state(state)

    def _stop_leadership(self, slot: Slot):
        del self[slot]

    def client_request(self, client_peer: int, command: AbstractCommand):
        # Slot does not access other slots at all (apart from it's dependency checks)

        slot = Slot(self.state.ident, self.next_instance_id)
        self.next_instance_id += 1

        # TODO: -> if deps contains a command we do not yet know about - the command is pre-created as is expected to be committed first.



        # TODO: every time we receive a pre_accept - we would like to update seq and deps.
        # TODO: but Acceptor may only need to specifically set the instance to something (command, seq, deps)

        # TODO: pre_accept|pre_accept_reply -> update seq,deps
        # TODO: accept|commit|... -> set seq,deps

        self.store.create(slot, slot.ballot(self.state.epoch), command, 1, [])

        self[slot] = LeaderState(client_peer)
        self.begin_pre_accept(slot)

    def begin_pre_accept(self, slot: Slot):
        self._start_leadership(slot, PreAcceptLeaderPhase())

        self.store.update_deps(slot)

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
        self._stop_leadership(slot)

    def begin_explicit_prepare(self, slot: Slot):
        self._start_leadership(slot, ExplicitPrepareLeaderPhase())

        # TODO: Ballot should include our replica ID as well.
        self.store[slot].set_ballot_next()
        self.store[slot].ballot.replica_id = self.state.peer

        for peer in self.peers_full:
            self.state.channel.prepare_request(peer, slot, self.store[slot].ballot)

        # Fake sending a PrepareRequest to ourselves
        inst = self.store[slot]
        phase = self[slot].phase  # type: ExplicitPrepareLeaderPhase
        phase.replies.append(PrepareReply(self.state.peer, inst.ballot, inst.command, inst.seq, inst.deps, inst.state))

    def finalise_explicit_prepare(self, slot: Slot, replies: List[PrepareReply]):
        def select_reply_and_update(items: List[PrepareReply], state: State):
            reply = [x for x in items if x.state == state][0]
            self.store[slot].set_command(reply.command)
            self.store[slot].set_deps(reply.seq, reply.deps)

        max_ballot = max(x.ballot for x in replies)
        replies = [x for x in replies if x.ballot == max_ballot]

        max_state = max(x.state for x in replies)

        if max_state == State.Committed:
            select_reply_and_update(replies, State.Committed)
            self.begin_commit(slot)
        elif max_state == State.Accepted:
            select_reply_and_update(replies, State.Accepted)
            self.begin_accept(slot)
        elif max_state == State.PreAccepted:
            def key(x: PrepareReply):
                return x.command, x.seq, x.deps

            selected = sorted(replies, key=key)
            selected = groupby(selected, key=key)
            selected = [
                (x, list(y))
                for x, y in selected
            ]  # type: List[Tuple[Tuple[AbstractCommand, int, List[int]], List[PrepareReply]]]
            selected = [
                y
                for x, y in selected
                if len(y) >= self.quorum_slow - 1 and
                   not any(z.peer == self.store[slot].ballot.leader_id for z in y)
            ]

            if len(selected):
                select_reply_and_update(selected[0], State.PreAccepted)
                self.begin_accept(slot)
            else:
                select_reply_and_update(replies, State.PreAccepted)
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

        phase = self[slot].phase  # type: PreAcceptLeaderPhase
        phase.replies.append(PreAcceptReply(peer, seq, deps))

        if len(phase.replies) + 1 >= self.quorum_fast:
            self.finalise_pre_accept(slot, phase.replies)

    def pre_accept_response_nack(self, peer: int, slot: Slot):
        if self._response_slot_state_check(slot, LeaderInstanceState.PreAccept):
            return

        self.begin_explicit_prepare(slot)

    def prepare_response_ack(self, peer: int, slot: Slot, ballot: Ballot, command: AbstractCommand,
                             seq: int, deps: List[Slot], state: State):
        if self._response_slot_state_check(slot, LeaderInstanceState.ExplicitPrepare):
            return

        phase = self[slot].phase  # type: ExplicitPrepareLeaderPhase
        phase.replies.append(PrepareReply(peer, ballot, command, seq, deps, state))

        if len(phase.replies) >= self.quorum_slow:
            self.finalise_explicit_prepare(slot, phase.replies)

    def prepare_response_nack(self, peer: int, slot: Slot):
        phase = self[slot].phase  # type: ExplicitPrepareLeaderPhase
        phase.replies_nack += 1

        if phase.replies_nack >= self.quorum_slow:
            self.begin_explicit_prepare(slot)
