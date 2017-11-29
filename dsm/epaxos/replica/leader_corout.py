import logging
from collections import defaultdict
from copy import copy
from functools import reduce
from itertools import groupby
from typing import NamedTuple, List, ClassVar, TypeVar, Union, Optional, Tuple, Any

from dsm.epaxos.command.state import Command
from dsm.epaxos.instance.state import PostPreparedState, StateType, InstanceState, PreparedState, Slot, Ballot, \
    AcceptedState, CommittedState, PreAcceptedState
from dsm.epaxos.network import packet

logger = logging.getLogger(__name__)


class Quorum(NamedTuple):
    peers: List[int]
    replica_id: int
    epoch: int

    @property
    def failure_size(self):
        return (len(self.peers) + 1) // 2

    @property
    def fast_size(self):
        return self.failure_size * 2

    @property
    def slow_size(self):
        return self.failure_size + 1


class Send(NamedTuple):
    dest: int
    payload: packet.Payload

    def __repr__(self):
        return f'Send({self.dest}, {self.payload})'


T_sub_payload = TypeVar('T')


class Receive(NamedTuple):
    type: List[Any]

    @classmethod
    def any(cls, *events):
        return Receive(events)

    def __repr__(self):
        rs = ','.join(f'{x.__name__}' for x in self.type)
        return f'Receive({rs})'


class Load(NamedTuple):
    slot: Slot

    def __repr__(self):
        return f'Load({self.slot})'


class LoadPostPrepared(NamedTuple):
    slot: Slot

    def __repr__(self):
        return f'LoadPostPrepared({self.slot})'


class StorePostPrepared(NamedTuple):
    slot: Slot
    inst: PostPreparedState

    def __repr__(self):
        return f'StorePostPrepared({self.slot}, {self.inst})'


class LeaderException(Exception):
    pass


class ExplicitPrepare(LeaderException):
    def __init__(self, reason=None):
        self.reason = reason


def leader_client_request(q: Quorum, slot: Slot, cmd, seq, deps):
    yield StorePostPrepared(
        slot,
        PreAcceptedState(
            slot,
            slot.ballot_initial(q.epoch),
            cmd,
            seq,
            deps
        )
    )
    yield from leader_pre_accept(q, slot, True)


def leader_explicit_prepare(q: Quorum, slot: Slot, reason=None):
    inst = yield Load(slot)  # type: InstanceState

    ballot = inst.ballot.next(q.replica_id) if inst else slot.ballot_initial(q.replica_id)

    inst = inst.with_ballot(ballot)

    # logger.debug(f'{q.replica_id} explicit prepare {inst} {ballot} {reason}')

    yield StorePostPrepared(
        slot,
        inst
    )

    for peer in q.peers:
        yield Send(peer, packet.PrepareRequest(slot, ballot))

    yield Send(q.replica_id, packet.PrepareRequest(slot, ballot))

    class Reply(NamedTuple):
        p: int
        r: packet.PrepareResponseAck

    replies = []  # type: List[Reply]

    while len(replies) < q.slow_size:
        peer, (ack, nack) = yield Receive.any(
            packet.PrepareResponseAck,
            packet.PreAcceptResponseNack,
        )  # type: Tuple[int, Tuple[Optional[packet.PrepareResponseAck], Optional[packet.PrepareResponseNack]]]

        if ack:
            ack = ack  # type: packet.PrepareResponseAck

            if ack.state == StateType.Committed:
                new_inst = ack.inst.with_ballot(ballot)
                yield StorePostPrepared(slot, new_inst)
                yield from leader_commit(q, slot)
                return

            # if ack.ballot < ballot:
            #    delayed reply
            #    # continue

            # todo: should replies from non-current ballots be ignored?
            replies.append(Reply(peer, ack))

        if nack:
            # logger.debug(f'{q.replica_id} explicit prepare NACK {inst} {ballot}')
            raise ExplicitPrepare('explicit:NACK')

    len_rep = len(replies)

    max_ballot = max(x.r.ballot for x in replies)
    replies = [x for x in replies if x.r.ballot == max_ballot]
    max_state = max(x.r.state for x in replies)

    replies = [x for x in replies if x.r.state == max_state]

    len_rep_fil = len(replies)

    loglog = lambda ni, tag=None: logger.info(f'{q.replica_id} Storing {ni} {len_rep} {len_rep_fil} {ballot} {tag}')
    loglog = lambda ni, tag=None: None

    if max_state == StateType.Committed:
        reply = [x.r for x in replies if x.state == max_state][0]  # type: packet.PrepareResponseAck
        new_inst = reply.inst.with_ballot(ballot)

        yield StorePostPrepared(slot, new_inst)
        loglog(new_inst)
        yield from leader_commit(q, slot)
        return
    elif max_state == StateType.Accepted:
        reply = [x.r for x in replies if x.r.state == max_state][0]  # type: packet.PrepareResponseAck
        new_inst = reply.inst.with_ballot(ballot)
        yield StorePostPrepared(slot, new_inst)
        loglog(new_inst)
        yield from leader_accept(q, slot)
        return

    # def key(x: packet.PrepareResponseAck):
    #     return x.r.state, x.r.command, x.r.seq, x.r.deps

    identic_keys = defaultdict(list)

    for r in replies:
        identic_keys[(r.r.state, r.r.command, r.r.seq, tuple(sorted(r.r.deps)))].append(r)

    identic_groups = [
        (x, list(y))
        for x, y in identic_keys.items()
    ]  # type: List[Tuple[Tuple[InstanceState, Command, int, List[int]], List[Reply]]]
    identic_groups = [
        y
        for x, y in identic_groups
        if len(y) >= q.slow_size - 1 and
           all(z.p != inst.slot.replica_id for z in y) and x[0] == StateType.PreAccepted
    ]

    if len(identic_groups):
        reply = identic_groups[0][0].r
        new_inst = reply.inst.with_ballot(ballot)
        yield StorePostPrepared(slot, new_inst)
        loglog(new_inst, 'identic')
        yield from leader_accept(q, slot)
    elif max_state == StateType.PreAccepted:
        reply = [x for x in replies if x.r.state == max_state][0].r
        new_inst = reply.inst.with_ballot(ballot)
        loglog(new_inst)
        yield StorePostPrepared(slot, new_inst)
        yield from leader_pre_accept(q, slot, False)
    else:
        new_inst = PreAcceptedState.noop(inst.slot, ballot)
        yield StorePostPrepared(slot, new_inst)
        loglog(new_inst)
        yield from leader_pre_accept(q, slot, False)


def leader_pre_accept(q: Quorum, slot: Slot, allow_fast: True):
    inst = yield LoadPostPrepared(slot)  # type: PostPreparedState

    for peer in q.peers:
        yield Send(peer, packet.PreAcceptRequest(inst.slot, inst.ballot, inst.command, inst.seq, inst.deps))

    replies = []
    while len(replies) + 1 < q.slow_size:
        _, (ack, nack) = yield Receive.any(
            packet.PreAcceptResponseAck,
            packet.PreAcceptResponseNack,
        )  # type: Tuple[Optional[packet.PreAcceptResponseAck], Optional[packet.PreAcceptResponseNack]]

        if ack:
            if ack.ballot != inst.ballot:
                # logger.debug(f'{q.replica_id} pre_accept Raising do to > {ack} {nack} {ack} {inst}')
                # raise ExplicitPrepare('pre_accept:BALLOT')
                pass
            else:
                replies.append(ack)
        if nack:
            # logger.debug(f'{q.replica_id} pre_accept Raising do to nack {ack} {nack} {ack} {inst}')
            pass
            # raise ExplicitPrepare('pre_accept:NACK')

    if allow_fast and (
            all(x.deps == inst.deps for x in replies) and
            all(x.seq == inst.seq for x in replies)
    ):
        yield StorePostPrepared(slot, inst.promote(StateType.Committed))
        yield from leader_commit(q, slot)
    else:
        seq = max(inst.seq, max(x.seq for x in replies))
        deps = sorted(set(list(reduce(lambda a, b: a + b, (x.deps for x in replies)) + inst.deps)))

        inst = inst.update(seq, deps).promote(StateType.Accepted)

        yield StorePostPrepared(slot, inst)
        yield from leader_accept(q, slot)


def leader_accept(q: Quorum, slot: Slot):
    inst = yield LoadPostPrepared(slot)  # type: PostPreparedState

    for peer in q.peers:
        yield Send(peer, packet.AcceptRequest(inst.slot, inst.ballot, inst.command, inst.seq, inst.deps))

    replies = []

    while len(replies) + 1 < q.slow_size:
        _, (ack, nack) = yield Receive.any(
            packet.AcceptResponseAck,
            packet.AcceptResponseNack,
        )  # type: Tuple[Optional[packet.AcceptResponseAck], Optional[packet.AcceptResponseNack]]

        if ack:
            if ack.ballot != inst.ballot:
                # logger.debug(f'{q.replica_id} accept Raising do to > {ack} {nack} {ack} {inst}')
                # raise ExplicitPrepare('accept:BALLOT')
                pass
            else:
                replies.append(ack)

        if nack:
            # logger.debug(f'{q.replica_id} accept Raising do to nack {ack} {nack} {ack} {inst}')
            pass
            # raise ExplicitPrepare('accept:NACK')

    yield StorePostPrepared(slot, inst.promote(StateType.Committed))
    yield from leader_commit(q, slot)


def leader_commit(q: Quorum, slot: Slot):
    inst = yield LoadPostPrepared(slot)  # type: PostPreparedState

    for peer in q.peers:
        yield Send(peer, packet.CommitRequest(inst.slot, inst.ballot, inst.command, inst.seq, inst.deps))
