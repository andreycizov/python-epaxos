from typing import NamedTuple, Optional

from copy import copy

from dsm.epaxos.command.state import AbstractCommand, Noop
from dsm.epaxos.instance.state import Slot, PostPreparedState, StateType, PreAcceptedState, AcceptedState, \
    CommittedState
from dsm.epaxos.network import packet
from dsm.epaxos.replica.leader_corout import Quorum, Receive, Load, LoadPostPrepared, Send, StorePostPrepared


# class Timeout(NamedTuple):
#     jiffies: int


class DepsQuery(NamedTuple):
    slot: Slot
    command: AbstractCommand


def acceptor_main(q: Quorum, slot: Slot):
    while True:
        # yield None

        peer, (pre_accept, accept, commit, prepare) = yield Receive.any(
            packet.PreAcceptRequest,
            packet.AcceptRequest,
            packet.CommitRequest,
            packet.PrepareRequest,
            # Timeout
        )

        if pre_accept:
            yield from acceptor_pre_accept(q, slot, peer, pre_accept)

        if accept:
            yield from acceptor_accept(q, slot, peer, accept)

        if commit:
            yield from acceptor_commit(q, slot, peer, commit)

        if prepare:
            yield from acceptor_prepare(q, slot, peer, prepare)

        # if timeout:


#


def acceptor_pre_accept(q: Quorum, slot: Slot, peer: int, pre_accept: packet.PreAcceptRequest):
    inst = yield Load(slot)  # type: Optional[PostPreparedState]

    if inst and inst.type in [StateType.Committed, StateType.Accepted]:
        yield Send(peer, packet.PreAcceptResponseNack(slot, 'STATE'))
        return

    if inst and pre_accept.ballot < inst.ballot:
        yield Send(peer, packet.PreAcceptResponseNack(slot, 'BALLOT'))
        return

    seq, deps = yield DepsQuery(slot, pre_accept.command)

    seq = max(seq, pre_accept.seq)
    deps = sorted(set(deps + pre_accept.deps))

    # inst = copy(inst)

    deps_comm_mask = []
    for x in deps:
        y = yield Load(x)  # type: Optional[PostPreparedState]

        deps_comm_mask.append(y and y.type >= StateType.Committed)

    if inst is None or inst.type == StateType.Prepared:
        inst = PreAcceptedState(
            slot,
            pre_accept.ballot,
            pre_accept.command,
            seq,
            deps
        )
    else:
        inst = inst.with_ballot(pre_accept.ballot).promote(StateType.PreAccepted).update(seq, deps)

    yield StorePostPrepared(
        slot,
        inst
    )

    yield Send(
        peer,
        packet.PreAcceptResponseAck(
            slot,
            inst.ballot,
            seq,
            deps,
            deps_comm_mask
        )
    )


def acceptor_accept(q: Quorum, slot: Slot, peer: int, accept: packet.AcceptRequest):
    inst = yield Load(slot)  # type: Optional[PostPreparedState]

    if inst and inst.type in [StateType.Committed, StateType.Accepted]:
        yield Send(peer, packet.AcceptResponseNack(slot))
        return

    if inst and accept.ballot < inst.ballot:
        yield Send(peer, packet.AcceptResponseNack(slot))
        return

    if inst is None or inst.type == StateType.Prepared:
        inst = AcceptedState(
            slot,
            accept.ballot,
            accept.command,
            accept.seq,
            accept.deps
        )
    else:
        inst = inst.with_ballot(accept.ballot).promote(StateType.Accepted).update(accept.seq, accept.deps)

    yield StorePostPrepared(
        slot,
        inst
    )

    yield Send(
        peer,
        packet.AcceptResponseAck(
            slot,
            accept.ballot
        )
    )


def acceptor_commit(q: Quorum, slot: Slot, peer: int, commit: packet.CommitRequest):
    inst = yield Load(slot)  # type: Optional[PostPreparedState]

    if inst and commit.ballot < inst.ballot:
        return

    if inst is None or inst.type == StateType.Prepared:
        inst = CommittedState(
            slot,
            commit.ballot,
            commit.command,
            commit.seq,
            commit.deps
        )
    else:
        inst = inst.with_ballot(commit.ballot).promote(StateType.Committed).update(commit.seq, commit.deps)

    yield StorePostPrepared(
        slot,
        inst
    )


def acceptor_prepare(q: Quorum, slot: Slot, peer: int, prepare: packet.PrepareRequest):
    inst = yield Load(slot)  # type: Optional[PostPreparedState]

    if inst and prepare.ballot < inst.ballot:
        yield Send(
            peer,
            packet.PrepareResponseNack(
                slot
            )
        )
        return

    if inst is None or inst.type == StateType.Prepared:
        yield Send(
            peer,
            packet.PrepareResponseAck(
                slot,
                prepare.ballot,
                Noop,
                0,
                [],
                StateType.Prepared
            )
        )
    else:
        yield Send(
            peer,
            packet.PrepareResponseAck(
                slot,
                inst.ballot,
                inst.command,
                inst.seq,
                inst.deps,
                inst.type
            )
        )
