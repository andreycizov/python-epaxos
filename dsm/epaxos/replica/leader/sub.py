from collections import defaultdict
from typing import NamedTuple, Optional, List

from dsm.epaxos.cmd.state import Command
from dsm.epaxos.inst.state import Slot, State, Stage
from dsm.epaxos.inst.store import InstanceStoreState, IncorrectBallot, IncorrectStage
from dsm.epaxos.net import packet
from dsm.epaxos.replica.net.ev import Send, Receive
from dsm.epaxos.replica.quorum.ev import Quorum
from dsm.epaxos.replica.state.ev import Load, Store


def leader_client_request(q: Quorum, slot: Slot, cmd: Command):
    inst = yield Store(
        slot,
        InstanceStoreState(
            slot.ballot_initial(q.epoch),
            State(
                Stage.PreAccepted,
                cmd,
                0,
                []
            )
        )
    )
    yield from leader_pre_accept(q, slot, inst, True)


class RecoveryReply(NamedTuple):
    p: int
    r: packet.PrepareResponseAck


def leader_explicit_prepare(q: Quorum, slot: Slot, reason=None):
    inst = yield Load(slot)  # type: InstanceStoreState

    try:
        ballot = inst.ballot.next(q.replica_id)
        inst = yield Store(
            slot,
            InstanceStoreState(
                ballot,
                inst.state
            )
        )  # type: InstanceStoreState
    except (IncorrectBallot, IncorrectStage) as e:
        return

    for peer in q.peers:
        yield Send(peer, packet.PrepareRequest(slot, inst.ballot))

    replies = list()  # type: List[RecoveryReply]

    replies.append(
        RecoveryReply(
            q.replica_id,
            packet.PrepareResponseAck(
                slot,
                ballot,
                inst.state.command,
                inst.state.seq,
                inst.state.deps,
                inst.state.stage
            )
        )
    )

    while len(replies) < q.slow_size:
        peer, (ack, nack) = yield Receive.any(
            packet.PrepareResponseAck,
            packet.PreAcceptResponseNack,
        )

        peer: int
        ack: Optional[packet.PrepareResponseAck]
        nack: Optional[packet.PrepareResponseNack]

        if ack:
            if ack.ballot != ballot:
                continue

            # todo: should replies from non-current ballots be ignored?
            replies.append(RecoveryReply(peer, ack))

        if nack:
            if nack.ballot != ballot:
                continue

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

    if max_state == Stage.Committed:
        reply = [x for x in replies if x.r.state == max_state][0].r

        inst = yield Store(
            slot,
            InstanceStoreState(
                ballot,
                State(
                    Stage.Committed,
                    reply.command,
                    reply.seq,
                    reply.deps
                )
            )
        )

        yield from leader_commit(q, slot, inst)
        return
    elif max_state == Stage.Accepted:
        reply = [x for x in replies if x.r.state == max_state][0].r

        inst = yield Store(
            slot,
            InstanceStoreState(
                ballot,
                State(
                    Stage.Accepted,
                    reply.command,
                    reply.seq,
                    reply.deps
                )
            )
        )

        yield from leader_accept(q, slot, inst)
        return

    identic_keys = defaultdict(list)

    for r in replies:
        identic_keys[(r.r.state, r.r.command.id if r.r.command else None, r.r.seq, tuple(sorted(r.r.deps)))].append(r)

    identic_groups = [
        (x, list(y))
        for x, y in identic_keys.items()
    ]  # type: List[Tuple[Tuple[InstanceState, Command, int, List[int]], List[RecoveryReply]]]
    identic_groups = [
        y
        for x, y in identic_groups
        if len(y) >= q.slow_size - 1 and
           all(z.p != slot.replica_id for z in y) and x[0] == Stage.PreAccepted
    ]

    if len(identic_groups):
        reply = identic_groups[0][0].r
        new_inst = yield Store(
            slot,
            InstanceStoreState(
                ballot,
                State(
                    Stage.PreAccepted,
                    reply.command,
                    reply.seq,
                    reply.deps
                )
            )
        )
        yield from leader_accept(q, slot, new_inst)
    elif max_state == Stage.PreAccepted:
        reply = [x for x in replies if x.r.state == max_state][0].r

        new_inst = yield Store(
            slot,
            InstanceStoreState(
                ballot,
                State(
                    Stage.PreAccepted,
                    reply.command,
                    reply.seq,
                    reply.deps
                )
            )
        )

        yield from leader_pre_accept(q, slot, new_inst, False)
    else:
        new_inst = yield Store(
            slot,
            InstanceStoreState(
                ballot,
                State(
                    Stage.PreAccepted,
                    None,
                    0,
                    []
                )
            )
        )
        yield from leader_pre_accept(q, slot, new_inst, False)


def leader_pre_accept(q: Quorum, slot: Slot, inst: InstanceStoreState, allow_fast: True):
    for peer in q.peers:
        yield Send(
            peer,
            packet.PreAcceptRequest(slot, inst.ballot, inst.state.command, inst.state.seq, inst.state.deps)
        )

    replies = []
    replies: List[packet.PreAcceptResponseAck]

    while len(replies) + 1 < q.slow_size:
        _, (ack, nack) = yield Receive.any(
            packet.PreAcceptResponseAck,
            packet.PreAcceptResponseNack,
        )

        ack: Optional[packet.PreAcceptResponseAck]
        nack: Optional[packet.PreAcceptResponseNack]

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
            all(x.deps == inst.state.deps for x in replies) and
            all(x.seq == inst.state.seq for x in replies)
    ):
        inst = yield Store(
            slot,
            InstanceStoreState(
                inst.ballot,
                State(
                    Stage.Committed,
                    inst.state.command,
                    inst.state.seq,
                    inst.state.deps
                )
            )
        )
        yield from leader_commit(q, slot, inst)
    else:
        seq = max(inst.state.seq, max(x.seq for x in replies))

        deps = []
        for rep in replies:
            deps.extend(rep.deps)

        deps = sorted(set(deps))

        inst = yield Store(
            slot,
            InstanceStoreState(
                inst.ballot,
                State(
                    Stage.Accepted,
                    inst.state.command,
                    seq,
                    deps
                )
            )
        )

        yield from leader_accept(q, slot, inst)


def leader_accept(q: Quorum, slot: Slot, inst: InstanceStoreState):
    for peer in q.peers:
        yield Send(peer, packet.AcceptRequest(slot, inst.ballot, inst.state.command, inst.state.seq, inst.state.deps))

    replies = []

    while len(replies) + 1 < q.slow_size:
        _, (ack, nack) = yield Receive.any(
            packet.AcceptResponseAck,
            packet.AcceptResponseNack,
        )

        ack: Optional[packet.AcceptResponseAck]
        nack: Optional[packet.AcceptResponseNack]

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

    inst = yield Store(
        slot,
        InstanceStoreState(
            inst.ballot,
            State(
                Stage.Committed,
                inst.state.command,
                inst.state.seq,
                inst.state.deps
            )
        )
    )

    yield from leader_commit(q, slot, inst)


def leader_commit(q: Quorum, slot: Slot, inst: InstanceStoreState):
    for peer in q.peers:
        yield Send(peer, packet.CommitRequest(slot, inst.ballot, inst.state.command, inst.state.seq, inst.state.deps))