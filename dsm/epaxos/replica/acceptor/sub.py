from dsm.epaxos.inst.state import Slot, State, Stage
from dsm.epaxos.inst.store import InstanceStoreState, IncorrectStage, IncorrectBallot
from dsm.epaxos.net import packet
from dsm.epaxos.replica.leader.ev import LeaderStop
from dsm.epaxos.replica.net.ev import Send, Receive
from dsm.epaxos.replica.quorum.ev import Quorum
from dsm.epaxos.replica.state.ev import Load, Store


def acceptor_single_ep(q: Quorum, slot: Slot):
    while True:

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


def acceptor_pre_accept(q: Quorum, slot: Slot, peer: int, pre_accept: packet.PreAcceptRequest):
    try:
        inst = yield Store(
            slot,
            InstanceStoreState(
                pre_accept.ballot,
                State(
                    Stage.PreAccepted,
                    pre_accept.command,
                    pre_accept.seq,
                    pre_accept.deps
                )
            )
        )  # type: InstanceStoreState

        deps_comm_mask = []

        for dep_slot in inst.state.deps:
            xx = yield Load(dep_slot)  # type: InstanceStoreState

            deps_comm_mask.append(xx.state.stage >= Stage.Committed)

        yield LeaderStop(slot, 'acceptor')
        yield Send(
            peer,
            packet.PreAcceptResponseAck(
                slot,
                inst.ballot,
                inst.state.seq,
                inst.state.deps,
                deps_comm_mask
            )
        )

    except IncorrectStage:
        return
    except IncorrectBallot as e:
        yield Send(peer, packet.PreAcceptResponseNack(slot, e.inst.ballot, 'BALLOT'))
        return


def acceptor_accept(q: Quorum, slot: Slot, peer: int, accept: packet.AcceptRequest):
    try:
        inst = yield Store(
            slot,
            InstanceStoreState(
                accept.ballot,
                State(
                    Stage.Accepted,
                    accept.command,
                    accept.seq,
                    accept.deps
                )
            )
        )  # type: InstanceStoreState

        yield LeaderStop(slot, 'acceptor')
        yield Send(
            peer,
            packet.AcceptResponseAck(
                slot,
                inst.ballot
            )
        )

    except IncorrectStage:
        return
    except IncorrectBallot as e:
        yield Send(peer, packet.AcceptResponseNack(slot, e.inst.ballot))
        return


def acceptor_commit(q: Quorum, slot: Slot, peer: int, commit: packet.CommitRequest):
    try:
        inst = yield Store(
            slot,
            InstanceStoreState(
                commit.ballot,
                State(
                    Stage.Accepted,
                    commit.command,
                    commit.seq,
                    commit.deps
                )
            )
        )  # type: InstanceStoreState

        yield LeaderStop(slot, 'acceptor')
        yield Send(
            peer,
            packet.AcceptResponseAck(
                slot,
                inst.ballot
            )
        )
    except IncorrectBallot as e:
        return
    except IncorrectStage as e:
        return


def acceptor_prepare(q: Quorum, slot: Slot, peer: int, prepare: packet.PrepareRequest):
    inst = yield Load(slot)  # type: InstanceStoreState

    if prepare.ballot < inst.ballot:
        yield Send(
            peer,
            packet.PrepareResponseNack(
                slot,
                inst.ballot
            )
        )
        return

    yield Send(
        peer,
        packet.PrepareResponseAck(
            slot,
            prepare.ballot,
            inst.state.command,
            inst.state.seq,
            inst.state.deps,
            inst.state.stage
        )
    )