from typing import Dict, Set, SupportsInt

import logging

from functools import reduce

import math

from dsm.epaxos.channel import Send, Receive, Event, Channel, Peer
from dsm.epaxos.packet import PreAcceptRequest, PreAcceptAckReply, AcceptRequest, CommitRequest, AcceptAckReply, \
    NackReply, \
    ClientRequest, ClientResponse, PrepareRequest
from dsm.epaxos.state import Command, Slot
from dsm.epaxos.instance import InstanceState, PreAcceptedState, PreAcceptedLeaderState, Instance, AcceptedState, \
    CommittedState, AcceptedLeaderState

logger = logging.getLogger(__name__)


class Replica(Peer):
    def __init__(
        self,
        channel: Channel,
        epoch: SupportsInt,
        replica_id: SupportsInt,
        quorum_set_fast: Set[SupportsInt],
        quorum_set_full: Set[SupportsInt]
    ):
        self.channel = channel
        self.epoch = epoch
        self.next_instance_id = 0

        self.replica_id = replica_id
        self.quorum_set_fast = quorum_set_fast
        self.quorum_set_full = quorum_set_full

        assert len(self.quorum_set_full | self.quorum_set_fast) == len(self.quorum_set_full)

        # Commands seen.
        self.instances = {}  # type: Dict[Slot, Instance]

    def find_interferences(self, cmd: Command):
        # TODO: do not allow the dependency list to grow linearly with history size.
        return {k for k, v in self.instances.items() if (cmd.id // 1000) == (v.command.id // 1000)}

    def _next_instance_slot(self):
        slot = Slot(self.replica_id, self.next_instance_id)
        self.next_instance_id += 1
        return slot

    @property
    def quorum(self):
        return int(math.floor(len(self.quorum_set_full) / 2))

    @property
    def neighbours_fast(self):
        return {x for x in self.quorum_set_fast if x != self.replica_id}

    def notify(self, event: Event):
        self.channel.notify(event)

    def receive(self, event: Receive):
        if isinstance(event.packet, ClientRequest):
            self.leader_request(event.peer, event.packet.command)
        elif isinstance(event.packet, PreAcceptRequest):
            self.replica_preaccept(event.peer, event.packet)
        elif isinstance(event.packet, PreAcceptAckReply):
            self.leader_preaccept_ack(event.peer, event.packet)
        elif isinstance(event.packet, AcceptRequest):
            self.replica_accept(event.peer, event.packet)
        elif isinstance(event.packet, AcceptAckReply):
            self.leader_accept_ack(event.packet)
        elif isinstance(event.packet, CommitRequest):
            self.replica_commit(event.peer, event.packet)
        elif isinstance(event.packet, NackReply):
            msg = event.packet
            if msg.slot in self.instances:
                inst = self.instances[msg.slot]
                if isinstance(inst.state, PreAcceptedLeaderState):
                    self.do_explicit_prepare(msg.slot)
                elif isinstance(inst.state, AcceptedLeaderState):
                    self.do_explicit_prepare(msg.slot)
                else:
                    logger.warning(
                        f'Replica {self.leader_id}, slot {msg.slot} received NACK for unreasonable instance {inst}')
            else:
                logger.warning(f'Replica {self.leader_id}, slot {msg.slot} is not known')
        else:
            logger.warning(f'Replica {self.leader_id}, unknown packet {event.packet}')

    def do_accept(self, slot, ballot, command, seq, deps):
        self.instances[slot] = Instance(
            ballot, command, seq, deps, AcceptedLeaderState(self.instances[slot].state.client_id))

        for x in self.neighbours_fast:
            self.notify(Send(x, AcceptRequest(slot, ballot, command, seq, deps)))

    def do_commit(self, slot, ballot, command, seq, deps):
        inst = self.instances[slot]

        self.notify(Send(inst.state.client_id, ClientResponse(inst.command)))
        self.instances[slot] = Instance(ballot, command, seq, deps, CommittedState())

        for x in self.neighbours_fast:
            self.notify(Send(x, CommitRequest(slot, ballot, command, seq, deps)))

    def do_explicit_prepare(self, slot):
        raise NotImplementedError('Error')

    def leader_request(self, sender, cmd: Command):
        # TODO: leader MAY wait for commands by itself (so it makes sense to subscribe for their reception).

        deps = {x for x in self.find_interferences(cmd)}
        seq = max({0} | {self.instances[x].seq for x in deps}) + 1

        slot = self._next_instance_slot()

        self.instances[slot] = Instance(slot.ballot(self.epoch), cmd, seq, deps, PreAcceptedLeaderState(sender))

        for x in self.neighbours_fast:
            self.notify(Send(x, PreAcceptRequest(slot, self.instances[slot].ballot, cmd, seq, deps)))

    def replica_preaccept(self, sender, msg: PreAcceptRequest):
        # TODO: replica may receive commands without any notice (and some of them in odd order).

        if msg.slot in self.instances and msg.ballot < self.instances[msg.slot].ballot:
            self.notify(Send(sender, NackReply(msg.slot)))
            return

        deps_local = {x for x in self.find_interferences(msg.command)}

        seq_local = max({0} | {self.instances[x].seq for x in deps_local}) + 1
        seq_local = max({msg.seq, seq_local})

        deps_local = deps_local | msg.deps

        self.instances[msg.slot] = Instance(msg.ballot, msg.command, seq_local, deps_local, PreAcceptedState())

        self.notify(Send(sender, PreAcceptAckReply(msg.slot, msg.ballot, msg.command, seq_local, deps_local)))

    def leader_preaccept_ack(self, sender, msg: PreAcceptAckReply):
        if msg.slot not in self.instances:
            logger.warning(
                f'Replica {self.leader_id} received command as leader in slot {msg.slot} that it does not know of')
            return

        if msg.slot in self.instances and msg.ballot < self.instances[msg.slot].ballot:
            self.do_explicit_prepare(msg.slot)
            return

        inst = self.instances[msg.slot]

        if isinstance(inst.state, PreAcceptedLeaderState):
            state = inst.state  # type: PreAcceptedLeaderState

            state.replies.append(msg)

            if len(state.replies) >= self.quorum:
                replies = state.replies

                deps_ok = reduce(lambda a, b: a == b, (x.deps for x in replies), inst.deps)
                seq_ok = reduce(lambda a, b: a == b, (x.seq for x in replies), inst.seq)

                if deps_ok and seq_ok:
                    self.do_commit(msg.slot, msg.ballot, msg.command, msg.seq, msg.deps)
                else:
                    deps = reduce(lambda a, b: a | b, (x.deps for x in state.replies), set())
                    seq = max({x.seq for x in state.replies})

                    self.do_accept(msg.slot, msg.ballot, msg.command, seq, deps)
        else:
            logger.warning(f'Replica {self.leader_id} {msg.slot} invalid state {inst.state} for this transaction')

    def replica_accept(self, sender, msg: AcceptRequest):
        if msg.slot in self.instances and msg.ballot < self.instances[msg.slot].ballot:
            self.notify(Send(sender, NackReply(msg.slot)))
            return

        self.instances[msg.slot] = Instance(msg.ballot, msg.command, msg.seq, msg.deps, AcceptedState())

        self.notify(Send(sender, AcceptAckReply(msg.slot, msg.ballot, msg.command, msg.seq, msg.deps)))

    def leader_accept_ack(self, msg: AcceptAckReply):
        if msg.slot not in self.instances:
            logger.warning(
                f'Replica {self.leader_id} received command as leader in slot {msg.slot} that it does not know of')
            return

        if msg.slot in self.instances and msg.ballot < self.instances[msg.slot].ballot:
            self.do_explicit_prepare(msg.slot)
            return

        inst = self.instances[msg.slot]
        state = inst.state  # type: AcceptedLeaderState

        if isinstance(state, AcceptedLeaderState):

            state.replies.append(msg)

            if len(state.replies) >= self.quorum:
                self.do_commit(msg.slot, msg.ballot, msg.command, inst.seq, inst.deps)
        else:
            logger.warning(f'Replica {self.leader_id} {msg.slot} invalid state {inst.state} for this transaction')

    def replica_commit(self, sender, msg: CommitRequest):
        if msg.slot in self.instances and msg.ballot < self.instances[msg.slot].ballot:
            self.notify(Send(sender, NackReply(msg.slot)))
            return

        self.instances[msg.slot] = Instance(msg.ballot, msg.command, msg.seq, msg.deps, CommittedState())

    def leader_prepare(self, slot: Slot):
        inst = self.instances[slot]

        ballot = inst.ballot.next()

        for x in self.neighbours_fast:
            self.notify(Send(x, PrepareRequest(slot, ballot)))

    def replica_prepare(self, sender, msg: PrepareRequest):
        if msg.slot in self.instances and msg.ballot < self.instances[msg.slot].ballot:
            pass


