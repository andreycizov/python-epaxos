from typing import Dict

from enum import Enum

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.instance.state import Instance, Slot, State
from dsm.epaxos.instance.store import InstanceStore
from dsm.epaxos.message.pre_accept import PreAcceptRequest, PreAcceptResponse, PreAcceptNackResponse
from dsm.epaxos.message.state import Vote
from dsm.epaxos.network.peer import Peer
from dsm.epaxos.replica.abstract import Behaviour
from dsm.epaxos.replica.state import ReplicaState


class LeaderInstanceState(Enum):
    PreAccept = 1


class LeaderInstance:
    def __init__(self, state: LeaderInstancePayload, client_peer: Peer, allow_fast=True):
        self.state = state
        self.client_peer = client_peer
        self.allow_fast = allow_fast


class LeaderInstancePayload:
    pass


class PreAcceptLeaderInstance(LeaderInstancePayload):
    name = LeaderInstanceState.PreAccept

    def __init__(self):
        self.replies = []  # type: None


class Leader(Behaviour):
    def __init__(
        self,
        state: ReplicaState,
        store: InstanceStore,
    ):
        super().__init__(state, store)
        self.instances = {}  # type: Dict[Slot, LeaderInstance]
        self.next_instance_id = 0

    def __contains__(self, item):
        return item in self.instances

    def __getitem__(self, item: Slot):
        return self.instances[item]

    def __setitem__(self, key: Slot, value: LeaderInstance):
        self.instances[key] = value

    @property
    def peers_fast(self):
        return {x for x in self.state.quorum_fast if x != self.state.ident}

    @property
    def quorum_n(self):
        return int(len(self.peers_fast) / 2) + 1

    def create_request(self, client_peer: Peer, command: AbstractCommand):
        slot = Slot(self.state.ident, self.next_instance_id)
        self.next_instance_id += 1
        self.request(client_peer, slot, command)

    def request(self, client_peer: Peer, slot: Slot, command: AbstractCommand):
        deps = self.store.interferences(slot, command)
        seq = max({0} | {self.store[x].seq for x in deps}) + 1

        # TODO: we need to provide checks here (?)
        inst = Instance(slot.ballot(self.state.epoch), command, seq, deps, State.PreAccepted)

        self.store[slot] = inst
        self[slot] = LeaderInstance(PreAcceptLeaderInstance(), client_peer)

        for peer in self.peers_fast:
            # Timeouts on PreAcceptRequest are guaranteed to be handled by the Replica part of the algorithm
            peer.send(PreAcceptRequest(Vote(slot, inst.ballot), inst.command, inst.seq, inst.deps))

    def pre_accept_reply(self, peer: Peer, slot: Slot, reply: PreAcceptResponse):
        if self[slot] != LeaderInstanceState.PreAccept:
            return

        if isinstance(reply, PreAcceptNackResponse):
            pass

        # TODO: If any of the replies have got a ballot number larger than the one we have - we should immediately go to ExplicitPrepare

        l_inst = self[slot].state  # type: PreAcceptLeaderInstance
        l_inst.replies.append(reply)

        if len(l_inst.replies) + 1 >= self.quorum_n:
            pass
