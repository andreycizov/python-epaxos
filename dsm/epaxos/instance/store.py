from typing import Dict, List

from copy import copy

from dsm.epaxos.command.deps.store import AbstractDepsStore
from dsm.epaxos.command.state import AbstractCommand, Noop
from dsm.epaxos.instance.state import Slot, Ballot, StateType, PreparedState, PostPreparedState, \
    PreAcceptedState, AcceptedState, CommittedState, InstanceState
from dsm.epaxos.replica.state import ReplicaState
from dsm.epaxos.timeout.store import TimeoutStore


class InstanceStore:
    def __init__(self, state: ReplicaState, deps_store: AbstractDepsStore):
        self.state = state
        self.deps_store = deps_store
        self.timeout_store = TimeoutStore(state, self)

        self.instances = {}  # type: Dict[Slot, InstanceState]

        # TODO: list all of the instances that are pending execution.

    def __contains__(self, item: Slot):
        return item in self.instances

    def __setitem__(self, slot: Slot, new_inst: InstanceState):
        inst = self[slot]

        assert new_inst.type >= inst.type and new_inst.ballot >= inst.ballot

        self.instances[slot] = new_inst

        self.deps_store.update(slot, inst, new_inst)
        self.timeout_store.update(slot, inst, new_inst)

    def __getitem__(self, slot: Slot) -> InstanceState:
        if slot not in self:
            self[slot] = PreparedState(slot, slot.ballot_initial(self.state.epoch))
        return self[slot]

    def increase_ballot(self, slot: Slot):
        inst = copy(self[slot])
        inst.ballot = Ballot(self.state.epoch, inst.ballot.b + 1, self.state.replica_id)

        self[slot] = inst

    def prepare(self, slot: Slot):
        assert slot not in self
        return self[slot]

    def pre_accept(self, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int = 0, deps: List[Slot] = list()):
        assert StateType.PreAccepted >= self[slot].type

        local_deps = self.deps_store.query(slot, command)

        slot_seq = max((self[x].seq for x in local_deps), default=0) + 1
        slot_seq = max([seq, slot_seq])

        remote_deps = sorted(set(local_deps + deps))

        slot_inst = PreAcceptedState(slot, ballot, command, slot_seq, remote_deps)

        self[slot] = slot_inst

        return slot_inst

    def _post_pre_accept(self, cls, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int, deps: List[Slot]):
        new_inst = cls(slot, ballot, command, seq, deps)

        self[slot] = new_inst

        return new_inst

    def accept(self, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int, deps: List[Slot]) -> AcceptedState:
        return self._post_pre_accept(AcceptedState, slot, ballot, command, seq, deps)

    def commit(self, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int,
               deps: List[Slot]) -> CommittedState:
        return self._post_pre_accept(CommittedState, slot, ballot, command, seq, deps)
