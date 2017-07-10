from typing import Dict, List

from dsm.epaxos.command.state import AbstractCommand, Noop
from dsm.epaxos.instance.state import Slot, Ballot, StateType, PreparedState, PostPreparedState, \
    PreAcceptedState, AcceptedState, CommittedState, InstanceState
from dsm.epaxos.replica.state import ReplicaState


class InstanceStore:
    def __init__(self, state: ReplicaState):
        self.state = state

        self.instances_deps = {}
        self.instances = {}  # type: Dict[Slot, InstanceState]
        self.to_execute = []  # type: List[Slot]

    def __contains__(self, item: Slot):
        return item in self.instances

    def __setitem__(self, slot: Slot, new_inst: InstanceState):
        inst = self[slot]

        assert new_inst.type >= inst.type and new_inst.ballot >= inst.ballot

        self.instances[slot] = new_inst
        # TODO: check if command is committed
        # TODO: check if state < committed and add it to timeout queue.

    def __getitem__(self, slot: Slot) -> InstanceState:
        if slot not in self:
            self[slot] = PreparedState(slot, slot.ballot_initial(self.state.epoch))
        return self[slot]

    def dependencies(self, slot: Slot, command: AbstractCommand) -> List[Slot]:
        """
        Currently we assume that the dependencies between commands may only be transitional
        """
        if command == Noop:
            return []

        def dependency_filter(v):
            if isinstance(v, PostPreparedState):
                return (command.ident // 1000) == (v.command.ident // 1000) and v.slot != slot
            else:
                return False

        return sorted(
            inst_slot
            for inst_slot, v in self.instances.items()
            if dependency_filter(v)
        )

    def ballot_next(self, slot: Slot):
        ballot = self[slot].ballot
        self[slot].ballot = Ballot(self.state.epoch, ballot.b + 1, self.state.replica_id)

    def prepare(self, slot: Slot):
        assert slot not in self
        return self[slot]

    def pre_accept(self, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int = 0, deps: List[Slot] = list()):
        assert StateType.PreAccepted >= self[slot].type

        local_deps = self.dependencies(slot, command)

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
