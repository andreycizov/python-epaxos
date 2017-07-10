from typing import Dict, List

from dsm.epaxos.command.state import AbstractCommand, Noop
from dsm.epaxos.instance.state import Slot, Instance, Ballot, StateType, PreparedState, PostPreparedState, \
    PreAcceptedState, AcceptedState, CommittedState
from dsm.epaxos.replica.state import ReplicaState


class InstanceStore:
    def __init__(self, state: ReplicaState):
        self.state = state

        self.instances_deps = {}
        self.instances = {}  # type: Dict[Slot, PreparedState]
        self.to_execute = []  # type: List[Slot]

    def __contains__(self, item: Slot):
        return item in self.instances

    def __getitem__(self, slot: Slot) -> PreparedState:
        if slot not in self.instances[slot]:
            self.instances[slot] = PreparedState(slot, slot.ballot_initial(self.state.epoch))
        return self.instances[slot]

    # def create(self, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int, deps: List[Slot]):
    #     self.instances[slot] = Instance(ballot, command, seq, deps, StateType.PreAccepted)
    #
    # def update_deps(self, slot: Slot, add_seq: int = 0, add_deps: List[Slot] = []):
    #     # TODO: what if one of our deps does not exist in our history ?
    #
    #     deps = self.dependencies(slot)
    #     deps = sorted(set(add_deps + deps))
    #
    #     seq = max((self[x].seq for x in deps), default=0) + 1
    #     seq = max([seq, add_seq])
    #
    #     self[slot].set_deps(seq, deps)

    def dependencies(self, slot: Slot, command: AbstractCommand) -> List[Slot]:
        """
        Currently we assume that the dependencies between commands may only be transitional
        """
        if command == Noop:
            return []

        return sorted(
            inst_slot
            for inst_slot, v in self.instances.items()
            if isinstance(v, PostPreparedState) and (
                (command.ident // 1000) == (v.command.ident // 1000) and inst_slot != slot)
        )

    # def prepare(self, slot: Slot):
    #     # TODO: implicit for: new_deps; client_request
    #
    #     # todo: set self[slot] = ((epoch, 0, slot.leader_id), NO_COMMAND,
    #     pass

    def ballot_next(self, slot: Slot):
        ballot = self[slot].ballot
        self[slot].ballot = Ballot(self.state.epoch, ballot.b + 1, self.state.replica_id)

    def prepare(self, slot: Slot):
        assert slot not in self
        return self[slot]

    def pre_accept(self, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int = 0, deps: List[Slot] = list()):
        assert StateType.PreAccepted >= self[slot].type

        slot_deps = self.dependencies(slot, command)
        slot_deps = sorted(set(slot_deps + deps))

        slot_seq = max((self[x].seq for x in deps), default=0) + 1
        slot_seq = max([seq, slot_seq])

        slot_inst = PreAcceptedState(slot, ballot, command, slot_seq, slot_deps)

        self.instances[slot] = slot_inst

        return slot_inst

    def _post_pre_accept(self, cls, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int, deps: List[Slot]):
        inst = self[slot]

        new_inst = cls(slot, ballot, command, seq, deps)

        assert new_inst.type >= inst.type and new_inst.ballot >= inst.ballot

        self.instances[slot] = new_inst

        return new_inst

    def accept(self, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int, deps: List[Slot]) -> AcceptedState:
        return self._post_pre_accept(AcceptedState, slot, ballot, command, seq, deps)

    def commit(self, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int,
               deps: List[Slot]) -> CommittedState:
        return self._post_pre_accept(CommittedState, slot, ballot, command, seq, deps)
