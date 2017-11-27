from typing import Dict, List, Optional, Set, Tuple, Deque

from copy import copy

from collections import defaultdict, deque

from functools import reduce

import logging
from tarjan import tarjan

from dsm.epaxos.command.deps.store import AbstractDepsStore
from dsm.epaxos.command.state import AbstractCommand, Noop
from dsm.epaxos.instance.state import Slot, Ballot, StateType, PreparedState, PostPreparedState, \
    PreAcceptedState, AcceptedState, CommittedState, InstanceState
from dsm.epaxos.replica.state import ReplicaState
from dsm.epaxos.timeout.store import TimeoutStore

logger = logging.getLogger(__name__)


class InstanceStore:
    def __init__(self, state: ReplicaState, deps_store: AbstractDepsStore, timeout_store: TimeoutStore):
        self.state = state
        self.deps_store = deps_store
        self.timeout_store = timeout_store

        self.instances = {}  # type: Dict[Slot, InstanceState]

        self.slot_cut = {}  # type: Dict[int, Slot]
        self.last_cp = None # type: Optional[Slot]

        self.executed_cut = {}  # type: Dict[int, Slot]

        self.executed = {}  # type: Dict[Slot, bool]

        self.execute_pending = deque()  # type: Deque[Slot]
        self.execute_log = deque()  # type: Deque[List[Slot]]

        self.commit_expected = defaultdict(set)  # type: Dict[Slot, Set[Slot]]

        self.client_rep = dict()

    def __contains__(self, item: Slot):
        return item in self.instances

    def __setitem__(self, slot: Slot, new_inst: InstanceState):
        inst = self[slot]

        assert new_inst.type >= inst.type and new_inst.ballot >= inst.ballot, repr((new_inst, inst))

        self.instances[slot] = new_inst

        self.deps_store.update(slot, inst, new_inst)

        # todo: so every new update of the instance sets a new timeout
        self.timeout_store.update(slot, inst, new_inst)

    def __getitem__(self, slot: Slot) -> InstanceState:
        if slot not in self:
            self.instances[slot] = PreparedState(slot, slot.ballot_initial(self.state.epoch))
        return self.instances[slot]

    def increase_ballot(self, slot: Slot, ballot: Optional[Ballot] = None):
        inst = copy(self[slot])

        if ballot is None:
            ballot = Ballot(self.state.epoch, inst.ballot.b + 1, self.state.replica_id)

        assert inst.ballot < ballot

        inst.ballot = ballot

        self[slot] = inst

    def prepare(self, slot: Slot):
        assert slot not in self
        return self[slot]

    def pre_accept(self, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int = 0, deps: List[Slot] = list()):
        assert StateType.PreAccepted >= self[slot].type and ballot >= self[slot].ballot, (
            (slot, ballot, command, seq, deps), self[slot])

        local_deps = self.deps_store.query(slot, command)

        slot_seq = max((self[x].seq for x in local_deps), default=0) + 1
        slot_seq = max([seq, slot_seq])

        remote_deps = sorted(set(local_deps + deps))

        slot_inst = PreAcceptedState(slot, ballot, command, slot_seq, remote_deps)

        self[slot] = slot_inst

        return slot_inst

    def _post_pre_accept(self, cls, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int, deps: List[Slot]):
        assert cls.type >= self[slot].type and ballot >= self[slot].ballot, (
            (slot, ballot, command, seq, deps), self[slot])

        new_inst = cls(slot, ballot, command, seq, deps)

        self[slot] = new_inst

        return new_inst

    def accept(self, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int, deps: List[Slot]) -> AcceptedState:
        return self._post_pre_accept(AcceptedState, slot, ballot, command, seq, deps)

    def _build_deps_graph(self, slot: Slot) -> Dict[Slot, Optional[List[Slot]]]:
        dep_graph = {}  # type: Dict[Slot, List[Slot]]

        pending_slots = deque()  # type: Deque[Slot]
        pending_slots.append(slot)

        while len(pending_slots):
            current_slot = pending_slots.popleft()

            dep_graph[current_slot] = []
            inst = self[current_slot]

            if isinstance(inst, PostPreparedState):
                for dep_slot in inst.deps:
                    if dep_slot not in dep_graph and dep_slot not in pending_slots and not self.is_executed(dep_slot):
                        pending_slots.append(dep_slot)
                    if not self.is_executed(dep_slot):
                        dep_graph[current_slot].append(dep_slot)

        return dep_graph

    def _build_exec_order(self, graph: Dict[Slot, List[Slot]]):
        exec_order = tarjan(graph)

        return reduce(lambda a, b: a + b, [sorted(x, key=lambda x: self[x].seq) for x in exec_order], [])

    def commit(self, slot: Slot, ballot: Ballot, command: AbstractCommand, seq: int,
               deps: List[Slot]) -> CommittedState:
        if self[slot].type < StateType.Committed:
            self.check_waited_for(slot)
            self.execute_pending.append(slot)

        return self._post_pre_accept(CommittedState, slot, ballot, command, seq, deps)

    def check_waited_for(self, slot: Slot):
        if slot in self.commit_expected:
            self.execute_pending.extend(self.commit_expected[slot])
            del self.commit_expected[slot]

    def is_executed(self, slot: Slot):
        if slot not in self.executed:
            return False
        else:
            return self.executed[slot]

    def set_executed(self, slot: Slot):
        self.executed[slot] = True

        self.update_executed_cut(slot.replica_id)

    def update_executed_cut(self, replica_id):
        # Whack-a-mole for executed flags.
        current_slot = self.executed_cut.get(replica_id, Slot(replica_id, -1))
        current_slot = Slot(replica_id, current_slot.instance_id + 1)

        while self.is_executed(current_slot):
            self.executed_cut[replica_id] = current_slot

            current_slot = self.executed_cut[replica_id]
            current_slot = Slot(replica_id, current_slot.instance_id + 1)

    def should_wait(self, slot: Slot, slots_to_wait: List[Slot]):
        if len(slots_to_wait):
            for slot_to_wait in slots_to_wait:
                self.commit_expected[slot_to_wait].update({slot})
            return True
        else:
            return False

    def execute(self, slot: Slot):
        if self.is_executed(slot):
            return

        assert self[slot].type == StateType.Committed

        slots_to_wait = [dep_slot for dep_slot in self[slot].deps if self[dep_slot].type < StateType.Committed]

        if self.should_wait(slot, slots_to_wait):
            return

        graph = self._build_deps_graph(slot)

        slots_to_wait = [x for x in graph.keys() if self[x].type < StateType.Committed]

        if self.should_wait(slot, slots_to_wait):
            return

        order = self._build_exec_order(graph)

        self.execute_log.append(order)

        for x in order:
            self.set_executed(x)

        return order

    def execute_all_pending(self):
        def last(iter_obj):
            r = None
            for x in iter_obj:
                r = x
            if r is None:
                raise ValueError()
            return r

        from itertools import groupby
        while len(self.execute_pending):
            cmds = self.execute(self.execute_pending.popleft())  # type: List[Slot]

            if cmds:
                for cmd in cmds:
                    act = self[cmd]  # type: PostPreparedState
                    prev_act = self.instances.get(self.last_cp)  # type: PostPreparedState

                    if act.command.ident == 0 and prev_act:

                        cut = {k: last(v) for k, v in groupby(act.deps, key=lambda x: x.replica_id)}
                        for k, v in cut.items():
                            zz = [isinstance(x, CommittedState) for k2, x in self.instances.items() if k2 < v and k2.replica_id == v.replica_id]
                            try:
                                assert all(zz)
                            except:
                                print(k, prev_act, [x for k2, x in self.instances.items() if k2 < v and not isinstance(x, CommittedState)])
                        self.slot_cut = {**self.slot_cut, **cut}

                        print('CP at', self.slot_cut)
                    if act.command.ident == 0:
                        self.last_cp = cmd

                    if cmd in self.client_rep:
                        self.state.channel.client_response(self.client_rep[cmd], act.command)
                        del self.client_rep[cmd]

            continue
            if cmds:
                print('EXEC START')
                for cmd in cmds:
                    print(self[cmd].command)
                print('EXEC END')

# todo: how do we solve issues where a new actor is required ?
