from itertools import groupby
from typing import Dict, List, Optional, Set, Tuple, Deque

from copy import copy

from collections import defaultdict, deque

from functools import reduce

import logging
from tarjan import tarjan

from dsm.epaxos.command.deps.store import AbstractDepsStore
from dsm.epaxos.instance.state import Slot, Ballot, StateType, PreparedState, PostPreparedState, \
    PreAcceptedState, AcceptedState, CommittedState, InstanceState
from dsm.epaxos.network import packet
from dsm.epaxos.replica.state import ReplicaState
from dsm.epaxos.timeout.store import TimeoutStore
from dsm.epaxos.command.state import Checkpoint

logger = logging.getLogger(__name__)


def last(iter_obj):
    r = None
    for x in iter_obj:
        r = x
    if r is None:
        raise ValueError()
    return r


class InstanceStore:
    def __init__(self, state: ReplicaState, deps_store: AbstractDepsStore, timeout_store: TimeoutStore):
        self.state = state
        self.deps_store = deps_store
        self.timeout_store = timeout_store

        self.instances = {}  # type: Dict[Slot, InstanceState]

        self.slot_cut = {}  # type: Dict[int, Slot]
        self.last_cp = None  # type: Optional[Slot]

        self.executed_cut = {}  # type: Dict[int, Slot]

        self.executed = {}  # type: Dict[Slot, bool]

        self.execute_pending = deque()  # type: Deque[Slot]
        self.execute_log = deque()  # type: Deque[List[Slot]]

        self.commit_expected = defaultdict(set)  # type: Dict[Slot, Set[Slot]]

        self.committed = dict()  # type: Dict[bool]

        self.client_rep = dict()

        self.command_to_slot = dict()

        self.file_log = open(f'./state_log-{self.state.replica_id}.txt', 'w+')

    def __contains__(self, item: Slot):
        return item in self.instances

    def __setitem__(self, slot: Slot, new_inst: InstanceState):
        inst = self[slot]

        # logger.info(repr((new_inst, inst)))
        # assert new_inst.ballot >= inst.ballot, repr((new_inst, inst))
        # assert ((inst.type >= StateType.Accepted and new_inst.type > StateType.Accepted) or (inst.type <= StateType.Accepted and new_inst.type <= StateType.Accepted)
        #         or (inst.type == StateType.PreAccepted and new_inst.type == StateType.Committed)
        #         ), repr((inst, new_inst))

        assert inst.ballot <= new_inst.ballot, (inst.ballot, new_inst.ballot)

        if inst.type == StateType.Committed and new_inst.type < StateType.Committed:
            assert False, repr((inst, new_inst))

        assert not self.is_cut(slot), repr((slot, inst, new_inst))

        if new_inst.ballot > Ballot(0, 10, 0):
            logger.debug(f'{self.state.replica_id} HW {new_inst}')

        self.deps_store.update(slot, inst, new_inst)

        self.file_log.write(
            f'1\t{inst.slot}\t{inst.type.name}\t{new_inst.ballot}\t{new_inst.type.name}\t{new_inst.command if isinstance(new_inst, PostPreparedState) else ""}\n')
        self.file_log.flush()

        if isinstance(new_inst, PostPreparedState):
            if new_inst.command:
                self.command_to_slot[new_inst.command.id] = slot

            for x in new_inst.deps:
                if self[x].type == StateType.Prepared:
                    self.timeout_store.update(slot, None, self[x])

        if inst.type != new_inst.type:
            new_inst = new_inst  # type: PostPreparedState
            if new_inst.type == StateType.Committed:
                try:

                    self.set_committed(slot)
                except AssertionError:
                    print(self.committed)
                    logger.exception(f'{inst} {new_inst}')
                    raise
                self.send_reply(slot)

        self.instances[slot] = new_inst

        # todo: so every new update of the instance sets a new timeout
        self.timeout_store.update(slot, inst, new_inst)

    def set_committed(self, slot: Slot):
        assert not self.is_committed(slot), self._repr(slot)
        self.file_log.write(f'2\t{slot}\n')
        self.file_log.flush()
        self.committed[slot] = True
        self.check_waited_for(slot)
        self.append_to_execute(slot)

    def append_to_execute(self, slot):
        # assert slot not in self.execute_pending
        self.execute_pending.append(slot)

    def __getitem__(self, slot: Slot) -> InstanceState:
        if slot not in self:
            self.instances[slot] = PreparedState(slot, slot.ballot_initial(self.state.epoch))
        return self.instances[slot]

    def next_ballot(self, slot: Slot):
        inst = self[slot]

        ballot = Ballot(self.state.epoch, inst.ballot.b + 1, self.state.replica_id)

        return ballot

    def increase_ballot(self, slot: Slot, ballot: Optional[Ballot] = None):
        inst = copy(self[slot])

        if ballot is None:
            ballot = self.next_ballot(slot)

        assert inst.ballot < ballot

        inst.ballot = ballot

        self[slot] = inst

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

    def check_waited_for(self, slot: Slot):
        if slot not in self.commit_expected:
            return

        expected = list(self.commit_expected.pop(slot))

        for x in expected:
            # assert self.is_committed(x), ''
            # assert not self.is_executed(x), ''
            # assert not x in self.execute_pending, self._repr(slot) + ' ' + repr((x, self.execute_pending))
            self.append_to_execute(x)

    def is_cut(self, slot: Slot):
        cut = self.slot_cut.get(slot.replica_id)
        return cut is not None and cut >= slot

    def is_executed(self, slot: Slot):
        return self.is_cut(slot) or self.executed.get(slot, False)

    def is_committed(self, slot: Slot):
        return self.is_cut(slot) or self.committed.get(slot, False)

    def set_executed(self, slot: Slot):
        assert self.executed.get(slot, False) is False, self[slot]

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

    def _repr(self, slot: Slot):
        return repr(
            (self[slot], self.committed.get(slot), self.executed.get(slot), self.executed_cut.get(slot.replica_id)))

    def execute(self, slot: Slot):
        if self.is_executed(slot):
            return

        # assert not slot in self.execute_pending, self._repr(slot)
        assert not self.is_executed(slot), self._repr(slot)
        assert self.is_committed(slot), self._repr(slot)

        slots_to_wait = [dep_slot for dep_slot in self[slot].deps if not self.is_committed(dep_slot)]

        if self.should_wait(slot, slots_to_wait):
            return

        graph = self._build_deps_graph(slot)

        slots_to_wait = [x for x in graph.keys() if not self.is_committed(x)]

        if self.should_wait(slot, slots_to_wait):
            return

        order = self._build_exec_order(graph)

        self.execute_log.append(order)

        for x in order:
            self.set_executed(x)

        return order

    def send_reply(self, slot):
        if slot in self.client_rep:
            self.state.channel.send(self.client_rep[slot], packet.ClientResponse(self[slot].command))
            del self.client_rep[slot]

    def _build_slot_cut(self, prev_act):
        cut = {k: last(v) for k, v in groupby(sorted(prev_act.deps), key=lambda x: x.replica_id)}
        for k, v in cut.items():
            zz = [isinstance(x, CommittedState) for k2, x in self.instances.items() if
                  k2 < v and k2.replica_id == v.replica_id]
            try:
                assert all(zz)
            except:
                print(k, prev_act, [x for k2, x in self.instances.items() if
                                    k2 < v and not isinstance(x, CommittedState)])

        return cut

    def set_slot_cut(self, cut):
        self.slot_cut = {**self.slot_cut, **cut}

        for k in [k for k in self.instances.keys() if k < self.slot_cut.get(k.replica_id, Slot(k.replica_id, -1))]:
            del self.instances[k]

        for k in [k for k in self.replica.leader.leading.keys() if k < self.slot_cut.get(k.replica_id, Slot(k.replica_id, -1))]:
            del self.replica.leader.leading[k]

        for k in [k for k in self.replica.leader.waiting_for.keys() if k < self.slot_cut.get(k.replica_id, Slot(k.replica_id, -1))]:
            del self.replica.leader.waiting_for[k]

        for k in [k for k in self.replica.acceptor.instances.keys() if k < self.slot_cut.get(k.replica_id, Slot(k.replica_id, -1))]:
            del self.replica.acceptor.instances[k]


        for k in [k for k in self.replica.acceptor.waiting_for.keys() if k < self.slot_cut.get(k.replica_id, Slot(k.replica_id, -1))]:
            del self.replica.acceptor.waiting_for[k]

        # for k in [k for k in self.instances.keys() if k < self.slot_cut.get(k, Slot(k, -1))]:
        #     del self.instances[k]


        print('CP at', self.slot_cut)
        return

    def execute_all_pending(self):
        while len(self.execute_pending):
            # todo: the command is executed,
            slots = self.execute(self.execute_pending.popleft())  # type: List[Slot]

            if slots:
                for slot in slots:
                    act = self[slot]  # type: PostPreparedState
                    prev_act = self.instances.get(self.last_cp)  # type: PostPreparedState

                    if act.command:
                        if isinstance(act.command.payload, Checkpoint) and prev_act:
                            self.set_slot_cut(self._build_slot_cut(prev_act))

                        if isinstance(act.command.payload, Checkpoint):
                            self.last_cp = slot



            continue
            if slots:
                print('EXEC START')
                for slot in slots:
                    print(self[slot].command)
                print('EXEC END')

# todo: how do we solve issues where a new actor is required ?
