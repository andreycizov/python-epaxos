from collections import deque
from pprint import pprint
from typing import NamedTuple, Dict, Deque, List, Optional, Tuple

from tarjan import tarjan

from dsm.epaxos.cmd.state import Command, Checkpoint
from dsm.epaxos.inst.state import Slot, Stage
from dsm.epaxos.inst.store import InstanceStore, InstanceStoreState
from dsm.epaxos.replica.main.ev import Reply
from dsm.epaxos.replica.quorum.ev import Quorum
from dsm.epaxos.replica.state.ev import InstanceState


class DepthFirstHelper:
    """
    Accumulate dependencies via `commit`, until the graph is complete, then return them
    when all of them have been marked via `commit`.
    """

    def __init__(self):
        # which slots are waiting for a commit on a given slot
        self.expected_ready_from = {}  # type: Dict[Slot, Dict[Slot, bool]]
        # how many commits are expected by the slot
        self.expected_ready_count_by = {}  # type: Dict[Slot, int]

        # self.ready_direct = {}  # type: Dict[Slot, bool]
        self.ready_waiting = {}
        self.ready_done = {}

        self.deque = deque()  # type: Deque[Slot]

    def _pop(self) -> List[Slot]:
        r = list(self.deque)
        self.deque = deque()
        return r

    def _push(self, slot: Slot):
        self.ready_done[slot] = True
        self.deque.append(slot)

    def set_expected_ready_from(self, slot: Slot, by: Slot):
        

        if slot not in self.expected_ready_from:
            self.expected_ready_from[slot] = {}

        if by not in self.expected_ready_count_by:
            self.expected_ready_count_by[by] = 0

        if by not in self.expected_ready_from[slot]:
            self.expected_ready_count_by[by] += 1

        self.expected_ready_from[slot][by] = True

    def is_waiting_deps(self, slot: Slot):
        return self.expected_ready_count_by.get(slot, 0) > 0

    def set_ready(self, slot: Slot):
        # todo: we may try telling our dependencies first, if there is anything that will be changed if we set_ready ourselves.
        # todo: then we'll have something to go with.

        if not self.is_waiting_deps(slot):
            self._push(slot)
        else:
            return []

        if slot not in self.expected_ready_from:
            return []

        ks = self.expected_ready_from[slot].keys()
        del self.expected_ready_from[slot]

        others = []
        for k in ks:
            self.expected_ready_count_by[k] -= 1
            if self.expected_ready_count_by[k] == 0:
                del self.expected_ready_count_by[k]
                # self.set_ready(slot)
                others.append(k)

        return others

    def ready(self, slot: Slot, deps: List[Slot]):
        # self.ready_direct[slot] = True

        # todo: will it return the dependencies in topological order by itself (?)

        for d in deps:
            self.set_expected_ready_from(d, slot)

        q = deque()
        q.append(slot)

        while len(q):
            slot = q.popleft()
            q.extend(self.set_ready(slot))

        return self._pop()


class ExecutorActor:
    def __init__(self, quorum: Quorum, store: InstanceStore):
        self.quorum = quorum
        self.store = store

        self.executed_cut = {}  # type: Dict[int, Slot]
        self.executed = {}  # type: Dict[Slot, bool]

        self.execute_pending = deque()  # type: Deque[Slot]

        self._log = open(f'executor-{self.quorum.replica_id}.log', 'w+')

        self.dph = DepthFirstHelper()

        # self.commit_expected = defaultdict(set)  # type: Dict[Slot, Set[Slot]]

    def log(self, fn: lambda: None):
        self._log.write(fn())
        self._log.flush()

    def _build_deps_graph(self, slot: Slot) -> Tuple[Dict[Slot, InstanceStoreState], Dict[Slot, Optional[List[Slot]]]]:
        """
        This is required as execution graph may contain cycles.
        """
        dep_graph = {}  # type: Dict[Slot, List[Slot]]

        instances = {}  # type: Dict[Slot, InstanceStoreState]
        pending_slots = deque()  # type: Deque[Slot]
        pending_slots.append(slot)

        while len(pending_slots):
            current_slot = pending_slots.popleft()
            inst = self.store.load(current_slot).inst

            instances[current_slot] = inst

            dep_graph[current_slot] = []

            for dep_slot in inst.state.deps:
                # a) if a command is executed, it's deps are executed
                if instances.get(dep_slot, None) is None and not self.is_executed(dep_slot):
                    pending_slots.append(dep_slot)
                # b)
                if not self.is_executed(dep_slot):
                    dep_graph[current_slot].append(dep_slot)

        return instances, dep_graph

    def _build_exec_order(self, instances: Dict[Slot, InstanceStoreState], graph: Dict[Slot, List[Slot]]):
        exec_order = tarjan(graph)

        # pprint((self.quorum.replica_id, instances, graph))

        return [sorted(x, key=lambda x: instances[x].state.seq) for x in exec_order]

    def set_execute_pending(self, slot: Slot):
        self.execute_pending.append(slot)

    def is_cut(self, slot: Slot):
        return self.executed_cut.get(slot.replica_id, Slot(slot.replica_id, -1)) > slot

    def set_executed(self, slot: Slot):
        assert self.is_committed(slot), (slot, self.store.load(slot))
        assert not self.is_executed(slot), (slot, self.store.load(slot))
        self.executed[slot] = True

    def is_executed(self, slot: Slot):
        return self.is_cut(slot) or self.executed.get(slot, False)

    def is_committed(self, slot: Slot):
        return self.is_cut(slot) or self.store.load(slot).inst.state.stage >= Stage.Committed

    def build_execute_order(self, slot: Slot):
        assert not self.is_executed(slot), slot
        insts, graph = self._build_deps_graph(slot)

        cc = self._build_exec_order(insts, graph)

        return insts, cc

    def execute_command(self, cmd: Command):
        if cmd.payload:
            if isinstance(cmd.payload, Checkpoint):
                print(cmd)
            else:
                pass

    def build_execute_pending(self):
        for item in self.execute_pending:
            # todo: are cyclic dependencies in the graph causing the double-execution of them?
            # if self.is_executed(item):
            #     continue
            insts, cc = self.build_execute_order(item)

            for c in cc:
                for x in c:
                    self.set_executed(x)
                    self.execute_command(insts[x].state.command)

        self.execute_pending = deque()

    def event(self, x):
        if isinstance(x, InstanceState):
            if x.inst.state.stage >= Stage.Committed:

                # a slot is
                # a) either executed
                # b) or is still in DepthFirstHelper

                if not self.is_executed(x.slot):
                    unlocked_list = self.dph.ready(x.slot, [x for x in x.inst.state.deps if not self.is_executed(x)])

                    for unlocked in unlocked_list:
                        self.set_execute_pending(unlocked)

                    try:
                        self.build_execute_pending()
                    except:
                        self.dph.log_state()
                        raise

                # if slot not in self.expected_commit_count_from and not self.is_executed(slot):
                #     committed_deps = True
                #
                #     for d in x.inst.state.deps:
                #         if not self.is_committed(d):
                #             self.set_expected_commit_from(d, slot)
                #             committed_deps = False
                #
                #     if committed_deps:
                #         self.set_committed(slot)
                #         self.log(lambda: (f'{self.quorum.replica_id}\t{x.slot}\tex-pend-B\n'))
                #         self.set_execute_pending(slot)
                #
                #     if self.execute_pending:
                #         try:
                #             self.build_execute_pending()
                #         except:
                #             pprint((self.quorum.replica_id, self.expected_commit_count_from))
                #             pprint((self.quorum.replica_id, self.expected_commit_from))
                #             pprint((self.quorum.replica_id, self.execute_pending))
                #
                #             raise
        else:
            assert False, x
        yield Reply()


def main():
    dph = DepthFirstHelper()

    a = Slot(0, 1)
    b = Slot(0, 2)
    c = Slot(0, 3)
    d = Slot(0, 4)
    e = Slot(0, 5)
    f = Slot(0, 6)

    print('a', dph.ready('b', ['c']))
    print('b', dph.ready('a', ['b']))
    print('c', dph.ready('c', ['a']))

    print(dph.expected_ready_count_by, dph.expected_ready_from)

    # dph.log_state()
    print('=====')
    print('c', dph.ready('b', []))
    # dph.log_state()
    print('=====')
    print('d', dph.ready('c', []))
    # dph.log_state()
    # print(dph.commit(d, [a]))
    # print(dph.commit(e, [d]))
    # print(dph.commit(b, [a, c]))
    # print(dph.commit(b, [a, c]))
    # print(dph.commit(b, [a, c, d]))
    # dph.log_state()

    # print(dph.commit(c, [b, a]))

    print('DONE')

    # dph.log_state()

    # dph.set_committed(b)
    # dph.set_committed(a)
    # dph.set_committed(c)

    # dph.pprint()


if __name__ == '__main__':
    main()
