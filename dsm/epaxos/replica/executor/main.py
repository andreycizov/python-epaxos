import logging
from collections import deque
from pprint import pprint
from typing import NamedTuple, Dict, Deque, List, Optional, Tuple, Set

from tarjan import tarjan

from dsm.epaxos.cmd.state import Command, Checkpoint
from dsm.epaxos.inst.state import Slot, Stage
from dsm.epaxos.inst.store import InstanceStore, InstanceStoreState
from dsm.epaxos.replica.main.ev import Reply, Tick
from dsm.epaxos.replica.quorum.ev import Quorum
from dsm.epaxos.replica.state.ev import InstanceState, CheckpointEvent

logger = logging.getLogger('executor')


class CC:
    """


    a: OUT -> cc1
    b: OUT -> cc2
    c0: IN -> cc1
    c1: OUT -> cc1

    INSERT d -> (a, g, f)

    a: INNER -> cc1
    b: OUT -> cc2
    c0: IN -> cc1
    c1: OUT -> cc1
    g: IN -> cc1
    f: IN -Ю cc1

    -----------
    removals need to be done as a transaction (all-or-none)

    swaps (
    -----------

    a > 0
    b < 1
    c > 1
    d < 1

    __________

    c < b
    d < b
    a > b

    -----------

    """

    def __init__(self, ins: Set[Slot], outs: Set[Slot], items: Set[Slot]):
        self.ins: Set[Slot] = ins
        self.outs: Set[Slot] = outs
        self.items: Set[Slot] = items

    def done(self):
        return len(self.ins) == 0

    def overlap(self, cc: 'CC'):
        # self is in the store.
        # all keys are not re-entrable

        if self.ins & cc.outs:
            return True
        else:
            return (self.ins | self.outs | self.items) & (cc.ins | cc.outs | cc.items)

        # todo: that can be made easier IFF "merge" is called once per a a job piece.
        # return (self.ins | self.outs | self.items) & (cc.ins | cc.outs | cc.items)
        # return len(self.ins & cc.outs) or len(cc.ins & self.outs) or len(self.items & cc.ins) or len(
        #     self.ins & cc.items) or len(self.items & cc.outs) or len(self.items & cc.outs)

    def all(self):
        return self.ins | self.outs | self.items

    def merge(self, cc: 'CC'):
        ins = self.ins | cc.ins
        outs = self.outs | cc.outs
        items = self.items | cc.items

        self.ins = ins - outs
        self.outs = outs - ins
        self.items = items | (ins & outs)

        self.ins = self.ins - self.items
        self.outs = self.outs - self.items

        assert len(self.ins & self.outs) == 0, repr(self)
        assert len(self.outs & self.items) == 0, repr(self)
        assert len(self.items & self.ins) == 0, repr(self)

    def __repr__(self):
        return f'CC({self.ins},{self.outs},{self.items})'

    def depth(self):
        return len(self.ins) + len(self.outs) + len(self.items)

    @classmethod
    def from_item(self, item: Slot, deps: Set[Slot]):
        return CC(
            deps,
            {item},
            set()
        )


class DepthFirstHelper:
    """
    Accumulate dependencies via `commit`, until the graph is complete, then return them
    when all of them have been marked via `commit`.
    """

    def __init__(self):
        self.ccs = {}  # type: Dict[int, CC]
        self.next_idx = 0

    def ready(self, slot: Slot, deps: List[Slot]):
        new_cc = CC.from_item(
            slot,
            set(deps)
        )

        overlaps = []

        for k, cc in self.ccs.items():
            if cc.overlap(new_cc):
                overlaps.append(k)

        for overlap in overlaps:
            new_cc.merge(self.ccs[overlap])
            del self.ccs[overlap]

        if new_cc.done():
            return list(new_cc.items | new_cc.outs)
        else:
            self.ccs[self.next_idx] = new_cc
            # print(self.ccs[self.next_idx])

            self.next_idx += 1
            return []


class ExecutorActor:
    def __init__(self, quorum: Quorum, store: InstanceStore):
        self.quorum = quorum
        self.store = store

        self.executed_cut = {}  # type: Dict[int, Slot]
        self.executed = {}  # type: Dict[Slot, bool]
        self.executing = {}  # type: Dict[Slot, bool]

        self._log = open(f'executor-{self.quorum.replica_id}.log', 'w+')

        self.dph = DepthFirstHelper()
        self.ctr = 0

        self.st_exec = 0
        self.st_max_depth = 0

        # self.commit_expected = defaultdict(set)  # type: Dict[Slot, Set[Slot]]

    def log(self, fn: lambda: None):
        self._log.write(fn())
        self._log.flush()

    def is_cut(self, slot: Slot):
        return self.executed_cut.get(slot.replica_id, Slot(slot.replica_id, -1)) >= slot

    def set_executed(self, slot: Slot):
        assert self.is_committed(slot), (slot, self.store.load(slot))
        assert not self.is_executed(slot), (slot, self.store.load(slot))
        self.executed[slot] = True
        self.st_exec += 1

        slot = slot

        while self.is_executed(self.executed_cut.get(slot.replica_id, Slot(slot.replica_id, -1)).next()):
            self.executed_cut[slot.replica_id] = slot
            del self.executed[slot]

            slot = slot.next()

    def is_executing(self, slot):
        return self.is_cut(slot) or self.executing.get(slot, False)

    def is_executed(self, slot: Slot):
        return self.is_cut(slot) or self.executed.get(slot, False)

    def is_committed(self, slot: Slot):
        return self.is_cut(slot) or self.store.load(slot).inst.state.stage >= Stage.Committed

    def execute_command(self, slot: Slot, cmd: Command):
        self.log(lambda: f'{self.quorum.replica_id}\tCOMM\t{slot}\t{cmd}\t{self.executed_cut}\t{self.ctr}\n')

        if cmd:
            if cmd.payload:
                if isinstance(cmd.payload, Checkpoint):
                    return slot
                else:
                    return None

    def build_execute_pending(self, cc):
        insts = {x: self.store.load(x).inst for x in cc}
        cc = sorted(cc, key=lambda x: insts[x].state.seq)

        cps = []
        for x in cc:
            self.set_executed(x)
            x = self.execute_command(x, insts[x].state.command)
            if x:
                cps.append(x)

        return cps

    def event(self, x):
        if isinstance(x, InstanceState):

            if x.inst.state.stage >= Stage.Committed:
                # self.log(lambda: f'{self.quorum.replica_id}\tSTAT\t{x.slot}\t{x.inst}\n')
                if not self.is_executed(x.slot) and not self.is_executing(x.slot):
                    self.executing[x.slot] = True
                    self.ctr += 1
                    self.log(lambda: f'{self.quorum.replica_id}\tDPH0\t{self.dph.ccs}\n')

                    unlocked_list = self.dph.ready(x.slot, [x for x in x.inst.state.deps if not self.is_executed(x)])
                    self.log(lambda: f'{self.quorum.replica_id}\tDPH1\t{self.dph.ccs}\n')
                    self.log(lambda: f'{self.quorum.replica_id}\tDPH2\t{unlocked_list}\n')

                    # if self.ctr % 100:
                    #     self.log(lambda: f'{self.quorum.replica_id}\tDPHX\t{self.dph.ccs}\n')

                    try:
                        for checkpoint in self.build_execute_pending(unlocked_list):
                            xx = self.store.load(checkpoint).inst
                            yield CheckpointEvent(checkpoint, {x.replica_id: x for x in xx.state.deps})
                    except:
                        self.log(lambda: f'{self.quorum.replica_id}\tDPHz\t{self.dph.ccs}\n')
                        raise
        elif isinstance(x, Tick):
            if x.id % 330 == 0:
                totd = sum(x.depth() for _, x in self.dph.ccs.items())
                logger.error(
                    f'{self.quorum.replica_id} Exec={self.st_exec} Deps={len(self.dph.ccs)} Depth={totd}')
                if totd > 10:
                    for cc in self.dph.ccs.values():
                        logger.error(
                            f'{self.quorum.replica_id} {cc}')
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

    print('a', dph.ready('b', ['a']))
    print('a', dph.ready('b', ['d']))
    print(dph.ccs)
    print('b', dph.ready('c', ['b']))
    print(dph.ccs)
    print('c', dph.ready('a', ['c']))
    print('c', dph.ready('d', []))
    print(dph.ccs)
    # print(dph.expected_ready_count_by, dph.expected_ready_from, dph.visited)

    # dph.log_state()
    print('=====')
    print('c', dph.ready('b', []))
    # dph.log_state()
    print('=A1===')

    print(dph.ready(1, [2]))
    print(dph.ready(4, [5]))
    print(dph.ready(2, [3]))
    print(dph.ready(3, [4]))
    print(dph.ready(5, []))
    print(dph.ccs)

    # print(dph.expected_ready_count_by, dph.expected_ready_from, dph.visited)

    print('=A2===')

    print(dph.ready(Slot(1, 10), [Slot(2, 3)]))
    print(dph.ready(Slot(2, 3), [Slot(1, 4)]))
    print(dph.ccs)

    # print(dph.expected_ready_count_by, dph.expected_ready_from, dph.visited)

    print('=B===')
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
