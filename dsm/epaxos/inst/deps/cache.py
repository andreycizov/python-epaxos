from itertools import groupby
from typing import NamedTuple, Dict, List, Optional

from dsm.epaxos.cmd.state import Command, Checkpoint, Mutator
from dsm.epaxos.inst.state import Slot


class CacheState(NamedTuple):
    slot: Slot
    seq: int


class CPCacheState(NamedTuple):
    state: CacheState
    deps: List[Slot]


class KeyedDepsCache:
    def __init__(self):
        self.store = {}  # type: Dict[int, CacheState]
        self.cp = None  # type: Optional[CPCacheState]

    def _last_seq_max(self, slot, mut: Mutator):
        # a checkpoint always depends on the previous checkpoint and the set of

        last_seq = -1
        for x in mut.keys:
            inter_val = self.store.get(x)

            if inter_val and inter_val.slot != slot and inter_val.slot < slot:
                last_seq = max(last_seq, inter_val.seq)

        if self.cp:
            last_seq = max(self.cp.state.seq, last_seq)

        return last_seq + 1

    def _update_store(self, slot: Slot, mut: Mutator, seq: int):
        r = []

        for x in mut.keys:
            inter_val = self.store.get(x)

            if inter_val and inter_val.slot != slot and inter_val.slot < slot:
                r.append(inter_val.slot)

            if inter_val is None or inter_val.slot < slot:
                self.store[x] = CacheState(
                    slot,
                    seq
                )

        return r

    def xchange(self, slot: Slot, cmd: Command):
        if isinstance(cmd.payload, Mutator):
            seq = self._last_seq_max(slot, cmd.payload)
            deps = self._update_store(
                slot,
                cmd.payload,
                seq
            )
            return seq, sorted(set(deps))
        elif isinstance(cmd.payload, Checkpoint):
            # Checkpoint - "These are the last slots I know about."

            if self.cp is None:
                new_seq = max((x.seq for x in self.store.values()), default=-1) + 1
                new_deps = [x.slot for x in self.store.values()]

            elif self.cp.state.slot == slot:
                new_seq = max(
                    max((x.seq for x in self.store.values()), default=-1),
                    self.cp.state.seq - 1
                ) + 1
                new_deps = [x.slot for x in self.store.values()] + self.cp.deps
            else:
                new_seq = max(max((x.seq for x in self.store.values()), default=-1), self.cp.state.seq) + 1
                new_deps = [x.slot for x in self.store.values()] + [self.cp.state.slot]

            new_deps = sorted(set(new_deps))

            def last(xs):
                r = None
                for x in xs:
                    r = x
                return x

            new_deps = [last(xs) for x, xs in groupby(new_deps, key=lambda x: x.replica_id)]

            self.cp = CPCacheState(
                CacheState(
                    slot,
                    new_seq
                ),
                sorted(new_deps)
            )

            self.store = {}
            return self.cp.state.seq, self.cp.deps
        else:
            assert False, (slot, cmd)
