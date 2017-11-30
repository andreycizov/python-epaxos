from itertools import groupby
from turtledemo.clock import tick
from typing import NamedTuple

from dsm.epaxos.inst.state import Stage
from dsm.epaxos.inst.store import InstanceStore
from dsm.epaxos.replica.main.ev import Wait, Reply, Tick
from dsm.epaxos.replica.quorum.ev import Quorum
from dsm.epaxos.replica.state.ev import LoadCommandSlot, Load, Store, InstanceState


class StateActor(NamedTuple):
    quorum: Quorum
    state: InstanceStore = InstanceStore()

    def event(self, x):
        if isinstance(x, Tick):
            def lenx(iter_obj):
                i = 0
                for x in iter_obj:
                    i += 1
                return i

            if x.id % 330 == 0:
                print(self.quorum.replica_id, {x.name: lenx(y) for x, y in groupby(sorted(x.state.stage for x in self.state.inst.values()))})

            yield Reply()
        elif isinstance(x, LoadCommandSlot):
            yield Reply(self.state.load_cmd_slot(x.id))
        elif isinstance(x, Load):
            yield Reply(self.state.load(x.slot).inst)
        elif isinstance(x, Store):
            # todo: all stores modify timeouts
            old, new = self.state.update(x.slot, x.inst)

            deps_comm = []
            for d in new.state.deps:
                r = self.state.load(d)

                if not r.exists:
                    self.state.update(d, r.inst)
                    yield InstanceState(d, r.inst)

                deps_comm.append(r.inst.state.stage == Stage.Committed)

            yield InstanceState(x.slot, new)
            yield Reply(new)
        else:
            assert False, x
