import logging
from itertools import groupby

from dsm.epaxos.inst.state import Stage
from dsm.epaxos.inst.store import InstanceStore
from dsm.epaxos.replica.main.ev import Wait, Reply, Tick
from dsm.epaxos.replica.quorum.ev import Quorum
from dsm.epaxos.replica.state.ev import LoadCommandSlot, Load, Store, InstanceState, CheckpointEvent

logger = logging.getLogger('state')


class Log:
    def __init__(self, filename):
        self._log = open(filename, 'w+')

    def __call__(self, fn=lambda: ''):
        self._log.write(fn())
        self._log.flush()


class StateActor:
    def __init__(self, quorum: Quorum, store: InstanceStore):
        self.quorum = quorum
        self.store = store
        self.prev_cp = None

        self.log = Log(f'state-{self.quorum.replica_id}.log')

    def event(self, x):
        if isinstance(x, Tick):
            def lenx(iter_obj):
                i = 0
                for x in iter_obj:
                    i += 1
                return i

            if x.id % 330 == 0:
                instc = sorted((x.name, lenx(y)) for x, y in groupby(sorted(x.state.stage for x in self.store.inst.values())))
                logger.error(f'{self.quorum.replica_id} {instc}')

            yield Reply()
        elif isinstance(x, LoadCommandSlot):
            yield Reply(self.store.load_cmd_slot(x.id))
        elif isinstance(x, Load):
            yield Reply(self.store.load(x.slot).inst)
        elif isinstance(x, Store):
            # todo: all stores modify timeouts
            old, new = self.store.update(x.slot, x.inst)

            self.log(lambda: f'{self.quorum.replica_id}\t{x.slot}\t{new}\n')

            deps_comm = []
            for d in new.state.deps:
                r = self.store.load(d)

                if not r.exists:
                    r_old, r_new = self.store.update(d, r.inst)
                    yield InstanceState(d, r_new)

                deps_comm.append(r.inst.state.stage == Stage.Committed)

            # if new.ballot.b > 10:
            #     logger.error(f'{self.quorum.replica_id} {x.slot} {new} HW')

            yield InstanceState(x.slot, new)
            yield Reply(new)
        elif isinstance(x, CheckpointEvent):
            self.store.set_cp(x.at)
            yield Reply(None)
        else:
            assert False, x
