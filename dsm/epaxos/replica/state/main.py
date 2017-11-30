from dsm.epaxos.inst.state import Stage
from dsm.epaxos.inst.store import InstanceStore
from dsm.epaxos.replica.main.ev import Wait, Reply
from dsm.epaxos.replica.state.ev import LoadCommandSlot, Load, Store, InstanceState


class StateActor:
    state: InstanceStore = InstanceStore()

    def run(self):
        while True:
            x = yield Wait()  # same as doing a Receive on something

            if isinstance(x, LoadCommandSlot):
                yield Reply(self.state.load_cmd_slot(x.id))
            elif isinstance(x, Load):
                yield Reply(self.state.load(x.slot))
            elif isinstance(x, Store):
                # todo: all stores modify timeouts
                r = self.state.update(x.slot, x.state)
                print(r)
                if r.state.stage == Stage.Committed:
                    print(r.state)
                    yield InstanceState(x.slot, r)
                yield Reply(r)
            else:
                assert False, x