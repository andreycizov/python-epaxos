from dsm.epaxos.inst.store import InstanceStore
from dsm.epaxos.replica.main.ev import Wait, Reply
from dsm.epaxos.replica.state.ev import LoadCommandSlot, Load, Store


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
                print('Store', x)
                yield Reply(self.state.update(x.slot, x.state))
            else:
                assert False, x