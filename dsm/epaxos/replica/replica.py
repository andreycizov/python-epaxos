from dsm.epaxos.instance.store import InstanceStore
from dsm.epaxos.replica.acceptor import Acceptor
from dsm.epaxos.replica.leader import Leader
from dsm.epaxos.replica.state import ReplicaState


class Replica(Leader, Acceptor):
    def __init__(
        self,
        state: ReplicaState,
        store: InstanceStore
    ):
        self.store = store
        Leader.__init__(self, state, store)
        Acceptor.__init__(self, state, store)

    def check_timeouts(self):
        for slot in self.store.timeout_store.query():
            self.begin_explicit_prepare(slot)
