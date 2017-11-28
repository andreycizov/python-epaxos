from dsm.epaxos.instance.store import InstanceStore
# from dsm.epaxos.replica.acceptor import Acceptor
from dsm.epaxos.replica.acceptor_direct import AcceptorCoroutine
# from dsm.epaxos.replica.leader import Leader
from dsm.epaxos.replica.leader_direct import LeaderCoroutine
from dsm.epaxos.replica.state import ReplicaState


class Replica:
    def __init__(
        self,
        state: ReplicaState,
        store: InstanceStore
    ):
        self.state = state
        self.store = store
        self.leader = LeaderCoroutine(state, store)
        self.acceptor = AcceptorCoroutine(state, store, self.leader)

    def tick(self):
        self.state.tick()

    def check_timeouts_minimum_wait(self):
        return self.acceptor.check_timeouts_minimum_wait()

    def check_timeouts(self):
        self.acceptor.check_timeouts()

    def execute_pending(self):
        self.store.execute_all_pending()

