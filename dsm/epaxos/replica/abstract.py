from dsm.epaxos.instance.store import InstanceStore
from dsm.epaxos.replica.state import ReplicaState


class Behaviour:
    def __init__(
        self,
        state: ReplicaState,
        store: InstanceStore,
    ):
        self.state = state
        self.store = store

