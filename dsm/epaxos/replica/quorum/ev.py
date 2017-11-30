from typing import NamedTuple, List

from dsm.epaxos.replica.config import ReplicaState


class Quorum(NamedTuple):
    peers: List[int]
    replica_id: int
    epoch: int

    @classmethod
    def from_state(cls, state: ReplicaState):
        return cls(
            [x for x in state.quorum_full if x != state.replica_id],
            state.replica_id,
            state.epoch
        )

    @property
    def failure_size(self):
        return (len(self.peers) + 1) // 2

    @property
    def fast_size(self):
        return self.failure_size * 2

    @property
    def slow_size(self):
        return self.failure_size + 1
