from typing import NamedTuple, List, Dict

from dsm.epaxos.replica.config import ReplicaState


class ReplicaAddress(NamedTuple):
    replica_addr: str
    client_addr: str


class Quorum(NamedTuple):
    peers: List[int]
    replica_id: int
    epoch: int

    peer_addrs: Dict[int, ReplicaAddress]

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
    def full_size(self):
        return len(self.peers)

    @property
    def fast_size(self):
        return self.failure_size * 2

    @property
    def slow_size(self):
        return self.failure_size + 1


class Configuration(NamedTuple):
    timeout: int = 1
    timeout_range: int = 3
    jiffies: int = 33
    checkpoint_each: int = 10 * 33

    @property
    def seconds_per_tick(self):
        return 1. / self.jiffies
