from typing import List, NamedTuple, Any

from collections import defaultdict


class ReplicaState:
    def __init__(
        self,
        channel: Any,
        epoch: int,
        replica_id: int,
        quorum_fast: List[int],
        quorum_full: List[int],
        live: bool = True,
        timeout: int = 1,
        jiffies: int = 33,
        timeout_range: int = 3
    ):
        self.channel = channel
        self.epoch = epoch
        self.replica_id = replica_id
        self.quorum_fast = quorum_fast
        self.quorum_full = quorum_full
        self.live = live
        self.timeout = timeout
        self.ticks = 0
        self.jiffies = jiffies
        self.seconds_per_tick = 1. / self.jiffies

        self.packet_counts = defaultdict(int)
        self.timeout_range = timeout_range

        self.total_sleep = 0
        self.total_exec = 0
        self.total_timeouts = 0
        self.total_recv = 0

    def tick(self):
        self.ticks += 1


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