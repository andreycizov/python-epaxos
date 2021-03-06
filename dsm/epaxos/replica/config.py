from collections import defaultdict
from typing import Any, List


class ReplicaState:
    def __init__(
        self,
        # channel: Any,
        epoch: int,
        replica_id: int,
        quorum_fast: List[int],
        quorum_full: List[int],
        live: bool = True,
        timeout: int = 3,
        jiffies: int = 33,
        timeout_range: int = 3,
        checkpoint_each: int = 10,
    ):
        # self.channel = channel
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
        self.checkpoint_each = checkpoint_each

    def tick(self):
        self.ticks += 1