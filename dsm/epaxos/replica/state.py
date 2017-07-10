from datetime import timedelta
from typing import Set

from dsm.epaxos.network.peer import Channel


class ReplicaState:
    def __init__(
        self,
        channel: Channel,
        epoch: int,
        replica_id: int,
        quorum_fast: Set[int],
        quorum_full: Set[int],
        live: bool = True,
        timeout: timedelta = timedelta(5)
    ):
        self.channel = channel
        self.epoch = epoch
        self.replica_id = replica_id
        self.quorum_fast = quorum_fast
        self.quorum_full = quorum_full
        self.live = live
        self.timeout = timeout
