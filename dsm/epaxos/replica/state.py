from typing import Set

from dsm.epaxos.network.peer import Channel


class ReplicaState:
    def __init__(
        self,
        channel: Channel,
        epoch: int,
        peer: int,
        quorum_fast: Set[int],
        quorum_full: Set[int],
        live: bool = True,
    ):
        self.channel = channel
        self.epoch = epoch
        self.peer = peer
        self.quorum_fast = quorum_fast
        self.quorum_full = quorum_full
        self.live = live
