from typing import SupportsInt, Set

from dsm.epaxos.network.peer import Channel


class ReplicaState:
    def __init__(
        self,
        channel: Channel,
        epoch: SupportsInt,
        peer: SupportsInt,
        quorum_fast: Set[SupportsInt],
        quorum_full: Set[SupportsInt],
        live: bool = True,
    ):
        self.channel = channel
        self.epoch = epoch
        self.peer = peer
        self.quorum_fast = quorum_fast
        self.quorum_full = quorum_full
        self.live = live
