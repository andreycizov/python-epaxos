from typing import SupportsInt, Set


class ReplicaState:
    def __init__(
        self,
        epoch: SupportsInt,
        ident: SupportsInt,
        quorum_fast: Set[SupportsInt],
        quorum_full: Set[SupportsInt],
        live: bool = True,
    ):
        self.epoch = epoch
        self.ident = ident
        self.quorum_fast = quorum_fast
        self.quorum_full = quorum_full
        self.live = live
