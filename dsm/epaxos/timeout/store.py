import heapq
from datetime import datetime
from typing import NamedTuple, List

from dsm.epaxos.instance.state import Slot, InstanceState, StateType, Ballot
from dsm.epaxos.instance.store import InstanceStore
from dsm.epaxos.replica.state import ReplicaState


class TimeoutStoreState(NamedTuple):
    time: datetime
    slot: Slot
    ballot: Ballot
    type: StateType


class TimeoutStore:
    def __init__(self, state: ReplicaState, store: InstanceStore):
        self.state = state
        self.store = store
        self.timeouts = []  # type: List[TimeoutStoreState]
        heapq.heapify(self.timeouts)

    def now(self):
        return datetime.now()

    def update(self, slot: Slot, old_inst: InstanceState, new_inst: InstanceState):
        if new_inst.type < StateType.Committed:
            heapq.heappush(self.timeouts,
                           TimeoutStoreState(self.now() + self.state.timeout, slot, new_inst.ballot, new_inst.type))

    def query(self) -> List[Slot]:
        now = self.now()

        r = []
        while len(self.timeouts) and self.timeouts[0].time < now:
            item = heapq.heappop(self.timeouts)  # type: TimeoutStoreState

            inst = self.store[item.slot]

            if inst.type == item.type and inst.ballot == item.ballot:
                r.append(item.slot)
        return r
