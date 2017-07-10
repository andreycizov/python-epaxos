import heapq
from datetime import datetime
from typing import NamedTuple, List, Dict

from dsm.epaxos.instance.state import Slot, InstanceState, StateType, Ballot
from dsm.epaxos.replica.state import ReplicaState


class TimeoutStoreState(NamedTuple):
    time: datetime
    slot: Slot
    ballot: Ballot
    type: StateType


class TimeoutStore:
    def __init__(self, state: ReplicaState):
        self.state = state
        self.timeouts = []  # type: List[TimeoutStoreState]
        self.last_states = {}  # type: Dict[Slot, TimeoutStoreState]
        heapq.heapify(self.timeouts)

    def now(self):
        return datetime.now()

    def update(self, slot: Slot, old_inst: InstanceState, new_inst: InstanceState):
        last_state = TimeoutStoreState(self.now() + self.state.timeout, slot, new_inst.ballot, new_inst.type)

        if new_inst.type < StateType.Committed:
            heapq.heappush(self.timeouts, last_state)

        self.last_states[slot] = last_state

    def query(self) -> List[Slot]:
        now = self.now()

        r = []
        while len(self.timeouts) and self.timeouts[0].time < now:
            item = heapq.heappop(self.timeouts)  # type: TimeoutStoreState

            inst = self.last_states[item.slot]

            if inst.type == item.type and inst.ballot == item.ballot:
                r.append(item.slot)
        return r
