import heapq
import random
from datetime import datetime, timedelta
from typing import NamedTuple, List, Dict, Optional

import logging

from dsm.epaxos.instance.state import Slot, InstanceState, StateType, Ballot
from dsm.epaxos.replica.state import ReplicaState

logger = logging.getLogger(__name__)


class TimeoutStoreState(NamedTuple):
    time: int
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
        last_state = TimeoutStoreState(self.state.ticks + self.state.timeout + random.randint(0, self.state.timeout_range), slot, new_inst.ballot, new_inst.type)

        if new_inst.type < StateType.Committed:
            self.last_states[slot] = last_state
        elif slot in self.last_states:
            del self.last_states[slot]

    def minimum_wait(self) -> Optional[float]:
        now = self.state.ticks

        if len(self.last_states.items()):
            return max(min((now - x.time) * self.state.seconds_per_tick for x in self.last_states.values()), 0)
        else:
            return None

    def query(self) -> List[Slot]:
        r = []
        for k in [k for k, v in self.last_states.items() if v.time < self.state.ticks]:
            del self.last_states[k]
            r.append(k)
        return r
