from enum import IntEnum
from typing import NamedTuple, List, Optional

from dsm.epaxos.cmd.state import Command


class Slot(NamedTuple):
    replica_id: int
    instance_id: int

    def ballot_initial(self, epoch=0):
        return Ballot(epoch, 0, self.replica_id)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.replica_id},{self.instance_id})'

    def next(self):
        return Slot(self.replica_id, self.instance_id + 1)

    @classmethod
    def serializer(cls, sub_ser):
        return lambda obj: [obj.replica_id, obj.instance_id]

    @classmethod
    def deserializer(cls, sub_des):
        return lambda json: cls(*json)


class Ballot(NamedTuple):
    epoch: int
    b: int
    replica_id: int

    def next(self, replica_id=None):
        if replica_id is None:
            replica_id = self.replica_id
        return Ballot(self.epoch, self.b + 1, replica_id)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.epoch},{self.b},{self.replica_id})'

    @classmethod
    def serializer(cls, sub_ser):
        return lambda obj: [obj.epoch, obj.b, obj.replica_id]

    @classmethod
    def deserializer(cls, sub_des):
        return lambda json: cls(*json)


class Stage(IntEnum):
    Prepared = 0
    PreAccepted = 1
    Accepted = 2
    Committed = 4
    Executed = 5
    Purged = 6


class State(NamedTuple):
    stage: Stage
    command: Optional[Command]
    seq: int
    deps: List[Slot]

    def __repr__(self):
        return f'{self.__class__.__name__}({self.stage.name},{self.command},{self.seq},{self.deps})'
