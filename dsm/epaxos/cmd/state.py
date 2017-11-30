import uuid
from typing import NamedTuple, Any, Union, List


class Checkpoint(NamedTuple):
    charlie: int


class Mutator(NamedTuple):
    op: str
    keys: List[int]


CLASSES = [
    Checkpoint,
    Mutator
]

CLASSES_MAP = {k.__name__[:1]: k for k in CLASSES}
CLASSES_MAP_BACK = {k: k.__name__[:1] for k in CLASSES}


class CommandID(uuid.UUID):
    @classmethod
    def create(cls):
        return uuid.uuid4()


class Command(NamedTuple):
    id: CommandID
    payload: Union[Checkpoint, Mutator]

    # @classmethod
    # def deserializer(cls, sub_des):
    #     return lambda json: cls(sub_des(uuid.UUID, json['i']), sub_des(CLASSES_MAP[json['x']], json['p']))
    #
    # @classmethod
    # def serializer(cls, sub_ser):
    #     return lambda obj: {'i': sub_ser(obj.id), 'x': CLASSES_MAP_BACK[obj.payload.__class__],
    #                         'p': sub_ser(obj.payload)}
