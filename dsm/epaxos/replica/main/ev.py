from typing import NamedTuple, Optional, Any


class Tick(NamedTuple):
    id: int


class Wait:
    def __repr__(self):
        return 'Wait()'


class Reply(NamedTuple):
    payload: Optional[Any] = None