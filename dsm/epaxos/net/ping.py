from typing import NamedTuple


class PingPacket(NamedTuple):
    seq: int


class PongPacket(NamedTuple):
    seq: int
