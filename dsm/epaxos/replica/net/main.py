from typing import Dict, Any, NamedTuple

from dsm.epaxos.net.packet import Packet, Payload
from dsm.epaxos.replica.main.ev import Wait, Reply
from dsm.epaxos.replica.net.ev import Send


class ClientNetComm(NamedTuple):
    dest: int
    payload: Payload


class NetActor:
    def __init__(self):
        self.peers = {}  # type: Dict[int, Any]

    def run(self):
        while True:
            x = yield Wait()  # same as doing a Receive on something

            if isinstance(x, Send):
                yield ClientNetComm(x.dest, x.payload)

                yield Reply('sent')
            else:
                assert False, x
