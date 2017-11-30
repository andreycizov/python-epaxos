from typing import Dict, Any

from dsm.epaxos.replica.main.ev import Wait, Reply
from dsm.epaxos.replica.net.ev import Send


class NetActor:
    def __init__(self):
        self.peers = {}  # type: Dict[int, Any]

    def send(self, payload: Send):
        raise NotImplementedError('')

    def run(self):
        while True:
            x = yield Wait()  # same as doing a Receive on something

            if isinstance(x, Send):
                self.send(x)
                yield Reply()
            else:
                assert False, x
