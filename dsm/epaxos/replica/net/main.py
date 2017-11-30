from typing import Dict, Any

from dsm.epaxos.replica.main.ev import Wait, Reply
from dsm.epaxos.replica.net.ev import Send


class NetActor:
    def __init__(self):
        self.peers = {}  # type: Dict[int, Any]

    def run(self):
        while True:
            x = yield Wait()  # same as doing a Receive on something

            if isinstance(x, Send):
                print('Sending', x.dest, x.payload)

                yield Reply('sent')
            else:
                assert False, x
