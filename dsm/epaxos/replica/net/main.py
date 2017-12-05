import logging
from typing import Dict, Any

from dsm.epaxos.replica.main.ev import Wait, Reply, Tick
from dsm.epaxos.replica.net.ev import Send

logger = logging.getLogger('net')


class NetActor:
    def __init__(self):
        self.peers = {}  # type: Dict[int, Any]

    def send(self, payload: Send):
        raise NotImplementedError('')

    def event(self, x):
        if isinstance(x, Send):
            self.send(x)
            yield Reply()
        elif isinstance(x, Tick):
            if x.id % 330 == 0 and hasattr(self, 'net_stats'):
                rcv = sorted([(k, v) for k, v in self.net_stats.recv.items()])
                snd = sorted([(k, v) for k, v in self.net_stats.send.items()])
                logger.error(f'{self.quorum.replica_id} {self.net_stats.traffic_recv} {self.net_stats.traffic_send} {rcv} {snd}')
            yield Reply()
        else:
            assert False, x
