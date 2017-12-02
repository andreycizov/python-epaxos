import logging
from datetime import datetime
from typing import Dict, NamedTuple, List

from dsm.epaxos.net import packet
from dsm.epaxos.net.packet import Packet
from dsm.epaxos.replica.main.ev import Tick, Reply
from dsm.epaxos.replica.net.ev import Send
from dsm.epaxos.replica.quorum.ev import Quorum

logger = logging.getLogger('pingpong')


class PingPongActor:
    def __init__(self, quorum: Quorum):
        self.quorum = quorum
        self.ping_every_tick = 10
        self.keep_times = 10
        self.last_ping = {}  # type: Dict[int, datetime]
        self.last_ping_id = {}  # type: Dict[int, int]
        self.pings_sent = {}  # type: Dict[int, int]
        self.pings_rcvd = {}  # type: Dict[int, int]
        self.pings_times = {}  # type: Dict[int, List[float]]

    def event(self, x):
        if isinstance(x, Packet):
            if isinstance(x.payload, packet.PingRequest):
                yield Send(x.origin, packet.PongResponse(x.payload.id))
            elif isinstance(x.payload, packet.PongResponse):
                if x.payload.id == self.last_ping_id.get(x.origin, -1):
                    time = datetime.now() - self.last_ping[x.origin]
                    self.pings_rcvd[x.origin] = self.pings_rcvd.get(x.origin, 0) + 1
                    self.pings_times[x.origin] = (self.pings_times.get(x.origin, []) + [time.total_seconds()])[:self.keep_times]
                else:
                    # todo: reordered pings
                    pass
            else:
                assert False, ''
        elif isinstance(x, Tick):
            if x.id % self.ping_every_tick == 0:
                now = datetime.now()
                for peer in self.quorum.peers:
                    self.last_ping[peer] = now
                    self.last_ping_id[peer] = self.last_ping_id.get(peer, 0)
                    self.pings_sent[peer] = self.pings_sent.get(peer, 0) + 1
                    yield Send(peer, packet.PingRequest(self.last_ping_id[peer]))

            if x.id % 300 == 0:
                pings_repl = sorted((k, f'{sum(v)/len(v)*1000:0.2f}ms') for k, v in self.pings_times.items())
                pings_recv = sorted((k, f'{v}/{self.pings_sent[k]}') for k, v in self.pings_rcvd.items())
                logger.error(f'{self.quorum.replica_id} {pings_repl} {pings_recv}')
        else:
            assert False, ''
        yield Reply()
