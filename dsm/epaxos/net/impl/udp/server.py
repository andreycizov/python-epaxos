import logging
import random
import select
from collections import defaultdict

from dsm.epaxos.net.impl.generic.server import ReplicaServer
from dsm.epaxos.net.impl.udp.util import _recv_parse_buffer, create_bind, create_socket, deserialize, serialize, \
    _addr_conv
from dsm.epaxos.net.packet import Packet
from dsm.epaxos.replica.net.ev import Send
from dsm.epaxos.replica.net.main import NetActor
from dsm.epaxos.replica.quorum.ev import Quorum

logger = logging.getLogger(__name__)

DROP_RATE = 0.01


class NetStats:
    def __init__(self):
        self.send = defaultdict(int)
        self.recv = defaultdict(int)
        self.traffic_send = 0
        self.traffic_recv = 0


class UDPNetActor(NetActor):
    def __init__(self, quorum: Quorum):
        super().__init__()
        self.net_stats: NetStats
        self.quorum = quorum
        self.socket = create_socket()
        self.clients = {}
        self.peers = {k: _addr_conv(x.replica_addr) for k, x in self.quorum.peer_addrs.items()}

    def send(self, s: Send):
        packet = Packet(
            self.quorum.replica_id,
            s.dest,
            s.payload.__class__.__name__,
            s.payload
        )

        if packet.destination in self.peers:
            dst = self.peers[packet.destination]
        elif packet.destination in self.clients:
            dst = self.clients.get(packet.destination)
        else:
            logger.error(f'Dropping packet {packet} due to unknown destination')
            return

        # print('>>>>>>>>>', self.quorum.replica_id, s.dest, packet)


        body = serialize(packet)

        self.net_stats.send[packet.type] += 1
        self.net_stats.traffic_send += len(body)


        if random.random() < DROP_RATE:
            return

        try:
            self.socket.sendto(body, dst)
        except OSError:
            logger.info(f'{body}')
            logger.exception('')

    def close(self):
        self.socket.close()


class UDPReplicaServer(ReplicaServer):
    def __init__(self, *args, **kwargs):
        self.net_stats = NetStats()
        super().__init__(*args, **kwargs)

        self.socket_server = create_bind(self.peer_addr[self.quorum.replica_id].replica_addr)

    def build_net_actor(self) -> NetActor:
        r = UDPNetActor(self.quorum)
        r.net_stats = self.net_stats
        return r

    def poll(self, min_wait):
        r, _, _ = select.select([self.socket_server], [], [], min_wait)
        return len(r) > 0

    def send(self):
        return 0

    def recv(self):
        for i, (addr, body) in enumerate(_recv_parse_buffer(self.socket_server)):
            # todo: save addr -> body mapping in here.

            x = deserialize(body)

            if random.random() < DROP_RATE:
                continue

            self.net_stats.recv[x.type] += 1
            self.net_stats.traffic_recv += len(body)

            # print('<<<<<<<<', self.quorum.replica_id, x)

            if x.origin not in self.net_actor.clients:
                self.net_actor.clients[x.origin] = addr

            yield x

    def close(self):
        self.socket_server.close()
        self.net_actor.close()
