import logging
import select

from dsm.epaxos.net.impl.generic.server import ReplicaServer
from dsm.epaxos.net.impl.udp.util import _recv_parse_buffer, create_bind, create_socket, deserialize, serialize, \
    _addr_conv
from dsm.epaxos.net.packet import Packet
from dsm.epaxos.replica.net.ev import Send
from dsm.epaxos.replica.net.main import NetActor
from dsm.epaxos.replica.quorum.ev import Quorum

logger = logging.getLogger(__name__)


class UDPNetActor(NetActor):
    def __init__(self, quorum: Quorum):
        super().__init__()
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

        self.socket.sendto(serialize(packet), dst)

    def close(self):
        self.socket.close()


class UDPReplicaServer(ReplicaServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.socket_server = create_bind(self.peer_addr[self.quorum.replica_id].replica_addr)

    def build_net_actor(self) -> NetActor:
        return UDPNetActor(self.quorum)

    def poll(self, min_wait):
        r, _, _ = select.select([self.socket_server], [], [], min_wait)
        return len(r) > 0

    def send(self):
        return 0

    def recv(self):
        for i, (addr, body) in enumerate(_recv_parse_buffer(self.socket_server)):
            # todo: save addr -> body mapping in here.

            x = deserialize(body)

            if x.origin not in self.net_actor.clients:
                self.net_actor.clients[x.origin] = addr

            yield x

    def close(self):
        self.socket_server.close()
        self.net_actor.close()
