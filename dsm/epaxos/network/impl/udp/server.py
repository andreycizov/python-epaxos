import select
import socket
from typing import Tuple

from dsm.epaxos.network.impl.generic.server import ReplicaServer
from dsm.epaxos.network.impl.udp.mapper import UDPReplicaReceiveChannel, UDPReplicaSendChannel
from dsm.epaxos.network.impl.udp.util import _addr_conv, _recv_parse_buffer
from dsm.epaxos.network.peer import Channel


class UDPReplicaServer(ReplicaServer):
    def init(self, replica_id: int) -> Tuple[Channel, Channel]:
        sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM
        )
        sock.bind(_addr_conv(self.peer_addr, replica_id))
        sock.settimeout(0)

        sock_send = socket.socket(
            socket.AF_INET,  # Internet
            socket.SOCK_DGRAM)

        self.socket = sock
        self.socket_send = sock_send

        self.clients = {k: _addr_conv(self.peer_addr, k) for k in self.peer_addr.keys()}

        return UDPReplicaSendChannel(self), UDPReplicaReceiveChannel(self)

    def poll(self, min_wait):
        r, _, _ = select.select([self.socket], [], [], min_wait)
        return len(r) > 0

    def send(self):
        return 0

    def recv(self):
        rcvd = 0

        for addr, body in _recv_parse_buffer(self.socket):
            self.channel_receive.receive_packet(addr, body)
            rcvd += 1

        return rcvd

    def close(self):
        self.socket.close()
        self.socket_send.close()
