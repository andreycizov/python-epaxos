import select
import socket
from typing import Tuple

from dsm.epaxos.net.impl.generic.server import ReplicaServer
from dsm.epaxos.net.impl.udp.mapper import UDPReplicaReceiveChannel, UDPReplicaSendChannel
from dsm.epaxos.net.impl.udp.util import _addr_conv, _recv_parse_buffer
from dsm.epaxos.net.peer import Channel


class UDPReplicaServer(ReplicaServer):
    def init(self, replica_id: int) -> Tuple[Channel, Channel]:
        sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM
        )
        sock.bind(_addr_conv(self.peer_addr, replica_id))
        sock.settimeout(0)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2000000)

        sock_send = socket.socket(
            socket.AF_INET,  # Internet
            socket.SOCK_DGRAM
        )



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

        for i, (addr, body) in enumerate(_recv_parse_buffer(self.socket)):
            self.channel_receive.receive_packet(addr, body)
            rcvd += 1

            # if i > 40:
            #     return False, rcvd

        return True, rcvd

    def close(self):
        self.socket.close()
        self.socket_send.close()
