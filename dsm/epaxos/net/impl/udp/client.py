import random
import socket

import select

from dsm.epaxos.cmd.state import Command
from dsm.epaxos.net.impl.generic.client import ReplicaClient
# from dsm.epaxos.net.impl.udp.mapper import UDPClientSendChannel, deserialize
from dsm.epaxos.net.impl.udp.util import _addr_conv, _recv_parse_buffer, create_socket, serialize, deserialize

# from dsm.epaxos.net.peer import Channel
from dsm.epaxos.net.packet import Packet, ClientRequest
from dsm.serializer import deserialize_json


class UDPReplicaClient(ReplicaClient):
    def __init__(
        self,
        *args
    ):
        super().__init__(*args)
        self.socket = create_socket()
        self.replica_addrs = {k: _addr_conv(self.peer_addr[k].replica_addr) for k in self.peer_addr.keys()}

    def poll(self, max_wait) -> bool:
        r, _, _ = select.select([self.socket], [], [], max_wait)
        return len(r) > 0

    def send(self, command: Command):
        payload = ClientRequest(
            command
        )

        packet = Packet(
            self.peer_id,
            self.leader_id,
            payload.__class__.__name__,
            payload
        )

        body = serialize(packet)

        self.socket.sendto(body, self.replica_addrs[packet.destination])

    def recv(self):
        addr, body = next(_recv_parse_buffer(self.socket))
        return deserialize(body)

    def close(self):
        self.socket.disconnect()
