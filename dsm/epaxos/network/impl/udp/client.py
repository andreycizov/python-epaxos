import random
import socket

import select

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.network.impl.generic.client import ReplicaClient
from dsm.epaxos.network.impl.udp.mapper import UDPClientSendChannel, deserialize
from dsm.epaxos.network.impl.udp.util import _addr_conv, _recv_parse_buffer
from dsm.epaxos.network.peer import Channel


class UDPReplicaClient(ReplicaClient):
    def __init__(
        self,
        *args
    ):
        super().__init__(*args)
        self.blacklisted = []

    def init(self, peer_id: int) -> Channel:
        self.socket = socket.socket(
            socket.AF_INET,  # Internet
            socket.SOCK_DGRAM
        )
        # self.socket.settimeout(0)

        self._replica_id = random.choice(list(self.peer_addr.keys()))

        self.clients = {k: _addr_conv(self.peer_addr, k) for k in self.peer_addr.keys()}

        return UDPClientSendChannel(self)

    @property
    def leader_id(self):
        return self._replica_id

    def connect(self, replica_id=None):
        if replica_id is None:
            if len(self.blacklisted) == len(list(self.peer_addr.keys())):
                self.blacklisted = []

            while replica_id is None or replica_id in self.blacklisted:
                replica_id = random.choice(list(self.peer_addr.keys()))

            # replica_id = list(self.peer_addr.keys())[self.peer_id % len(self.peer_addr)]

        self._replica_id = replica_id

    def poll(self, max_wait) -> bool:
        r, _, _ = select.select([self.socket], [], [], max_wait)
        return len(r) > 0

    def send(self, command: AbstractCommand):
        self.channel.client_request(self.leader_id, command)

    def recv(self):
        addr, body = next(_recv_parse_buffer(self.socket))
        return deserialize(body)

    def close(self):
        pass
        # self.socket.disconnect()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
