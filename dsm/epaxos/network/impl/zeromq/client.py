import random

import zmq

from dsm.epaxos.command.state import Command
from dsm.epaxos.network.impl.generic.client import ReplicaClient
from dsm.epaxos.network.impl.zeromq.mapper import ZMQClientSendChannel, deserialize
from dsm.epaxos.network.peer import Channel


class ZMQReplicaClient(ReplicaClient):
    def __init__(
        self,
        *args
    ):
        super().__init__(*args)

    def init(self, peer_id: int) -> Channel:
        self.poller = zmq.Poller()

        self.context = zmq.Context()

        socket = self.context.socket(zmq.DEALER)
        socket.setsockopt(zmq.IDENTITY, str(peer_id).encode())
        socket.linger = 0

        self.socket = socket
        self.poller.register(self.socket, zmq.POLLIN)

        self._replica_id = None

        return ZMQClientSendChannel(self)

    @property
    def leader_id(self):
        return self._replica_id

    def connect(self, replica_id=None):
        if self.leader_id is None:
            pass
        else:
            self.socket.disconnect(self.peer_addr[self.leader_id].replica_addr)

        if len(self.blacklisted) == len(list(self.peer_addr.keys())):
            self.blacklisted = []

        while replica_id is None or replica_id in self.blacklisted:
            replica_id = random.choice(list(self.peer_addr.keys()))

        self._replica_id = replica_id

        self.socket.connect(self.peer_addr[replica_id].replica_addr)

    def poll(self, max_wait) -> bool:
        poll_result = dict(self.poller.poll(max_wait * 1000.))
        return self.socket in poll_result

    def send(self, command: Command):
        self.channel.client_request(self.leader_id, command)

    def recv(self):
        payload, = self.socket.recv_multipart()

        return deserialize(payload)

    def close(self):
        self.socket.disconnect(self.peer_addr[self._replica_id].replica_addr)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()