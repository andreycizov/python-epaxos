from datetime import datetime
from typing import Dict

from dsm.epaxos.command.state import AbstractCommand
from dsm.epaxos.network.impl.generic.server import ReplicaAddress, logger
from dsm.epaxos.network.peer import Channel


class ReplicaClient:
    def __init__(
        self,
        peer_id: int,
        peer_addr: Dict[int, ReplicaAddress],
    ):
        self.peer_id = peer_id
        self.peer_addr = peer_addr

        self.channel = self.init(peer_id)
        self.blacklisted = []

    def init(self, peer_id: int) -> Channel:
        raise NotImplementedError()

    @property
    def leader_id(self):
        raise NotImplementedError()

    def connect(self, replica_id=None):
        raise NotImplementedError()

    def poll(self, max_wait) -> bool:
        raise NotImplementedError()

    def send(self, command: AbstractCommand):
        raise NotImplementedError()

    def recv(self):
        raise NotImplementedError()

    def request(self, command: AbstractCommand, timeout=10):
        assert self.leader_id is not None

        self.send(command)

        start = datetime.now()

        while True:
            poll_result = self.poll(timeout)

            if poll_result:
                rtn = self.recv()
                # logger.info(f'Client `{self.peer_id}` -> {self.replica_id} Send={command} Recv={rtn.payload}')

                end = datetime.now()
                latency = (end - start).total_seconds()
                return latency, rtn
            else:
                # logger.info(f'Client `{self.peer_id}` -> {self.replica_id} RetrySend={command}')
                self.blacklisted = [self._replica_id]
                logger.info(f'{self.peer_id} Blacklisted {self._replica_id}')
                self.connect()
                self.send(command)
