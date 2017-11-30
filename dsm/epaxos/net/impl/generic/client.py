import random
from datetime import datetime, timedelta
from typing import Dict

from dsm.epaxos.cmd.state import Command
from dsm.epaxos.net.impl.generic.server import logger
from dsm.epaxos.replica.quorum.ev import ReplicaAddress


class ReplicaClient:
    def __init__(
        self,
        peer_id: int,
        peer_addr: Dict[int, ReplicaAddress],
    ):
        self.peer_id = peer_id
        self.peer_addr = peer_addr

        self.leader_id = None
        self.blacklisted = []

    def connect(self, replica_id=None):
        if replica_id is None:
            if len(self.blacklisted) == len(list(self.peer_addr.keys())):
                self.blacklisted = []

            while replica_id is None or replica_id in self.blacklisted:
                replica_id = random.choice(list(self.peer_addr.keys()))

        self.leader_id = replica_id

    def poll(self, max_wait) -> bool:
        raise NotImplementedError()

    def send(self, command: Command):
        raise NotImplementedError()

    def recv(self):
        raise NotImplementedError()

    def request(self, command: Command, timeout=10, timeout_resend=0.1, retries_max=5):
        # assert self.leader_id is not None

        start = datetime.now()

        while True:
            self.send(command)
            retries = retries_max

            while True:
                poll_result = self.poll(timeout_resend)
                retries -= 1

                if poll_result:
                    rtn = self.recv()
                    # print(f'{self.peer_id} reply {rtn}')
                    # logger.info(f'Client `{self.peer_id}` -> {self.replica_id} Send={command} Recv={rtn.payload}')

                    end = datetime.now()
                    latency = (end - start).total_seconds()
                    return latency, rtn
                elif retries == 0:
                    break
                else:
                    self.send(command)

            # logger.info(f'Client `{self.peer_id}` -> {self.replica_id} RetrySend={command}')
            # self.blacklisted = [self._replica_id]
            # logger.info(f'{self.peer_id} Blacklisted {self._replica_id}')
            self.connect()

    def close(self):
        pass

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
