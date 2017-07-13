import logging
from datetime import datetime, timedelta
from itertools import groupby
from typing import NamedTuple, Dict, Tuple

from dsm.epaxos.command.deps.default import DefaultDepsStore
from dsm.epaxos.instance.store import InstanceStore
from dsm.epaxos.network.peer import Channel
from dsm.epaxos.replica.replica import Replica
from dsm.epaxos.replica.state import ReplicaState
from dsm.epaxos.timeout.store import TimeoutStore

logger = logging.getLogger(__name__)


class ReplicaAddress(NamedTuple):
    replica_addr: str


class ReplicaServer:
    def __init__(
        self,
        epoch: int,
        replica_id: int,
        peer_addr: Dict[int, ReplicaAddress],
    ):
        self.peer_addr = peer_addr

        self.channel_send, self.channel_receive = self.init(replica_id)

        state = ReplicaState(
            self.channel_send,
            epoch, replica_id,
            set(peer_addr.keys()),
            set(peer_addr.keys()),
            True,
            5
        )

        deps_store = DefaultDepsStore()
        timeout_store = TimeoutStore(state)
        store = InstanceStore(state, deps_store, timeout_store)

        self.state = state
        self.replica = Replica(state, store)

    def init(self, replica_id: int) -> Tuple[Channel, Channel]:
        raise NotImplementedError()

    def poll(self, min_wait) -> bool:
        """
        Poll the clients and servers, then return `True` if we are ready
        :param min_wait: maximum wait time for the socket
        :return: is the socket ready for reading
        """
        raise NotImplementedError()

    def send(self) -> int:
        """
        If the protocol queues packets insted of sending them right away, then do this now.
        :return: Number of packets sent
        """
        return 0

    def recv(self) -> int:
        raise NotImplementedError()

    def main(self):
        logger.info(f'Replica `{self.state.replica_id}` started.')

        last_tick_time = datetime.now()
        poll_delta = 0.
        last_tick = self.state.ticks

        pkts_rcvd = 0
        pkts_sent = 0

        while True:
            min_wait = self.replica.check_timeouts_minimum_wait()
            min_wait_poll = max(0, self.state.seconds_per_tick - poll_delta)

            if min_wait:
                min_wait = min(min_wait, min_wait_poll)
            else:
                min_wait = min_wait_poll

            poll_result = self.poll(min_wait)
            poll_delta = (datetime.now() - last_tick_time).total_seconds()

            if poll_delta > self.state.seconds_per_tick:
                self.replica.tick()
                self.replica.check_timeouts()
                last_tick_time = last_tick_time + timedelta(seconds=self.state.seconds_per_tick)

            pkts_sent += self.send()

            if poll_result:
                rcvd = self.recv()
                pkts_rcvd += rcvd

                if rcvd:
                    self.replica.execute_pending()
            else:
                pass

            pkts_sent += self.send()

            if self.state.ticks != last_tick and self.state.ticks % (self.state.jiffies * 30) == 0:
                last_tick = self.state.ticks
                print(
                    datetime.now(),
                    self.state.ticks,
                    self.state.seconds_per_tick,
                    self.state.replica_id,
                    sorted((y.name, len(list(x))) for y, x in
                           groupby(sorted([v.type for k, v in self.replica.store.instances.items()]))),
                    # sorted((k, v) for k, v in self.replica.store.executed_cut.items())
                    sorted([(k, v) for k, v in self.state.packet_counts.items()])
                )

    def run(self):
        try:
            self.main()
        finally:
            self.close()

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
