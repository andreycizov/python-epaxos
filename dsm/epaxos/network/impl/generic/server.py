import contextlib
import logging
import uuid
from datetime import datetime, timedelta
from itertools import groupby
from time import sleep
from typing import NamedTuple, Dict, Tuple

from dsm.epaxos.command.deps.default import DefaultDepsStore
from dsm.epaxos.command.state import Command, Checkpoint
from dsm.epaxos.instance.store import InstanceStore
from dsm.epaxos.network.peer import Channel
from dsm.epaxos.replica.replica import Replica
from dsm.epaxos.replica.state import ReplicaState
from dsm.epaxos.timeout.store import TimeoutStore

logger = logging.getLogger(__name__)


class ReplicaAddress(NamedTuple):
    replica_addr: str


@contextlib.contextmanager
def timeit(fn):
    s = datetime.now()
    yield
    e = datetime.now()
    fn(e - s)


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
            list(peer_addr.keys()),
            list(peer_addr.keys()),
            True
        )

        deps_store = DefaultDepsStore()
        timeout_store = TimeoutStore(state)
        store = InstanceStore(state, deps_store, timeout_store)

        self.state = state
        self.replica = Replica(state, store)
        store.replica = self.replica

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
        last_tick = -1

        pkts_rcvd = 0
        pkts_sent = 0

        last_seen_tick = 0
        cp_ech = 10

        start_time = datetime.now()
        has_slept = False
        should_poll = True

        td_tick = timedelta(seconds=self.state.seconds_per_tick)
        next_tick_time = start_time + td_tick

        logger.info(f'TPS=`{self.state.jiffies}` CP_EVER=`{self.state.jiffies * cp_ech}`')

        ll_tick = last_tick - 1

        def upd_exec(x):
            self.state.total_exec += x.total_seconds()

        def upd_timeouts(x):
            self.state.total_timeouts += x.total_seconds()


        def upd_recv(x):
            self.state.total_recv += x.total_seconds()

        while True:
            loop_start_time = datetime.now()

            to_next_tick = (next_tick_time - loop_start_time).total_seconds()

            min_wait_poll = max([0, to_next_tick * 0.9, td_tick.total_seconds()*0.90])

            if should_poll:
                poll_result = self.poll(min_wait_poll)
                should_poll = True
            else:
                poll_result = True

            loop_poll_time = datetime.now()
            self.state.total_sleep += (loop_poll_time - loop_start_time).total_seconds()



            # print(self.state.ticks)

            assert last_seen_tick == self.state.ticks or last_seen_tick == self.state.ticks - 1, (
                last_seen_tick, self.state.ticks)
            last_seen_tick = self.state.ticks

            # print(self.state.ticks)

            if self.state.ticks != last_tick and self.state.ticks % (self.state.jiffies * cp_ech) == 0:
                checkpoint_id = self.state.ticks // (self.state.jiffies * cp_ech)
                q_length = len(self.state.quorum_full)
                r_idx = self.state.quorum_full.index(self.state.replica_id)

                if checkpoint_id % q_length == r_idx:
                    self.replica.leader.start(
                        Command(
                            uuid.uuid4(),
                            Checkpoint(
                                checkpoint_id * q_length + r_idx
                            )
                        )
                    )

                last_tick = self.state.ticks

                fmtd = '\n'.join(f'\t\t{x.name}: {y}' for x, y in sorted((y, len(list(x))) for y, x in
                       groupby(sorted([v.type for k, v in self.replica.store.instances.items()]))))

                fmtd3 = '\n'.join(f'\t\t{x}: {y}' for x, y in sorted([(k, v) for k, v in self.state.packet_counts.items()]))

                logger.debug(f'\n{self.state.replica_id}\t{self.state.ticks}\n\tInstances:\n{fmtd}\n\tPackets:\n{fmtd3}')


            if loop_poll_time > next_tick_time:
                self.replica.tick()

                with timeit(upd_timeouts):
                    self.replica.check_timeouts()
                next_tick_time = next_tick_time + td_tick

            pkts_sent += self.send()

            if (datetime.now() - start_time).total_seconds() > 20 and self.state.replica_id == 5 and not has_slept:
                logger.info('Sleeping')
                sleep(40)
                logger.info('Sleept')
                has_slept = True

            if poll_result:
                with timeit(upd_recv):
                    should_poll, rcvd = self.recv()
                pkts_rcvd += rcvd

                if rcvd:
                    with timeit(upd_exec):
                        self.replica.execute_pending()
            else:
                pass

            pkts_sent += self.send()


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