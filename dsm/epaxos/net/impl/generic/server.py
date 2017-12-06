import contextlib
import logging
from datetime import datetime, timedelta
from time import sleep
from typing import Dict, Iterable, NamedTuple

from dsm.epaxos.net.packet import Packet
from dsm.epaxos.replica.inst import Replica
from dsm.epaxos.replica.net.main import NetActor
from dsm.epaxos.replica.quorum.ev import Configuration, Quorum, ReplicaAddress

logger = logging.getLogger('cli')

class Timer(NamedTuple):
    s: datetime

    def passed(self):
        return datetime.now() - self.s

@contextlib.contextmanager
def timeit(fn=None):
    s = datetime.now()
    yield Timer(s)
    e = datetime.now()

    if fn:
        fn(e - s)


class Stats:
    def __init__(self):
        self.ticks = 0
        self.total_exec = 0
        self.total_timeouts = 0
        self.total_sleep = 0
        self.total_recv = 0


class ReplicaServer:
    def __init__(
        self,
        epoch: int,
        replica_id: int,
        peer_addr: Dict[int, ReplicaAddress],
    ):
        self.peer_addr = peer_addr
        self.quorum = Quorum(
            [x for x in peer_addr.keys() if x != replica_id],
            replica_id,
            epoch,
            peer_addr
        )

        self.config = Configuration()

        self.net_actor = self.build_net_actor()
        self.replica = Replica(self.quorum, self.config, self.net_actor)
        self.stats = Stats()

    def build_net_actor(self) -> NetActor:
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

    def recv(self) -> Iterable[Packet]:
        raise NotImplementedError()

    def main(self):
        logger.info(f'Replica `{self.quorum.replica_id}` started.')

        last_tick_time = datetime.now()
        poll_delta = 0.
        last_tick = -1

        pkts_rcvd = 0
        pkts_sent = 0

        last_seen_tick = 0
        cp_ech = self.config.checkpoint_each

        start_time = datetime.now()
        has_slept = False
        should_poll = True

        td_tick = timedelta(seconds=self.config.seconds_per_tick)
        next_tick_time = start_time + td_tick

        logger.info(f'TPS=`{self.config.jiffies}` CP_EVER=`{cp_ech}`')

        ll_tick = last_tick - 1

        def upd_exec(x):
            self.stats.total_exec += x.total_seconds()

        def upd_timeouts(x):
            self.stats.total_timeouts += x.total_seconds()

        def upd_recv(x):
            self.stats.total_recv += x.total_seconds()

        while True:
            loop_start_time = datetime.now()

            to_next_tick = (next_tick_time - loop_start_time).total_seconds()

            min_wait_poll = max([0, min([to_next_tick * 0.9, td_tick.total_seconds() * 0.90])])

            assert min_wait_poll < td_tick.total_seconds(), min_wait_poll

            if should_poll:
                poll_result = self.poll(min_wait_poll)
            else:
                poll_result = True

            loop_poll_time = datetime.now()
            self.stats.total_sleep += (loop_poll_time - loop_start_time).total_seconds()

            # print(self.state.ticks)

            assert last_seen_tick == self.stats.ticks or last_seen_tick == self.stats.ticks - 1, (
                last_seen_tick, self.stats.ticks)
            last_seen_tick = self.stats.ticks

            # print(self.state.ticks)

            if loop_poll_time > next_tick_time:
                self.stats.ticks += 1
                self.replica.tick(self.stats.ticks)
                next_tick_time = next_tick_time + td_tick


            pkts_sent += self.send()

            # if (datetime.now() - start_time).total_seconds() > 20 and self.quorum.replica_id == 5 and not has_slept:
            #     logger.info('Sleeping')
            #     sleep(40)
            #     logger.info('Sleept')
            #     has_slept = True

            if poll_result:
                rcvd = 0

                with timeit(upd_recv) as tmr:
                    rcvd_a = 0
                    for i, x in enumerate(self.recv()):

                        # with timeit() as tmr2:
                        self.replica.packet(x)
                            # if tmr2.passed().total_seconds() > 1:
                            #     logger.debug(f'{self.quorum.replica_id} {tmr2.passed()} HW {x}')
                        rcvd_a += 1

                        if (i+1) % 100 == 0:

                            passed = tmr.passed()

                            if passed.total_seconds() > min_wait_poll:
                                # logger.debug(f'{self.quorum.replica_id} {passed.total_seconds()} HW {rcvd_a}')
                                break

                    rcvd += rcvd_a

                pkts_rcvd += rcvd
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
