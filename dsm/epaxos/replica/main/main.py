import logging
from typing import NamedTuple
from uuid import uuid4

from dsm.epaxos.cmd.state import Command, Checkpoint
from dsm.epaxos.inst.state import Slot, Ballot, State, Stage
from dsm.epaxos.inst.store import InstanceStoreState

from dsm.epaxos.net.packet import Packet, PACKET_CLIENT, PACKET_LEADER, PACKET_ACCEPTOR, ClientRequest
from dsm.epaxos.replica.acceptor.main import AcceptorCoroutine
from dsm.epaxos.replica.client.main import ClientsActor
from dsm.epaxos.replica.corout import coroutiner, CoExit
from dsm.epaxos.replica.leader.ev import LeaderStart, LeaderExplicitPrepare, LeaderStop
from dsm.epaxos.replica.leader.main import LeaderCoroutine
from dsm.epaxos.replica.main.ev import Reply, Wait, Tick
from dsm.epaxos.replica.net.ev import Send
from dsm.epaxos.replica.net.main import NetActor
from dsm.epaxos.replica.config import ReplicaState
from dsm.epaxos.replica.quorum.ev import Quorum, Configuration
from dsm.epaxos.replica.state.ev import LoadCommandSlot, Load, Store, InstanceState
from dsm.epaxos.replica.state.main import StateActor

logger = logging.getLogger(__name__)

STATE_MSGS = (LoadCommandSlot, Load, Store)
STATE_EVENTS = (InstanceState,)
LEADER_MSGS = (LeaderStart, LeaderStop, LeaderExplicitPrepare)
NET_MSGS = (Send,)


class Unroutable(Exception):
    def __init__(self, payload):
        self.payload = payload
        super().__init__(payload)

    pass


class MainCoroutine(NamedTuple):
    state: None
    clients: None
    leader: None
    acceptor: None
    net: None

    trace: bool = False

    def route(self, req, d=0):
        if isinstance(req, STATE_MSGS):
            return self.run_sub(self.state, req, d)
        elif isinstance(req, LEADER_MSGS):
            return self.run_sub(self.leader, req, d)
        elif isinstance(req, STATE_MSGS):
            return self.run_sub(self.state, req, d)
        elif isinstance(req, NET_MSGS):
            return self.run_sub(self.net, req, d)
        elif isinstance(req, STATE_EVENTS):
            self.run_sub(self.clients, req, d)
            self.run_sub(self.acceptor, req, d)
            return Reply(None)
        elif isinstance(req, Reply):
            return req
        else:
            self._trace(f'Unroutable {req}')
            raise Unroutable(req)

    def _trace(self, fn, t=False):
        x = self.trace and t and False
        # x = False
        if x:
            print(*(fn()))

    def run_sub(self, corout, ev, d=1, t=True):
        corout = corout.event(ev)
        self._trace(lambda: (' ' * d + 'BEGIN', ev), t)
        rep = None
        try:
            rep = coroutiner(corout)
            self._trace(lambda: (' ' * (d + 2) + '>', rep), t)

            while not isinstance(rep, Reply):
                try:
                    corout_payload = self.route(rep, d + 4)
                except BaseException as e:
                    rep = corout.throw(e)
                else:
                    rep = coroutiner(corout, corout_payload)
                self._trace(lambda: (' ' * (d + 2) + '|', rep), t)
        except CoExit:
            logger.error('CoExit')

        assert isinstance(rep, Reply), (rep, corout)

        self._trace(lambda: (' ' * d + 'END', rep), t)
        return rep.payload

    def event(self, ev):
        if isinstance(ev, Tick):
            self.run_sub(self.acceptor, ev, 0, False)
            self.run_sub(self.state, ev, 0, False)
        elif isinstance(ev, Packet):
            if isinstance(ev.payload, PACKET_CLIENT):
                self.run_sub(self.clients, ev)
            elif isinstance(ev.payload, PACKET_LEADER):
                self.run_sub(self.leader, ev)
            elif isinstance(ev.payload, PACKET_ACCEPTOR):
                self.run_sub(self.acceptor, ev)
            else:
                assert False, ev
        elif isinstance(ev, STATE_EVENTS):
            self.run_sub(self.clients, ev)
        else:
            assert False, ev


def main():
    q = Quorum(
        [1, 2, 3, 4],
        0,
        0
    )

    c = Configuration(
        1,
        3,
        33,
        10 * 33
    )

    state = StateActor().run()
    clients = ClientsActor().run()
    leader = LeaderCoroutine(q).run()
    acceptor = AcceptorCoroutine(q, c).run()
    net = NetActor().run()

    next(state)
    next(clients)
    next(leader)
    next(acceptor)
    next(net)

    m = MainCoroutine(
        state,
        clients,
        leader,
        acceptor,
        net,
    ).run()

    # print('S', next(m))
    x = next(m)

    while True:

        assert isinstance(x, Wait), x

        x = m.send(
            Packet(
                0,
                0,
                'ClientRequest',
                ClientRequest(
                    Command(
                        uuid4(),
                        Checkpoint(
                            1
                        )
                    )
                )
            )
        )

        print(x)

        # while isinstance(x, ClientNetComm):
        #     print(x)
        #     x = m.send(Reply(True))

    print('>>>>>>>>', x)

    x = m.send(
        Reply(True)
    )

    print(next(m))


if __name__ == '__main__':
    main()
