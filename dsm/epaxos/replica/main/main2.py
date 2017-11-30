import logging
from typing import NamedTuple
from uuid import uuid4

from dsm.epaxos.cmd.state import Command, Checkpoint
from dsm.epaxos.inst.state import Slot, Ballot, State, Stage
from dsm.epaxos.inst.store import InstanceStoreState

from dsm.epaxos.net.packet import Packet, PACKET_CLIENT, PACKET_LEADER, PACKET_ACCEPTOR, ClientRequest
from dsm.epaxos.replica.acceptor.main import AcceptorCoroutine
from dsm.epaxos.replica.client.main import ClientsActor
from dsm.epaxos.replica.corout import coroutiner
from dsm.epaxos.replica.leader.ev import LeaderStart
from dsm.epaxos.replica.leader.main import LeaderCoroutine
from dsm.epaxos.replica.main.ev import Reply, Wait, Tick
from dsm.epaxos.replica.net.ev import Send
from dsm.epaxos.replica.net.main import NetActor, ClientNetComm
from dsm.epaxos.replica.config import ReplicaState
from dsm.epaxos.replica.state.ev import LoadCommandSlot, Load, Store, InstanceState
from dsm.epaxos.replica.state.main import StateActor

logger = logging.getLogger(__name__)

STATE_MSGS = (LoadCommandSlot, Load, Store)
STATE_EVENTS = (InstanceState,)
LEADER_MSGS = (LeaderStart,)
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

    trace: bool = True

    def route(self, req, d=0):
        if isinstance(req, STATE_MSGS):
            yield from self.run_sub(self.state, req, d)
        elif isinstance(req, LEADER_MSGS):
            yield from self.run_sub(self.leader, req, d)
        elif isinstance(req, STATE_MSGS):
            yield from self.run_sub(self.state, req, d)
        elif isinstance(req, NET_MSGS):
            yield from self.run_sub(self.net, req, d)
        elif isinstance(req, Reply):
            yield req
        else:
            self._trace(f'Unroutable {req}')
            raise Unroutable(req)

    def _trace(self, *args):
        # print(*args)
        pass

    def run_sub(self, corout, ev, d=1):
        self._trace(' ' * d + 'BEGIN', ev)
        rep = coroutiner(corout, ev)
        self._trace(' ' * (d + 1) + '>', rep)
        prev_rep = None

        # When do we assume that
        while not isinstance(rep, Wait):
            prev_rep = rep

            # reqx = None
            try:
                zzz_iter = self.route(rep, d + 2)
                rep2 = next(zzz_iter)
                while not isinstance(rep2, Reply):
                    rep3 = yield rep2
                    rep2 = zzz_iter.send(rep3)
                reqx = rep2
                assert isinstance(reqx, Reply), reqx
                self._trace(' ' * (d + 1) + '>', 'DONE', reqx)
            except Unroutable as e:
                self._trace(' ' * (d + 1) + '>', 'UNROUTABLE', rep)
                reqx = yield rep
                self._trace(' ' * (d + 1) + '>', 'UNROUTABLE RTN', reqx)

            if not isinstance(reqx, Reply):
                corout.throw(Unroutable(reqx))
            # assert isinstance(reqx, Reply), reqx

            rep = coroutiner(corout, reqx.payload)
            self._trace(' ' * (d + 1) + '>', rep)
        assert isinstance(prev_rep, Reply)

        self._trace(' ' * d + 'END', prev_rep)
        yield Reply(prev_rep.payload)

    def run(self):
        while True:
            ev = yield Wait()

            if isinstance(ev, Tick):
                yield from self.run_sub(self.acceptor, ev)
            elif isinstance(ev, Packet):
                if isinstance(ev.payload, PACKET_CLIENT):
                    yield from self.run_sub(self.clients, ev)
                elif isinstance(ev.payload, PACKET_LEADER):
                    yield from self.run_sub(self.leader, ev)
                elif isinstance(ev.payload, PACKET_ACCEPTOR):
                    yield from self.run_sub(self.acceptor, ev)
                else:
                    assert False, ev
            elif isinstance(ev, STATE_EVENTS):
                yield from self.run_sub(self.clients, ev)
            else:
                assert False, ev


def main():
    st = ReplicaState(
        0,
        0,
        [1, 2, 3, 4],
        [1, 2, 3, 4],
    )

    state = StateActor().run()
    clients = ClientsActor().run()
    leader = LeaderCoroutine(st).run()
    acceptor = AcceptorCoroutine(st).run()
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

    while True:
        x = next(m)

        assert isinstance(x, Wait)

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

        while isinstance(x, ClientNetComm):
            print(x)
            x = m.send(Reply(True))


    print('>>>>>>>>', x)

    x = m.send(
       Reply(True)
    )

    print(next(m))


if __name__ == '__main__':
    main()
