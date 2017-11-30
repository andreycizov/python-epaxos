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
from dsm.epaxos.replica.net.main import NetActor
from dsm.epaxos.replica.config import ReplicaState
from dsm.epaxos.replica.state.ev import LoadCommandSlot, Load, Store, SlotState
from dsm.epaxos.replica.state.main import StateActor

STATE_MSGS = (LoadCommandSlot, Load, Store)
STATE_EVENTS = (SlotState,)
LEADER_MSGS = (LeaderStart,)
NET_MSGS = (Send,)


class MainCoroutine(NamedTuple):
    state: None
    clients: None
    leader: None
    acceptor: None
    net: None

    def route(self, req, d=0):
        if isinstance(req, STATE_MSGS):
            return self.run_sub(self.state, req, d)
        elif isinstance(req, LEADER_MSGS):
            return self.run_sub(self.leader, req, d)
        elif isinstance(req, STATE_MSGS):
            return self.run_sub(self.state, req, d)
        elif isinstance(req, NET_MSGS):
            return self.run_sub(self.net, req, d)
        elif isinstance(req, Reply):
            return req.payload
        else:
            assert False, req

    def run_sub(self, corout, ev, d=1):
        # print(' ' * d + 'BEGIN', ev)
        rep = coroutiner(corout, ev)
        # print(' ' * (d + 1) + '>', rep)
        prev_rep = None
        while not isinstance(rep, Wait):
            prev_rep = rep
            reqx = self.route(rep, d + 2)
            rep = coroutiner(corout, reqx)
            # print(' ' * (d + 1) + '>', rep)
        assert isinstance(prev_rep, Reply)

        # print(' ' * d + 'END', prev_rep)
        return prev_rep.payload

    def run(self):
        while True:
            ev = yield

            if isinstance(ev, Tick):
                self.run_sub(self.acceptor, ev)
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
    st = ReplicaState(
        None,
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



    next(m)

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

    x = m.send(
        SlotState(
            Slot(0, 1),
            InstanceStoreState(
                Ballot(0, 0, 0),
                State(
                    Stage.Committed,
                    None,
                    0,
                    []
                )
            )
        )
    )

    print(x)


if __name__ == '__main__':
    main()
