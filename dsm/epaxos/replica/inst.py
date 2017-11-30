from dsm.epaxos.inst.store import InstanceStore
from dsm.epaxos.replica.acceptor.main import AcceptorCoroutine
from dsm.epaxos.replica.client.main import ClientsActor
from dsm.epaxos.replica.config import ReplicaState
from dsm.epaxos.replica.leader.main import LeaderCoroutine
from dsm.epaxos.replica.main.ev import Tick
from dsm.epaxos.replica.main.main import MainCoroutine
from dsm.epaxos.replica.net.main import NetActor
from dsm.epaxos.replica.state.main import StateActor


class Replica:
    def __init__(self, i_state: ReplicaState):
        self.state = i_state
        self.store = InstanceStore()

        state = StateActor().run()
        clients = ClientsActor().run()
        leader = LeaderCoroutine(i_state).run()
        acceptor = AcceptorCoroutine(i_state).run()
        net = NetActor().run()

        next(state)
        next(clients)
        next(leader)
        next(acceptor)
        next(net)

        self.main = MainCoroutine(
            state,
            clients,
            leader,
            acceptor,
            net
        ).run()

    def checkpoint(self):
        pass

    def tick(self):
        self.state.tick()
        self.main.send(Tick(self.state.ticks))


