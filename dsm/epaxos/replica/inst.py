from dsm.epaxos.inst.store import InstanceStore
from dsm.epaxos.net.packet import Packet
from dsm.epaxos.replica.acceptor.main import AcceptorCoroutine
from dsm.epaxos.replica.client.main import ClientsActor
from dsm.epaxos.replica.config import ReplicaState
from dsm.epaxos.replica.executor.main import ExecutorActor
from dsm.epaxos.replica.leader.main import LeaderCoroutine
from dsm.epaxos.replica.main.ev import Tick, Wait
from dsm.epaxos.replica.main.main import MainCoroutine
from dsm.epaxos.replica.net.main import NetActor
from dsm.epaxos.replica.pingpong.main import PingPongActor
from dsm.epaxos.replica.quorum.ev import Configuration, Quorum
from dsm.epaxos.replica.state.main import StateActor


class Replica:
    def __init__(self, quorum: Quorum, config: Configuration, net_actor: NetActor):
        self.quorum = quorum
        self.store = InstanceStore()

        state = StateActor(self.quorum, self.store)
        clients = ClientsActor(self.quorum)
        leader = LeaderCoroutine(quorum, )
        acceptor = AcceptorCoroutine(quorum, config)
        net = net_actor
        executor = ExecutorActor(self.quorum, self.store)
        pingpong = PingPongActor(self.quorum)

        self.main = MainCoroutine(
            state,
            clients,
            leader,
            acceptor,
            net,
            executor,
            pingpong,
            trace=self.quorum.replica_id == 1
        )

    def packet(self, p: Packet):
        self.main.event(p)

    def tick(self, idx):
        self.main.event(Tick(idx))
