from dsm.epaxos.inst.state import Slot
from dsm.epaxos.net import packet
from dsm.epaxos.net.packet import PACKET_LEADER
from dsm.epaxos.replica.leader.ev import LeaderStart, LeaderExplicitPrepare
from dsm.epaxos.replica.corout import coroutiner, CoExit
from dsm.epaxos.replica.leader.sub import leader_client_request, leader_explicit_prepare
from dsm.epaxos.replica.main.ev import Wait, Reply
from dsm.epaxos.replica.net.ev import Receive
from dsm.epaxos.replica.quorum.ev import Quorum
from dsm.epaxos.replica.config import ReplicaState


class LeaderCoroutine:
    def __init__(self, state: ReplicaState):
        self.state = state
        self.subs = {}  # type: GEN_T
        self.waiting_for = {}  # type: Dict[Slot, T_sub_payload]
        self.next_instance_id = 0

    def begin_explicit_prepare(self, slot, to_exec=True, reason=None):
        self.store.file_log.write(
            f'3\t{slot}\t{reason}\n')
        self.store.file_log.flush()

    def run_sub(self, slot, payload=None):
        corout = self.subs[slot]
        try:
            req = coroutiner(corout, payload)
            while not isinstance(req, Receive):
                rep = yield req
                req = coroutiner(corout, rep)
            req: Receive
            self.waiting_for[slot] = req.type
        except CoExit:
            del self.waiting_for[slot]
            del self.subs[slot]

    def run(self):
        while True:
            x = yield Wait()

            if isinstance(x, packet.Packet) and isinstance(x.payload, PACKET_LEADER):
                slot = x.payload.slot  # type: Slot

                if slot in self.waiting_for:
                    yield from self.run_sub(slot, Receive.from_waiting(self.waiting_for.pop(slot), x.payload))

            elif isinstance(x, LeaderStart):
                slot = Slot(self.state.replica_id, self.next_instance_id)
                self.next_instance_id += 1

                self.subs[slot] = leader_client_request(Quorum.from_state(self.state), slot, x.command)

                yield from self.run_sub(slot)
                yield Reply(slot)
            elif isinstance(x, LeaderExplicitPrepare):
                prev = self.subs.get(x.slot) is not None

                self.subs[x.slot] = leader_explicit_prepare(Quorum.from_state(self.state), x.slot, x.reason)

                yield from self.run_sub(x.slot)
                yield prev
            else:
                assert False, x