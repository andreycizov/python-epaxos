from typing import NamedTuple, Dict, Any

from dsm.epaxos.inst.state import Slot
from dsm.epaxos.net import packet
from dsm.epaxos.net.packet import PACKET_ACCEPTOR
from dsm.epaxos.replica.acceptor.sub import acceptor_single_ep
from dsm.epaxos.replica.corout import coroutiner, CoExit
from dsm.epaxos.replica.main.ev import Wait
from dsm.epaxos.replica.net.ev import Receive
from dsm.epaxos.replica.state import ReplicaState, Quorum


class AcceptorCoroutine(NamedTuple):
    state: ReplicaState
    subs: Dict[Slot, Any] = {}
    waiting_for = {}

    def run_sub(self, slot: Slot, payload=None):
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

            if isinstance(x, packet.Packet) and isinstance(x.payload, PACKET_ACCEPTOR):
                slot = x.slot

                if slot not in self.subs:
                    self.subs[slot] = acceptor_single_ep(Quorum.from_state(self.state), slot)

                if slot in self.waiting_for:
                    yield from self.run_sub(slot, Receive.from_waiting(self.waiting_for.pop(slot), x.payload))
            else:
                assert False, x