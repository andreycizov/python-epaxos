from typing import Optional

from dsm.epaxos.inst.state import Stage, Slot
from dsm.epaxos.inst.store import InstanceStoreState
from dsm.epaxos.net import packet
from dsm.epaxos.net.packet import Packet
from dsm.epaxos.replica.leader.ev import LeaderStart
from dsm.epaxos.replica.main.ev import Wait, Reply
from dsm.epaxos.replica.net.ev import Send
from dsm.epaxos.replica.state.ev import LoadCommandSlot, InstanceState


class ClientsActor:
    def __init__(self):
        self.peers = {}  # type: Dict[int, List[Slot]]
        self.clients = {}

    def event(self, x):
        if isinstance(x, Packet):
            assert isinstance(x.payload, packet.ClientRequest)

            # we may use the clientrequest as a way of keeping the knowledge of whom to reply.
            # client dests are then

            loaded = yield LoadCommandSlot(x.payload.command.id)

            if loaded is None:
                slot = yield LeaderStart(x.payload.command)
            else:
                slot, inst = loaded

                inst: Optional[InstanceStoreState]

                if inst.state.stage >= Stage.Committed:
                    yield Send(
                        x.origin,
                        packet.ClientResponse(
                            inst.state.command
                        )
                    )

            slot: Slot

            self.clients[slot] = x.origin

            if x.origin not in self.peers:
                self.peers[x.origin] = []

            self.peers[x.origin].append(slot)
        elif isinstance(x, InstanceState):
            if x.slot in self.clients and x.inst.state.stage == Stage.Committed:
                # print('REPLY')

                yield Send(
                    self.clients[x.slot],
                    packet.ClientResponse(
                        x.inst.state.command
                    )
                )

                peer = self.peers[self.clients[x.slot]]
                peer.remove(x.slot)

                del self.clients[x.slot]
        else:
            assert False, x
        yield Reply()
