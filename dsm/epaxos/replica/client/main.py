from dsm.epaxos.inst.state import Stage
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

    def run(self):
        while True:
            x = yield Wait()  # same as doing a Receive on something

            if isinstance(x, Packet):
                assert isinstance(x.payload, packet.ClientRequest)

                # we may use the clientrequest as a way of keeping the knowledge of whom to reply.
                # client dests are then

                slot = yield LoadCommandSlot(x.payload.command.id)

                if slot is None:
                    slot = yield LeaderStart(x.payload.command)

                self.clients[slot] = x.origin

                if x.origin not in self.peers:
                    self.peers[x.origin] = []

                self.peers[x.origin].append(slot)

                yield Reply('ACK')
            elif isinstance(x, InstanceState):
                if x.slot in self.clients and x.inst.state.stage == Stage.Committed:
                    yield Send(
                        self.clients[x.slot],
                        packet.ClientResponse(
                            x.inst.state.command
                        )
                    )

                    peer = self.peers[self.clients[x.slot]]
                    peer.remove(x.slot)

                    print('Done', x.slot, self.clients, x)

                    del self.clients[x.slot]

                yield Reply()
            else:
                assert False, x