import logging
from typing import Optional

from dsm.epaxos.inst.state import Stage, Slot
from dsm.epaxos.inst.store import InstanceStoreState
from dsm.epaxos.net import packet
from dsm.epaxos.net.packet import Packet
from dsm.epaxos.replica.leader.ev import LeaderStart
from dsm.epaxos.replica.main.ev import Wait, Reply, Tick
from dsm.epaxos.replica.net.ev import Send
from dsm.epaxos.replica.quorum.ev import Quorum
from dsm.epaxos.replica.state.ev import LoadCommandSlot, InstanceState

logger = logging.getLogger('clients')


class ClientsActor:
    def __init__(self, quorum: Quorum):
        self.quorum = quorum
        self.peers = {}  # type: Dict[int, List[Slot]]
        self.clients = {}

        self.st_starts = 0
        self.st_restarts = 0

    def event(self, x):
        if isinstance(x, Packet):
            assert isinstance(x.payload, packet.ClientRequest)

            # we may use the clientrequest as a way of keeping the knowledge of whom to reply.
            # client dests are then

            loaded = yield LoadCommandSlot(x.payload.command.id)

            if loaded is None:
                self.st_starts += 1
                slot = yield LeaderStart(x.payload.command)
            else:
                self.st_restarts += 1
                slot, inst = loaded

                inst: Optional[InstanceStoreState]

                # logger.error(f'{self.quorum.replica_id} Learned about a new client {x.origin} of slot {slot} {inst.state.command}')

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
        elif isinstance(x, Tick):
            if x.id % 330 == 0:
                logger.error(f'{self.quorum.replica_id} St={self.st_starts}/{self.st_restarts}')
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
