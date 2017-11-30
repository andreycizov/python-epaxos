import logging
from itertools import groupby
from typing import NamedTuple, Dict, Any

from dsm.epaxos.cmd.state import Command, CommandID, Checkpoint
from dsm.epaxos.inst.state import Slot
from dsm.epaxos.net import packet
from dsm.epaxos.net.packet import PACKET_ACCEPTOR
from dsm.epaxos.replica.acceptor.sub import acceptor_single_ep
from dsm.epaxos.replica.corout import coroutiner, CoExit
from dsm.epaxos.replica.leader.ev import LeaderStart
from dsm.epaxos.replica.main.ev import Wait, Tick, Reply
from dsm.epaxos.replica.net.ev import Receive
from dsm.epaxos.replica.quorum.ev import Quorum, Configuration

logger = logging.getLogger(__name__)


class AcceptorCoroutine(NamedTuple):
    quorum: Quorum
    config: Configuration
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
        # what does not work here (?)

        while True:
            x = yield Wait()

            if isinstance(x, packet.Packet) and isinstance(x.payload, PACKET_ACCEPTOR):
                x = x.payload

                slot = x.slot

                if slot not in self.subs:
                    self.subs[slot] = acceptor_single_ep(self.quorum, slot)

                if slot in self.waiting_for:
                    yield from self.run_sub(slot, Receive.from_waiting(self.waiting_for.pop(slot), x.payload))

                yield Reply()
            elif isinstance(x, Tick):
                if x.id % self.config.checkpoint_each == 0:
                    checkpoint_id = x.id // self.config.checkpoint_each
                    r_idx = sorted(self.quorum.peers + [self.quorum.replica_id]).index(self.quorum.replica_id)

                    q_length = self.quorum.full_size

                    if checkpoint_id % q_length == r_idx:
                        yield LeaderStart(
                            Command(
                                CommandID.create(),
                                Checkpoint(
                                    checkpoint_id * q_length + r_idx
                                )
                            )
                        )

                    last_tick = x.id

                    # fmtd = '\n'.join(f'\t\t{x.name}: {y}' for x, y in sorted((y, len(list(x))) for y, x in
                    #                                                          groupby(sorted([v.state.stage for k, v in
                    #                                                                          self.replica.store.inst.items()]))))
                    #
                    # fmtd3 = '\n'.join(
                    #     f'\t\t{x}: {y}' for x, y in sorted([(k, v) for k, v in self.state.packet_counts.items()]))

                    fmtd = ''
                    fmtd3 = ''

                    logger.debug(
                        f'\n{self.quorum.replica_id}\t{x.id}\n\tInstances:\n{fmtd}\n\tPackets:\n{fmtd3}')

                yield Reply()
            else:
                assert False, x
