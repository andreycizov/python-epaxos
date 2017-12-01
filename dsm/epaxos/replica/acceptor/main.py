import logging
import random
from itertools import groupby
from typing import NamedTuple, Dict, Any, List

from dsm.epaxos.cmd.state import Command, CommandID, Checkpoint
from dsm.epaxos.inst.state import Slot, Stage
from dsm.epaxos.inst.store import between_checkpoints
from dsm.epaxos.net import packet
from dsm.epaxos.net.packet import PACKET_ACCEPTOR
from dsm.epaxos.replica.acceptor.getsizeof import getsize
from dsm.epaxos.replica.acceptor.sub import acceptor_single_ep
from dsm.epaxos.replica.corout import coroutiner, CoExit
from dsm.epaxos.replica.leader.ev import LeaderStart, LeaderExplicitPrepare
from dsm.epaxos.replica.main.ev import Wait, Tick, Reply
from dsm.epaxos.replica.net.ev import Receive
from dsm.epaxos.replica.quorum.ev import Quorum, Configuration
from dsm.epaxos.replica.state.ev import InstanceState, CheckpointEvent

logger = logging.getLogger(__name__)


class AcceptorCoroutine:
    def __init__(
        self,
        quorum: Quorum,
        config: Configuration,
        subs: Dict[Slot, Any] = dict(),
        waiting_for={},
        timeouts_slots: Dict[int, Dict[Slot, bool]] = {},
        slots_timeouts: Dict[Slot, int] = {}
    ):
        self.quorum = quorum
        self.config = config
        self.subs = subs
        self.waiting_for = waiting_for
        self.timeouts_slots = timeouts_slots
        self.slots_timeouts = slots_timeouts
        self.last_cp = None
        self.cp = {}

        self.tick = 0

    def run_sub(self, slot: Slot, payload=None):
        corout = self.subs[slot]
        try:
            req = coroutiner(corout, payload)
            while not isinstance(req, Receive):
                try:
                    rep = yield req
                except BaseException as e:
                    req = corout.throw(e)
                else:
                    req = coroutiner(corout, rep)
            req: Receive
            self.waiting_for[slot] = req.type
        except CoExit:
            if slot in self.waiting_for:
                del self.waiting_for[slot]
            if slot in self.subs:
                del self.subs[slot]
        # except BaseException as e:
        #     corout.throw(e)

    def event(self, x):
        if isinstance(x, packet.Packet) and isinstance(x.payload, PACKET_ACCEPTOR):
            slot = x.payload.slot

            if slot not in self.subs:
                self.subs[slot] = acceptor_single_ep(self.quorum, slot)
                yield from self.run_sub(slot)

            if slot in self.waiting_for:
                rcv = Receive.from_waiting(self.waiting_for.pop(slot), x.payload)
                yield from self.run_sub(slot, (x.origin, rcv))
        elif isinstance(x, InstanceState):
            if x.slot in self.slots_timeouts:
                slot_tick = self.slots_timeouts[x.slot]

                del self.timeouts_slots[slot_tick][x.slot]

                if x.inst.state.stage == Stage.Committed:
                    del self.slots_timeouts[x.slot]

            if x.inst.state.stage < Stage.Committed:
                tick = self.tick + self.config.timeout + random.randint(0, self.config.timeout_range)
                # print(self.quorum.replica_id, 'SET TIMEOUT ', self.tick, tick)

                self.slots_timeouts[x.slot] = tick

                if tick not in self.timeouts_slots:
                    self.timeouts_slots[tick] = {}
                self.timeouts_slots[tick][x.slot] = True

            if x.inst.state.stage >= Stage.Committed:
                if x.slot in self.waiting_for:
                    del self.waiting_for[x.slot]
                if x.slot in self.subs:
                    del self.subs[x.slot]

        elif isinstance(x, Tick):
            self.tick = x.id

            # print(self.quorum.replica_id, self.tick)
            if x.id in self.timeouts_slots:
                to_start = []
                for slot, truth in self.timeouts_slots[x.id].items():
                    to_start.append(slot)
                    del self.slots_timeouts[slot]
                del self.timeouts_slots[x.id]

                for slot in to_start:
                    yield LeaderExplicitPrepare(
                        slot,
                        'TIMEOUT'
                    )

            if x.id % self.config.checkpoint_each == 0:
                checkpoint_id = x.id // self.config.checkpoint_each
                r_idx = sorted(self.quorum.peers + [self.quorum.replica_id]).index(self.quorum.replica_id)

                q_length = self.quorum.full_size

                if checkpoint_id % q_length == r_idx:
                    import sys
                    cp_cmd = Command(
                        CommandID.create(),
                        Checkpoint(
                            checkpoint_id * q_length + r_idx
                        )
                    )
                    print('SIZEOF', getsize(cp_cmd))
                    yield LeaderStart(
                        cp_cmd
                    )

                last_tick = x.id

                # fmtd = '\n'.join(f'\t\t{x.name}: {y}' for x, y in sorted((y, len(list(x))) for y, x in
                #                                                          groupby(sorted([v.state.stage for k, v in
                #                                                                          self.replica.store.inst.items()]))))
                #
                # fmtd3 = '\n'.join(
                #     f'\t\t{x}: {y}' for x, y in sorted([(k, v) for k, v in self.state.packet_counts.items()]))

                # fmtd = ''
                # fmtd3 = ''

                # logger.debug(
                #     f'\n{self.quorum.replica_id}\t{x.id}\n\tInstances:\n{fmtd}\n\tPackets:\n{fmtd3}')
        elif isinstance(x, CheckpointEvent):
            if self.last_cp:
                new_cp = {**self.cp, **self.last_cp.at}
                ctr = 0

                for slot in between_checkpoints(self.cp, new_cp):

                    if slot in self.subs:
                        ctr += 1
                        del self.subs[slot]
                    if slot in self.waiting_for:
                        ctr += 1
                        del self.waiting_for[slot]

                self.cp = new_cp
                logger.error(f'{self.quorum.replica_id} cleaned old things between {ctr}: {self.cp}')

            self.last_cp = x
        else:
            assert False, x

        yield Reply()
