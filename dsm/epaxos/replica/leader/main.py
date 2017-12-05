import logging

from dsm.epaxos.inst.state import Slot, Stage
from dsm.epaxos.inst.store import between_checkpoints, CheckpointCycle
from dsm.epaxos.net import packet
from dsm.epaxos.net.packet import PACKET_LEADER
from dsm.epaxos.replica.leader.ev import LeaderStart, LeaderExplicitPrepare, LeaderStop
from dsm.epaxos.replica.corout import coroutiner, CoExit
from dsm.epaxos.replica.leader.sub import leader_client_request, leader_explicit_prepare
from dsm.epaxos.replica.main.ev import Wait, Reply
from dsm.epaxos.replica.net.ev import Receive
from dsm.epaxos.replica.quorum.ev import Quorum
from dsm.epaxos.replica.state.ev import InstanceState, CheckpointEvent

logger = logging.getLogger('leader')


class LeaderCoroutine:
    def __init__(self, quorum: Quorum):
        self.quorum = quorum
        self.subs = {}  # type: GEN_T
        self.waiting_for = {}  # type: Dict[Slot, T_sub_payload]
        self.next_instance_id = 0

        self.cp = CheckpointCycle()

    def begin_explicit_prepare(self, slot, to_exec=True, reason=None):
        self.store.file_log.write(
            f'3\t{slot}\t{reason}\n')
        self.store.file_log.flush()

    def run_sub(self, slot, payload=None):
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
        if isinstance(x, packet.Packet) and isinstance(x.payload, PACKET_LEADER):
            slot = x.payload.slot  # type: Slot

            if slot in self.waiting_for:
                yield from self.run_sub(slot, (x.origin, Receive.from_waiting(self.waiting_for.pop(slot), x.payload)))

            yield Reply()
        elif isinstance(x, InstanceState):
            if x.inst.state.stage >= Stage.Committed:
                if x.slot in self.waiting_for:
                    del self.waiting_for[x.slot]
                if x.slot in self.subs:
                    del self.subs[x.slot]
            yield Reply()
        elif isinstance(x, LeaderStart):
            slot = Slot(self.quorum.replica_id, self.next_instance_id)
            self.next_instance_id += 1

            self.subs[slot] = leader_client_request(self.quorum, slot, x.command)

            yield from self.run_sub(slot)
            yield Reply(slot)
        elif isinstance(x, LeaderStop):
            if x.slot in self.waiting_for:
                del self.waiting_for[x.slot]
            if x.slot in self.subs:
                del self.subs[x.slot]
            yield Reply()
        elif isinstance(x, LeaderExplicitPrepare):
            prev = self.subs.get(x.slot) is not None

            self.subs[x.slot] = leader_explicit_prepare(self.quorum, x.slot, x.reason)

            yield from self.run_sub(x.slot)
            yield Reply(prev)
        elif isinstance(x, CheckpointEvent):
            ctr = 0

            for slot in between_checkpoints(*self.cp.cycle(x.at)):
                if slot in self.subs:
                    ctr += 1
                    del self.subs[slot]
                if slot in self.waiting_for:
                    ctr += 1
                    del self.waiting_for[slot]

            logger.error(f'{self.quorum.replica_id} cleaned old things between {ctr}: {self.cp}')

            yield Reply()
        else:
            assert False, x
