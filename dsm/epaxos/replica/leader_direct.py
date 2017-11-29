import logging
from typing import Dict, Generator, Union

from dsm.epaxos.command.state import Command
from dsm.epaxos.instance.state import PostPreparedState, PreAcceptedState
from dsm.epaxos.instance.new_state import Slot
from dsm.epaxos.instance.store import InstanceStore
from dsm.epaxos.network.packet import Payload, ClientRequest, SLOTTED, DivergedResponse
from dsm.epaxos.network.peer import DirectInterface
from dsm.epaxos.replica.abstract import Behaviour
# from dsm.epaxos.replica.leader import LeaderState
from dsm.epaxos.replica.state import ReplicaState

from dsm.epaxos.replica.leader_corout import Send, Receive, Load, LoadPostPrepared, StorePostPrepared, T_sub_payload, \
    LeaderException, ExplicitPrepare, leader_explicit_prepare, leader_client_request, Quorum

# GEN_T = Dict[Slot, Generator[Union[Send, Receive, Load, LoadPostPrepared, StorePostPrepared]]]

logger = logging.getLogger(__name__)


def build_waiting_for(waiting_for, payload):
    return tuple(payload if payload.__class__ == x else None for x in waiting_for)


class LeaderCoroutine(Behaviour, DirectInterface):
    def __init__(
        self,
        state: ReplicaState,
        store: InstanceStore,
    ):
        super().__init__(state, store)
        self.leading = {}  # type: GEN_T
        self.peers = {}
        self.waiting_for = {}  # type: Dict[Slot, T_sub_payload]
        self.next_instance_id = 0

    def get_quorum(self):
        return Quorum(
            [x for x in self.state.quorum_full if x != self.state.replica_id],
            self.state.replica_id,
            self.state.epoch
        )

    def start(self, command: Command):
        slot = Slot(self.state.replica_id, self.next_instance_id)
        self.next_instance_id += 1

        local_deps = self.store.deps_store.query(slot, command)
        slot_seq = max((self.store[x].seq if isinstance(self.store[x], PostPreparedState) else 0 for x in local_deps), default=-1) + 1

        self.leading[slot] = leader_client_request(self.get_quorum(), slot, command, slot_seq, local_deps)

        self.exec(slot)

        return slot

    def packet(self, peer: int, payload: Payload):
        if isinstance(payload, ClientRequest):
            x = self.store.command_to_slot.get(payload.command.id)
            if x:
                self.store.client_rep[x] = peer
                self.store.send_reply(x)
            else:
                slot = self.start(payload.command)
                self.store.client_rep[slot] = peer

        elif isinstance(payload, DivergedResponse):
            raise NotImplementedError('Diverged')
        elif payload.__class__ in SLOTTED:
            if payload.slot in self.waiting_for:
                r = tuple(payload if payload.__class__ == x else None for x in self.waiting_for[payload.slot])

                self.exec(payload.slot, (peer, build_waiting_for(self.waiting_for.pop(payload.slot), payload)))
            else:
                pass
                # logger.error(f'Nobody is waiting for it. {payload.slot}, {payload}')
        else:
            raise NotImplementedError(
                f'{payload} {payload.__class__.__name__} {isinstance(payload, Slotted)} {payload.__class__.__bases__}')

    def begin_explicit_prepare(self, slot, to_exec=True, reason=None):
        self.store.file_log.write(
            f'3\t{slot}\t{reason}\n')
        self.store.file_log.flush()
        self.leading[slot] = leader_explicit_prepare(self.get_quorum(), slot, reason)
        if to_exec:
            self.exec(slot)

    def exec(self, slot, send=None):
        while True:
            try:
                to_send = send
                send = None
                nxt = None
                if to_send:
                    nxt = self.leading[slot].send(to_send)
                else:
                    nxt = next(self.leading[slot])

                if isinstance(nxt, Send):
                    self.state.channel.send(nxt.dest, nxt.payload)
                elif isinstance(nxt, Receive):
                    self.waiting_for[slot] = nxt.type
                    return
                elif isinstance(nxt, Load):
                    send = self.store[nxt.slot]
                elif isinstance(nxt, LoadPostPrepared):
                    if nxt.slot in self.store:
                        send = self.store[nxt.slot]
                        assert isinstance(send, PostPreparedState)
                    else:
                        assert False
                elif isinstance(nxt, StorePostPrepared):
                    self.store[nxt.slot] = nxt.inst
                else:
                    assert False, str(nxt)
            except StopIteration:
                self._stop_leadership(slot)
                return
            except ExplicitPrepare as e:
                self._stop_leadership(slot)
                self.begin_explicit_prepare(slot, False, e.reason)
            except LeaderException as e:
                raise
            except BaseException as e:
                self.leading[slot].throw(e)

    def _stop_leadership(self, slot):
        if slot in self.leading:
            del self.leading[slot]

            if slot in self.waiting_for:
                del self.waiting_for[slot]
