from typing import List

import logging

import gevent

from dsm.epaxos.instance.state import StateType, CommittedState, PostPreparedState
from dsm.epaxos.instance.new_state import Slot, Ballot
from dsm.epaxos.instance.store import InstanceStore
from dsm.epaxos.network.packet import Payload, SLOTTED, DivergedResponse
from dsm.epaxos.network.peer import AcceptorInterface, DirectInterface
from dsm.epaxos.replica.abstract import Behaviour
from dsm.epaxos.replica.acceptor_corout import acceptor_main, DepsQuery
# from dsm.epaxos.replica.leader import Leader
from dsm.epaxos.replica.leader_corout import Send, Receive, Load, LoadPostPrepared, StorePostPrepared
from dsm.epaxos.replica.leader_direct import LeaderCoroutine, build_waiting_for
from dsm.epaxos.replica.state import ReplicaState

logger = logging.getLogger(__name__)


class AcceptorCoroutine(Behaviour, DirectInterface):
    def __init__(
        self,
        state: ReplicaState,
        store: InstanceStore,
        leader: LeaderCoroutine,
    ):
        super().__init__(state, store)
        self.leader = leader
        self.instances = {}
        self.waiting_for = {}

    def packet(self, peer: int, payload: Payload):
        if payload.__class__ in SLOTTED:
            slot = payload.slot
            if self.store.is_cut(slot):
                self.state.channel.send(peer, DivergedResponse(slot))
                return

            if slot not in self.instances:
                self.instances[slot] = acceptor_main(self.leader.get_quorum(), payload.slot)
                self.exec(slot)
            self.exec(slot, (peer, build_waiting_for(self.waiting_for.pop(slot), payload)))
        else:
            raise NotImplementedError(
                f'{payload} {payload.__class__.__name__} {isinstance(payload, Slotted)} {payload.__class__.__bases__}')

    def exec(self, slot, send=None):
        while True:
            try:
                to_send = send
                send = None
                nxt = None
                if to_send:
                    nxt = self.instances[slot].send(to_send)
                else:
                    nxt = next(self.instances[slot])

                # logger.debug(f'{self.state.replica_id} {slot} {nxt} {to_send}')

                if nxt is None:
                    pass
                elif isinstance(nxt, Send):
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
                    self.leader._stop_leadership(nxt.slot)
                elif isinstance(nxt, DepsQuery):
                    deps = self.store.deps_store.query(nxt.slot, nxt.command)
                    send = max((self.store[x].seq if isinstance(self.store[x], PostPreparedState) else 0 for x in deps), default=0), deps
                else:
                    assert False, str(nxt)
            except StopIteration:
                # logger.debug(f'{self.state.replica_id} {slot} {nxt} {to_send} << EXIT')
                self.leader._stop_leadership(slot)
                return
            except BaseException as e:
                self.instances[slot].throw(e)

    def check_timeouts_minimum_wait(self):
        return self.store.timeout_store.minimum_wait()

    def check_timeouts(self):
        for slot in self.store.timeout_store.query():
            # logger.debug(f'{self.state.replica_id} explicit prepare {slot}')
            self.leader.begin_explicit_prepare(slot, reason='TIMEOUT')
