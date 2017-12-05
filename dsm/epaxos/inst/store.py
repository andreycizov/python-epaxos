import logging
from typing import NamedTuple, Dict, Optional, Tuple
from uuid import UUID

from dsm.epaxos.cmd.state import CommandID
from dsm.epaxos.inst.deps.cache import KeyedDepsCache
from dsm.epaxos.inst.state import State, Ballot, Slot, Stage


class InstanceStoreState(NamedTuple):
    ballot: Ballot
    state: State

    def __repr__(self):
        return f'ISS({self.ballot},{self.state})'


logger = logging.getLogger(__name__)


class TransitionException(Exception):
    def __init__(self, slot: Slot, curr_inst: Optional[InstanceStoreState], new_inst: Optional[InstanceStoreState]):
        self.slot = slot
        self.inst = curr_inst


class IncorrectBallot(TransitionException):
    pass


class IncorrectStage(TransitionException):
    pass


class IncorrectCommand(TransitionException):
    pass


class SlotTooOld(TransitionException):
    pass


class LoadResult(NamedTuple):
    exists: bool
    inst: InstanceStoreState


def between_checkpoints(old, new):
    for x in new.keys():
        max_slot = new.get(x, Slot(x, 0))
        low_slot = old.get(x, Slot(x, 0))

        for y in range(low_slot.instance_id, max_slot.instance_id):
            yield Slot(x, y)


CP_T = Dict[int, Slot]


class CheckpointCycle:
    def __init__(self):
        # [ old ][ mid ][ current ]
        self.cp_old = {}  # type: CP_T
        self.cp_mid = {}  # type: CP_T

    def earlier(self, slot: Slot):
        return slot < self.cp_old.get(slot.replica_id, Slot(slot.replica_id, -1))

    def cycle(self, cp: Dict[int, Slot]) -> Tuple[CP_T, CP_T]:
        """
        :param cp: new checkpoint
        :return: range of the recycled checkpoint
        """

        cp_prev_old = self.cp_old
        cp_prev_mid = self.cp_mid
        cp_old = {**self.cp_old, **self.cp_mid}
        cp_mid = {**self.cp_mid, **cp}

        self.cp_old = cp_old
        self.cp_mid = cp_mid

        return cp_prev_old, cp_prev_mid

    def __repr__(self):
        o = sorted(self.cp_old.items())
        m = sorted(self.cp_mid.items())
        return f'CheckpointCycle({o}, {m})'


class InstanceStore:
    def __init__(self):
        self.inst = {}  # type: Dict[Slot, InstanceStoreState]
        self.cmd_to_slot = {}  # type: Dict[CommandID, Slot]
        self.deps_cache = KeyedDepsCache()
        self.cp = CheckpointCycle()

    def set_cp(self, cp: Dict[int, Slot]):
        for slot in between_checkpoints(*self.cp.cycle(cp)):
            if slot in self.inst:
                assert self.inst[slot].state.stage == Stage.Committed, 'Attempt to checkpoint before Commit'
                del self.inst[slot]

    def load(self, slot: Slot):
        if self.cp.earlier(slot):
            raise SlotTooOld(slot, None, None)

        r = self.inst.get(slot)
        exists = True

        if r is None:
            exists = False
            r = InstanceStoreState(
                slot.ballot_initial(),
                State(
                    Stage.Prepared,
                    None,
                    -1,
                    []
                )
            )

        return LoadResult(exists, r)

    def load_cmd_slot(self, id: CommandID) -> Optional[Tuple[Slot, InstanceStoreState]]:
        r = self.cmd_to_slot.get(id)
        if not r:
            return None
        else:
            return r, self.load(r).inst

    def update(self, slot: Slot, new: InstanceStoreState):
        exists, old = self.load(slot)

        if new.ballot < old.ballot:
            raise IncorrectBallot(slot, old, new)

        if new.state.stage < old.state.stage:
            raise IncorrectStage(slot, old, new)

        if old.state.stage > Stage.PreAccepted and old.state.command is not None and old.state.command != new.state.command:
            raise IncorrectCommand(slot, old, new)

        if new.state.stage == Stage.PreAccepted and new.state.command:
            # rethink the command ordering
            seq, deps = self.deps_cache.xchange(slot, new.state.command)

            upd = InstanceStoreState(
                new.ballot,
                State(
                    new.state.stage,
                    new.state.command,
                    max(seq, new.state.seq),
                    sorted(set(new.state.deps + deps))
                )
            )
        else:
            upd = new

        self.inst[slot] = upd

        if exists and old.state.command:
            if old.state.command.id in self.cmd_to_slot:
                del self.cmd_to_slot[old.state.command.id]
            else:
                logger.error(f'Command id {old.state.command} not found in self.cmd_to_slot')

        if new.state.command:
            self.cmd_to_slot[new.state.command.id] = slot

        return old, upd
