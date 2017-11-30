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


class TransitionException(Exception):
    def __init__(self, curr_inst: InstanceStoreState, new_inst: InstanceStoreState):
        self.inst = curr_inst


class IncorrectBallot(TransitionException):
    pass


class IncorrectStage(TransitionException):
    pass


class IncorrectCommand(TransitionException):
    pass


class LoadResult(NamedTuple):
    exists: bool
    inst: InstanceStoreState


class InstanceStore:
    def __init__(self):
        self.inst = {}  # type: Dict[Slot, InstanceStoreState]
        self.cmd_to_slot = {}  # type: Dict[CommandID, Slot]
        self.deps_cache = KeyedDepsCache()

    def load(self, slot: Slot):
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
            raise IncorrectBallot(old, new)

        if new.state.stage < old.state.stage:
            raise IncorrectStage(old, new)

        if old.state.command is not None and old.state.command != new.state.command:
            raise IncorrectCommand(old, new)

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

        if old.state.command:
            del self.cmd_to_slot[old.state.command.id]

        if new.state.command:
            self.cmd_to_slot[new.state.command.id] = slot

        return old, upd
