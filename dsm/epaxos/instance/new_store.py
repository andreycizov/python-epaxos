from typing import NamedTuple, Dict

from dsm.epaxos.instance.new_state import State, Ballot, Slot, Stage


class InstanceStoreState(NamedTuple):
    ballot: Ballot
    state: State


class InstanceStore:
    def __init__(self):
        self.inst = {}  # type: Dict[Slot, InstanceStoreState]

    def update(self, slot: Slot, new: InstanceStoreState):
        old = self.inst.get(slot)

        if old is None:
            old = InstanceStoreState(
                slot.ballot_initial(),
                State(
                    Stage.Prepared,
                    None,
                    -1,
                    []
                )
            )

        # todo: stages are serial and must only go up, so are ballots.
        assert old.ballot <= new.ballot and old.state.stage <= new.state.stage, (slot, old, new)
        # todo: slots must not change their commands unless they are empty
        assert old.state.command is None or old.state.command == new.state.command, (slot, old, new)

        upd = InstanceStoreState(
            new.ballot,
            State(
                new.state.stage,
                new.state.command,
                max([old, new.state.seq]) + 1,
                sorted(set(
                    old.state.deps + new.state.deps
                ))
            )
        )

        self.inst[slot] = upd

        return upd
