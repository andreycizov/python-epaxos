from dsm.epaxos.instance.state import Slot, Ballot


class Vote:
    def __init__(self, slot: Slot, ballot: Ballot):
        self.slot = slot
        self.ballot = ballot

    def __repr__(self):
        return f'({self.slot}, {self.ballot})'

