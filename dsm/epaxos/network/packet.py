from dsm.epaxos.instance.state import Slot, Ballot


class BallotPacket:
    def __init__(self, slot: Slot, ballot: Ballot):
        self.slot = slot
        self.ballot = ballot