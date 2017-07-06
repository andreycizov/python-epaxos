from collections import deque

from dsm.epaxos.packet import Packet


class Event:
    pass


class CommunicationEvent(Event):
    def __init__(self, peer, packet: Packet):
        self.peer = peer
        self.packet = packet

    def __repr__(self):
        return f'{self.__class__.__name__}({self.peer}, {self.packet})'


class Send(CommunicationEvent):
    pass


class Receive(CommunicationEvent):
    pass


class TimeoutEvent(Event):
    def __init__(self, at, payload):
        self.at = at
        self.payload = payload


class TimeoutSet(TimeoutEvent):
    pass


class TimeoutFired(TimeoutEvent):
    pass


class Channel:
    def __init__(self):
        self.items = deque()

    def notify(self, event: Event):
        self.items.append(event)

    def pop_while(self):
        try:
            while True:
                yield self.items.popleft()
        except IndexError:
            return


class Peer:
    def receive(self, event: Receive):
        pass
