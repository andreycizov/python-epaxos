from itertools import count
from typing import Dict, AnyStr

from dsm.epaxos.channel import Channel, Receive, Send, Peer
from dsm.epaxos.packet import ClientRequest
from dsm.epaxos.replica import Replica, logger
from dsm.epaxos.state import Command


class ClientPeer(Peer):
    def __init__(self, id):
        self.id = id

    def receive(self, event: Receive):
        logger.warning(f'Client {self.id} received {event}')


ch_w_11 = Channel()
ch_w_12 = Channel()
ch_w_101 = Channel()
ch_w_102 = Channel()
ch_w_103 = Channel()

ch_r_11 = Channel()
ch_r_12 = Channel()
ch_r_101 = Channel()
ch_r_102 = Channel()
ch_r_103 = Channel()

epoch = 0
cl_11 = 11
cl_12 = 12
repl_1 = 101
repl_2 = 102
repl_3 = 103
q = {repl_1, repl_2, repl_3}

replica_1 = Replica(
    ch_w_101,
    epoch,
    repl_1,
    q,
    q
)

replica_2 = Replica(
    ch_w_102,
    epoch,
    repl_2,
    q,
    q,
)

replica_3 = Replica(
    ch_w_103,
    epoch,
    repl_3,
    q,
    q,
)

client_11 = ClientPeer(cl_11)
client_12 = ClientPeer(cl_12)

comm_1 = Command(1001)
comm_2 = Command(1002)
comm_3 = Command(1003)
comm_4 = Command(1004)
comm_5 = Command(1005)

channel_write_map = {
    cl_11: ch_w_11,
    cl_12: ch_w_12,
    repl_1: ch_w_101,
    repl_2: ch_w_102,
    repl_3: ch_w_103,
}

channel_read_map = {
    cl_11: ch_r_11,
    cl_12: ch_r_12,
    repl_1: ch_r_101,
    repl_2: ch_r_102,
    repl_3: ch_r_103,
}

client_map = {
    cl_11: client_11,
    cl_12: client_12,
    repl_1: replica_1,
    repl_2: replica_2,
    repl_3: replica_3,
}


def plex(channel_write_map, channel_read_map):
    for k, v in channel_write_map.items():
        for item in v.pop_while():
            if isinstance(item, Send):
                print(f'{k}\t->\t{item.peer}\t{item.packet}')
                channel_read_map[item.peer].notify(Receive(k, item.packet))


def process(channel_map: Dict[AnyStr, Channel], client_map: Dict[AnyStr, Peer]):
    r = 0
    for k, v in channel_map.items():
        for item in v.pop_while():
            r += 1
            client_map[k].receive(item)
    return r


ch_w_11.notify(Send(repl_1, ClientRequest(comm_1)))
ch_w_12.notify(Send(repl_3, ClientRequest(comm_2)))
ch_w_12.notify(Send(repl_2, ClientRequest(comm_3)))
ch_w_12.notify(Send(repl_2, ClientRequest(comm_4)))
ch_w_12.notify(Send(repl_2, ClientRequest(comm_5)))

for i in count():
    print(f'RTT: {i + 1}')
    plex(channel_write_map, channel_read_map)
    if process(channel_read_map, client_map) == 0:
        break
