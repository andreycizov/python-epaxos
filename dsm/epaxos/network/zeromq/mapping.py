import logging
from collections import deque

import zmq

from dsm.epaxos.network.mapper import ReplicaSendChannel, ReplicaReceiveChannel
from dsm.epaxos.network.packet import Packet
from dsm.epaxos.network.serializer import serialize_json, deserialize_json
from dsm.epaxos.network.zeromq import impl

logger = logging.getLogger(__name__)


def serialize(packet: Packet):
    repr = serialize_json(packet)
    # repr = zlib.compress(repr)
    return repr


def deserialize(body: bytes):
    # body = zlib.decompress(body)
    return deserialize_json(Packet, body)


class ZMQReplicaReceiveChannel(ReplicaReceiveChannel):
    def __init__(self, server: 'impl.ReplicaServer'):
        self.server = server

    @property
    def replica(self):
        return self.server.replica

    def receive_packet(self, body):
        packet = deserialize(body)
        # print('RCVD', packet.origin, packet.destination, packet.payload)
        self.receive(packet)


class ZMQReplicaSendChannel(ReplicaSendChannel):
    def __init__(self, server: 'impl.ReplicaServer'):
        self.server = server
        self.queue = deque()

    @property
    def peer_id(self):
        return self.server.state.replica_id

    def send_packets(self):
        i = 0
        try:
            while True:
                item = self.queue.popleft()

                x, y = item

                # print(x, y)

                self.server.socket.send_multipart(item, zmq.NOBLOCK, copy=False, track=False)

                # self.server.socket.send(x, zmq.SNDMORE | zmq.NOBLOCK, copy=False, track=False)
                # self.server.socket.send(y, zmq.NOBLOCK, copy=False, track=False)
                i += 1
        except IndexError:
            pass
        return i

    def send_packet(self, packet: Packet):
        # print('SEND', packet.origin, packet.destination, packet.payload)

        bts = serialize(packet)

        self.queue.append([str(packet.destination).encode(), bts])


class ZMQClientSendChannel(ReplicaSendChannel):
    def __init__(self, client: 'impl.ReplicaClient'):
        self.client = client

    @property
    def peer_id(self):
        return self.client.peer_id

    def send_packet(self, packet: Packet):
        bts = serialize(packet)

        self.client.socket.send_multipart([str(packet.destination).encode(), bts], flags=zmq.NOBLOCK, copy=False)
