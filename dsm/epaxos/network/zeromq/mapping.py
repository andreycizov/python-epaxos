import logging
import zmq

from dsm.epaxos.network.mapper import ReplicaSendChannel, ReplicaReceiveChannel
from dsm.epaxos.network.packet import Packet
from dsm.epaxos.network.serializer import deserialize_namedtuple, serialize_namedtuple
from dsm.epaxos.network.zeromq import impl

logger = logging.getLogger(__name__)


def serialize(packet: Packet):
    repr = serialize_namedtuple(packet)
    # repr = zlib.compress(repr)
    return repr


def deserialize(body: bytes):
    # body = zlib.decompress(body)
    return deserialize_namedtuple(Packet, body)


class ZMQReplicaReceiveChannel(ReplicaReceiveChannel):
    def __init__(self, server: 'impl.ReplicaServer'):
        self.server = server

    @property
    def replica(self):
        return self.server.replica

    def receive_packet(self, body):
        self.receive(deserialize(body))


class ZMQReplicaSendChannel(ReplicaSendChannel):
    def __init__(self, server: 'impl.ReplicaServer'):
        self.server = server

    @property
    def peer_id(self):
        return self.server.state.replica_id

    def send_packet(self, packet: Packet):
        bts = serialize(packet)

        self.server.socket.send_multipart([str(packet.destination).encode(), bts], flags=zmq.NOBLOCK, copy=False)


class ZMQClientSendChannel(ReplicaSendChannel):
    def __init__(self, client: 'impl.ReplicaClient'):
        self.client = client

    @property
    def peer_id(self):
        return self.client.peer_id

    def send_packet(self, packet: Packet):
        bts = serialize(packet)

        self.client.socket.send_multipart([str(packet.destination).encode(), bts], flags=zmq.NOBLOCK, copy=False)
