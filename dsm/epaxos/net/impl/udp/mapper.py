import struct

from dsm.epaxos.net.packet import ClientRequest, Packet
from dsm.serializer import deserialize_json, serialize_json

# from dsm.epaxos.net.impl.udp import server, client

CLIENT_REQUEST = ClientRequest.__name__


def serialize(packet: Packet):
    bts = serialize_json(packet)
    # bts = zlib.compress(bts)

    len_bts = struct.pack('I', len(bts))

    return len_bts + bts


def deserialize(body: bytes) -> Packet:
    # body = zlib.decompress(body)
    return deserialize_json(Packet, body)


class UDPReplicaReceiveChannel(ReplicaReceiveChannel):
    def __init__(self, server: 'server.UDPReplicaServer'):
        self.server = server

    @property
    def replica(self):
        return self.server.replica

    def receive_packet(self, addr, body):
        packet = deserialize(body)

        if packet.type == CLIENT_REQUEST:
            self.server.clients[packet.origin] = addr
        self.receive(packet)


class UDPReplicaSendChannel(ReplicaSendChannel):
    def __init__(self, server: 'server.UDPReplicaServer'):
        self.server = server

    @property
    def peer_id(self):
        return self.server.state.replica_id

    def send_packet(self, packet: Packet):
        # print('SEND', packet.origin, packet.destination, packet.payload)

        body = serialize(packet)


        try:
            self.server.socket_send.sendto(body, self.server.clients[packet.destination])
        except OSError:
            print(f'Failed to send {len(body)} {body}')


class UDPClientSendChannel(ReplicaSendChannel):
    def __init__(self, client: 'client.ReplicaClient'):
        self.client = client

    @property
    def peer_id(self):
        return self.client.peer_id

    def send_packet(self, packet: Packet):
        bts = serialize(packet)

        self.client.socket.sendto(bts, self.client.clients[packet.destination])
