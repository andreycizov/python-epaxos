import socket
import struct
from typing import Dict
from urllib.parse import urlparse

from dsm.epaxos.replica.quorum.ev import ReplicaAddress
from dsm.epaxos.net.packet import Packet
from dsm.serializer import serialize_json, deserialize_json


def _addr_conv(my_addr):
    my_addr = urlparse(my_addr)

    udp_ip = my_addr.hostname
    udp_port = my_addr.port

    return udp_ip, udp_port


def _recv_parse_buffer(socket):
    try:
        while True:
            buffer, addr = socket.recvfrom(2 ** 16)

            buffer = memoryview(buffer)

            if len(buffer) < 4:
                break
            size, = struct.unpack('I', buffer[:4])
            if len(buffer) < 4 + size:
                break

            yield addr, buffer[4:size + 4].tobytes()
    except BlockingIOError:
        return


def create_bind(addr, buff_size=2 * 1000 * 1000):
    sock = create_socket()
    sock.bind(_addr_conv(addr))
    sock.settimeout(0)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, buff_size)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, buff_size)
    return sock


def create_socket(buff_size=2 * 1000 * 1000):
    sock_send = socket.socket(
        socket.AF_INET,  # Internet
        socket.SOCK_DGRAM
    )
    sock_send.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, buff_size)
    sock_send.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, buff_size)
    return sock_send


def serialize(packet: Packet):
    bts = serialize_json(packet)
    # bts = zlib.compress(bts)

    len_bts = struct.pack('I', len(bts))

    return len_bts + bts


def deserialize(body: bytes) -> Packet:
    # body = zlib.decompress(body)
    return deserialize_json(Packet, body)
