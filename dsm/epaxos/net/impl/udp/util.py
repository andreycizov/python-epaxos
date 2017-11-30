import struct
from typing import Dict
from urllib.parse import urlparse

from dsm.epaxos.net.impl.generic.server import ReplicaAddress


def _addr_conv(peer_addr: Dict[int, ReplicaAddress], peer_id):
    my_addr = peer_addr[peer_id].replica_addr
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