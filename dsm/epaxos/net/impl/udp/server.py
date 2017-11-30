import select

from dsm.epaxos.net.impl.generic.server import ReplicaServer
from dsm.epaxos.net.impl.udp.util import _recv_parse_buffer, create_bind, create_socket, deserialize


class UDPReplicaServer(ReplicaServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.socket_server = create_bind(self.peer_addr[self.state.replica_id].replica_addr)
        self.socket_send = create_socket()

    def poll(self, min_wait):
        r, _, _ = select.select([self.socket_server], [], [], min_wait)
        return len(r) > 0

    def send(self):
        return 0

    def recv(self):
        for i, (addr, body) in enumerate(_recv_parse_buffer(self.socket_server)):
            # todo: save addr -> body mapping in here.

            yield addr, deserialize(body)

    def close(self):
        self.socket_server.close()
        self.socket_send.close()
