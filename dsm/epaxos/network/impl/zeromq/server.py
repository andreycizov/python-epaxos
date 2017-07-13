from typing import Tuple

import zmq

from dsm.epaxos.network.impl.generic.server import ReplicaServer
from dsm.epaxos.network.impl.zeromq.mapper import ZMQReplicaSendChannel, ZMQReplicaReceiveChannel
from dsm.epaxos.network.peer import Channel


class ZMQReplicaServer(ReplicaServer):
    def init(self, replica_id: int) -> Tuple[Channel, Channel]:
        self.context = zmq.Context(len(self.peer_addr) - 1, shadow=False)

        socket = self.context.socket(zmq.ROUTER)
        socket.bind(self.peer_addr[replica_id].replica_addr)
        socket.setsockopt(zmq.IDENTITY, str(replica_id).encode())
        socket.setsockopt(zmq.ROUTER_HANDOVER, 1)
        socket.setsockopt(zmq.RCVBUF, 2 ** 20)
        socket.setsockopt(zmq.SNDBUF, 2 ** 20)
        socket.sndhwm = 1000000
        socket.rcvhwm = 1000000
        socket.setsockopt(zmq.LINGER, 0)
        socket.hwm = 50
        # socket.setsockopt(zmq.HWM, 20)

        self.poller = zmq.Poller()
        self.poller.register(socket, zmq.POLLIN)
        self.socket = socket

        # logger.info(f'Replica `{replica_id}` connecting.')
        for peer_id, addr in self.peer_addr.items():
            if peer_id == replica_id:
                continue

            self.socket.connect(addr.replica_addr)

        return ZMQReplicaSendChannel(self), ZMQReplicaReceiveChannel(self)

    def poll(self, min_wait):
        poll_result = self.poller.poll(min_wait * 1000.)

        return self.socket in dict(poll_result)

    def send(self):
        return self.channel_send.send_packets()

    def recv(self):
        rcvd = 0
        while True:
            try:
                replica_request = self.socket.recv_multipart(flags=zmq.NOBLOCK)[-1]

                self.channel_receive.receive_packet(replica_request)
                rcvd += 1
            except zmq.ZMQError:
                break
        return rcvd

    def close(self):
        self.socket.close()
        self.context.term()
