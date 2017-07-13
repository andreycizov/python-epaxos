import socket
from typing import NamedTuple

from  multiprocessing import Process
import time
from urllib.parse import urlparse

COUNT = 200000
ADDR = 'tcp://127.0.0.1:5557'

class Combi(NamedTuple):
    recv: int
    send: int
    should_multipart: bool
    should_multipart_hand: bool


COMBINATIONS = {
    'a': Combi(1, 1, 1, 1)
}


def worker(n):
    c = COMBINATIONS[n]
    addr = urlparse(ADDR)
    UDP_IP = addr.hostname
    UDP_PORT = addr.port

    sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM
    )
    sock.bind((UDP_IP, UDP_PORT))

    first = True
    for task_nbr in range(COUNT):
        data, addr = sock.recvfrom(2)
        if len(data) and first:
            print('\tXXX', data, addr)
            first = False
            # else:
            #     break



def main(n):
    c = COMBINATIONS[n]
    p = Process(target=worker, args=(n,))
    p.start()
    sock = socket.socket(
        socket.AF_INET,  # Internet
        socket.SOCK_DGRAM)  # UDP

    addr = urlparse(ADDR)
    UDP_IP = addr.hostname
    UDP_PORT = addr.port

    payload = b'123'*120

    for num in range(COUNT):
        sock.sendto(payload, (UDP_IP, UDP_PORT))

    return p


if __name__ == "__main__":
    for n in COMBINATIONS.keys():
        print(n, COMBINATIONS[n])
        start_time = time.time()
        p = main(n)
        end_time = time.time()
        duration = end_time - start_time
        msg_per_sec = COUNT / duration

        print("\tDuration: %s" % duration)
        print("\tMessages Per Second: %s" % msg_per_sec)
        p.join()
