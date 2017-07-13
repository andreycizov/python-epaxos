from typing import NamedTuple

import zmq
from  multiprocessing import Process
import time

COUNT = 200000


class Combi(NamedTuple):
    recv: int
    send: int
    should_multipart: bool
    should_multipart_hand: bool


COMBINATIONS = {
    'a': Combi(zmq.PULL, zmq.PUSH, True, False),
    'a1': Combi(zmq.PULL, zmq.PUSH, False, True),
    'a2': Combi(zmq.PULL, zmq.PUSH, False, False),
    'b': Combi(zmq.ROUTER, zmq.ROUTER, True, False),
    'b1': Combi(zmq.ROUTER, zmq.ROUTER, False, True),
    'b2': Combi(zmq.ROUTER, zmq.ROUTER, False, False),
}


def worker(n):
    c = COMBINATIONS[n]
    context = zmq.Context()
    work_receiver = context.socket(c.recv)
    work_receiver.identity = b'a'
    work_receiver.linger = 0
    work_receiver.connect("tcp://127.0.0.1:5557")
    # work_receiver.send(b'a')

    for task_nbr in range(COUNT):
        message = work_receiver.recv()
        if task_nbr == 0:
            print('\tXXX')
        # else:
        #     break
    work_receiver.close()
    context.term()


def main(n):
    c = COMBINATIONS[n]
    p = Process(target=worker, args=(n,))
    p.start()
    context = zmq.Context()
    ventilator_send = context.socket(c.send)
    ventilator_send.identity = b'asd'
    ventilator_send.linger = 0
    ventilator_send.sndhwm = 10000
    ventilator_send.bind("tcp://127.0.0.1:5557")
    for num in range(COUNT):
        while True:
            try:
                if c.should_multipart:
                    ventilator_send.send_multipart([b'a', b'', "MESSAGE".encode()], flags=zmq.NOBLOCK, copy=False)
                elif c.should_multipart_hand:
                    ventilator_send.send(b'a', flags=zmq.SNDMORE|zmq.NOBLOCK, copy=False)
                    ventilator_send.send(b'', flags=zmq.SNDMORE|zmq.NOBLOCK, copy=False)
                    ventilator_send.send(b'MESSAGE', flags=zmq.NOBLOCK, copy=False)
                else:
                    ventilator_send.send(b'\x01\00aMESSAGE', flags=zmq.NOBLOCK, copy=False)
                break
            except zmq.ZMQError:
                time.sleep(0)

    ventilator_send.close()
    context.__exit__()
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
