import random
from pprint import pprint
from typing import Tuple, NamedTuple, Dict
from uuid import uuid4

import tarjan

from dsm.epaxos.cmd.state import Command, Mutator, Checkpoint
from dsm.epaxos.inst.deps.cache import KeyedDepsCache
from dsm.epaxos.inst.state import Slot

IN = [
    Command(
        uuid4(),
        Mutator(
            'SET',
            [1, 2]
        )
    ),
    Command(
        uuid4(),
        Mutator(
            'SET',
            [1, 2, 4]
        )
    ),
    Command(
        uuid4(),
        Mutator(
            'SET',
            [10]
        )
    ),
    Command(
        uuid4(),
        Checkpoint(5)
    ),
]

IN = [(Slot(0, i), x) for i, x in enumerate(IN)]

IN_LAST = [
    Command(
        uuid4(),
        Checkpoint(6)
    ),
]

IN_LAST = [(Slot(1, i), x) for i, x in enumerate(IN_LAST)]

store = KeyedDepsCache()


def xchange(slot: Slot, cmd: Command):
    return store.xchange(slot, cmd)


def shuffle(x):
    r = list(x)
    random.shuffle(r)
    return r


def build_deps(population):
    rd = {}
    rs = {}
    for slot, cmd in population:
        seq, deps = xchange(slot, cmd)
        rd[slot] = deps
        rs[slot] = seq
    return rd, rs


pop = IN_LAST + shuffle(IN + IN + IN) + IN_LAST

pprint(pop)

rd, rs = build_deps(pop)

pprint(rd)
pprint(rs)

rdt = tarjan.tarjan(rd)

pprint(rdt)

for cc in rdt:
    for c in sorted(cc, key=lambda x: rs[x]):
        print(c)
