import random
from pprint import pprint
from typing import Tuple, NamedTuple, Dict

import tarjan


class XX(NamedTuple):
    id: str
    keys: Tuple[str, ...]


def X(name, *args: str):
    return XX(name, tuple(args))


IN = [
    X('Wa', 'a'),
    X('Wb', 'b'),
    X('Wab', 'a', 'b'),
    X('Ra', 'a'),
    X('Rb', 'b'),
    X('X0b', 'b'),
]

IN_LAST = [
    X('z', 'www')
]


class Interferences(NamedTuple):
    last_seq: int
    id: str


INTER = {}  # type: Dict[str, Interferences]


def xchange(cmd: XX):
    last_seq = -1
    for x in cmd.keys:
        inter_val = INTER.get(x)

        if inter_val and inter_val.id != cmd.id:
            last_seq = max(last_seq, inter_val.last_seq)

    last_seq = last_seq + 1

    r = []

    for x in cmd.keys:
        inter_val = INTER.get(x)

        if inter_val:
            r.append(inter_val.id)

        INTER[x] = Interferences(
            last_seq,
            cmd.id
        )

    r = [x for x in set(r) if x != cmd.id]

    return last_seq, r


def shuffle(x):
    r = list(x)
    random.shuffle(r)
    return r


def build_deps(population):
    rd = {}
    rs = {}
    for cmd in population:
        seq, deps = xchange(cmd)
        rd[cmd.id] = deps
        rs[cmd.id] = seq
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
