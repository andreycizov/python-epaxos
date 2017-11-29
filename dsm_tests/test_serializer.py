import typing
import unittest

from dsm.serializer import _serialize


class A(typing.NamedTuple):
    a: int


class B(typing.NamedTuple):
    b: int


class UnionNamedTuple(typing.NamedTuple):
    a: typing.Union[A, B]


class NullableNamedTuple(typing.NamedTuple):
    a: typing.Optional[A]


class SerializerTest(unittest.TestCase):
    def test_a(self):
        x = UnionNamedTuple(A(6))
        y = UnionNamedTuple(B(6))
        z = NullableNamedTuple(None)
        w = NullableNamedTuple(A(6))

        print(_serialize(x))
        print(_serialize(y))
        print(_serialize(z))
        print(_serialize(w))
