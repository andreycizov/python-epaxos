import json
import typing
import uuid
from enum import Enum
from functools import lru_cache

import bson

ATOMS = (int, str, bool)

T = typing.TypeVar('T')
D = typing.TypeVar('D')
T_ser_actual = typing.Callable[[T], typing.Dict[str, typing.Any]]
T_ser = typing.Callable[[D], T_ser_actual]
T_des = typing.Callable[[typing.Dict[str, typing.Any]], typing.Any]


@lru_cache(maxsize=1024)
def _generate_type_serializer(t):
    if t.__class__.__name__ == '_Union':
        assert hasattr(t, '__args__')
        serializers = []

        for x in t.__args__:
            try:
                serializers.append((x, _generate_type_serializer(x)))
            except:
                raise ValueError(f'{x}')

        def ser(obj):
            for i, (s_t, s) in enumerate(serializers):
                if isinstance(obj, s_t):
                    return [i, s(obj)]
            raise ValueError(f'{t} {obj}')

        return ser
    elif issubclass(t, Enum):
        return lambda val: val.name
    elif issubclass(t, uuid.UUID):
        return lambda val: val.hex
    elif issubclass(t, ATOMS):
        return lambda val: val
    elif issubclass(t, type(None)):
        return lambda _: None

    elif issubclass(t, typing.List):
        assert hasattr(t, '__args__')
        serializer = _generate_type_serializer(t.__args__[0])
        return lambda val: [serializer(x) for x in val]
    elif hasattr(t, 'serializer'):
        return t.serializer(_generate_type_serializer)
    elif hasattr(t, '_fields') and hasattr(t, '_field_types'):
        # NamedTuple meta

        fields = [f for f, _ in t._field_types.items()]
        serializers = {}
        for f, t in t._field_types.items():
            try:
                serializers[f] = _generate_type_serializer(t)
            except:
                raise ValueError(f'{f}')

        def ser(val):
            r = {}
            for f in fields:
                try:
                    r[f] = serializers[f](getattr(val, f))
                except:
                    raise ValueError(f'{t}:{f}:{val}')
            return r

        return ser
    else:
        raise NotImplementedError(f'{t}')


@lru_cache(maxsize=1024)
def _generate_type_deserializer(t):
    if t.__class__.__name__ == '_Union':
        assert hasattr(t, '__args__')
        desers = [_generate_type_deserializer(x) for x in t.__args__]

        def deser(json):
            return desers[json[0]](json[1])

        return deser
    elif t in ATOMS:
        return lambda val: val
    elif issubclass(t, type(None)):
        return lambda _: None
    elif issubclass(t, uuid.UUID):
        return lambda val: uuid.UUID(hex=val)

    elif issubclass(t, typing.List):
        assert hasattr(t, '__args__')
        deserializer = _generate_type_deserializer(t.__args__[0])
        return lambda val: [deserializer(x) for x in val]
    elif issubclass(t, Enum):
        return lambda val: t[val]
    elif hasattr(t, 'deserializer'):
        return t.deserializer(_generate_type_deserializer)
    elif hasattr(t, '_field_types'):
        # NamedTuple meta

        fields = [f for f, _ in t._field_types.items()]
        deserializers = {f: _generate_type_deserializer(t) for f, t in t._field_types.items()}
        return lambda val: t(**{
            f: deserializers[f](val[f]) for f in fields
        })
    else:
        raise NotImplementedError(f'{t}')


def _serialize(val):
    serializer = _generate_type_serializer(val.__class__)
    return serializer(val)


def _deserialize(t, json):
    deserializer = _generate_type_deserializer(t)
    return deserializer(json)


def serialize_json(val):
    return json.dumps(_serialize(val)).encode()


def deserialize_json(t, body):
    return _deserialize(t, json.loads(body.decode()))


def serialize_bson(val):
    return bson.dumps(_serialize(val))


def deserialize_bson(t, body):
    return _deserialize(t, bson.loads(body))
