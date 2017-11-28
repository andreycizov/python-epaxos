import json
import typing
from enum import Enum
from functools import lru_cache

import bson

ATOMS = (int, str, bool)


@lru_cache(maxsize=128)
def _generate_type_serializer(t):
    if issubclass(t, Enum):
        return lambda val: val.name
    elif issubclass(t, ATOMS):
        return lambda val: val
    elif issubclass(t, typing.List):
        assert hasattr(t, '__args__')
        serializer = _generate_type_serializer(t.__args__[0])
        return lambda val: [serializer(x) for x in val]
    elif hasattr(t, 'serialize'):
        return t.serialize
    elif hasattr(t, '_fields') and hasattr(t, '_field_types'):
        # NamedTuple meta

        fields = [f for f, _ in t._field_types.items()]
        serializers = {f: _generate_type_serializer(t) for f, t in t._field_types.items()}
        return lambda val: {
            f: serializers[f](getattr(val, f)) for f in fields
        }
    else:
        raise NotImplementedError('')


@lru_cache(maxsize=128)
def _generate_type_deserializer(t):
    if t in ATOMS:
        return lambda val: val
    elif issubclass(t, typing.List):
        assert hasattr(t, '__args__')
        deserializer = _generate_type_deserializer(t.__args__[0])
        return lambda val: [deserializer(x) for x in val]
    elif issubclass(t, Enum):
        return lambda val: t[val]
    elif hasattr(t, 'deserialize'):
        return t.deserialize
    elif hasattr(t, '_field_types'):
        # NamedTuple meta

        fields = [f for f, _ in t._field_types.items()]
        deserializers = {f: _generate_type_deserializer(t) for f, t in t._field_types.items()}
        return lambda val: t(**{
            f: deserializers[f](val[f]) for f in fields
        })
    else:
        raise NotImplementedError(f'{t}, {val}')


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
