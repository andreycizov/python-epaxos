import json
import typing
from enum import Enum
from typing import NamedTuple


def _serialize_type(t, val):
    if isinstance(val, Enum):
        return val.name
    elif isinstance(val, int):
        return val
    elif isinstance(val, str):
        return val
    elif issubclass(t, typing.List):
        assert hasattr(t, '__args__')
        return [_serialize_type(t.__args__[0], x) for x in val]
    elif hasattr(val, '_fields') and hasattr(val, '_field_types'):
        return _serialize_namedtuple(val)
    else:
        return val.__class__.serialize(val)


def _serialize_namedtuple(tpl: NamedTuple):
    if hasattr(tpl.__class__, 'serialize'):
        return tpl.__class__.serialize(tpl)
    else:
        r = {}
        for f, t in tpl._field_types.items():
            val = getattr(tpl, f)

            r[f] = _serialize_type(t, val)
        return r


def serialize_namedtuple(tpl: NamedTuple):
    return json.dumps(_serialize_namedtuple(tpl)).encode()


def _deserialize_type(t, val):
    if t == int:
        return val
    elif t == str:
        return val
    elif issubclass(t, typing.List):
        assert hasattr(t, '__args__')
        return [_deserialize_type(t.__args__[0], x) for x in val]
    elif issubclass(t, Enum):
        return t[val]
    elif hasattr(t, '_field_types'):
        return _deserialize_namedtuple(t, val)
    elif hasattr(t, 'deserialize'):
        return t.deserialize(val)
    else:
        raise NotImplementedError(f'{t}, {val}')


def _deserialize_namedtuple(tpl_cls, json):
    if hasattr(tpl_cls, 'deserialize'):
        return tpl_cls.deserialize(json)
    else:
        kwargs = {}

        for f, t in tpl_cls._field_types.items():
            val = json[f]
            kwargs[f] = _deserialize_type(t, val)

        return tpl_cls(**kwargs)


def deserialize_namedtuple(tpl_cls, body):
    return _deserialize_namedtuple(tpl_cls, json.loads(body.decode()))