# -*- coding: utf-8 -*-

import re
import struct
import threading

from io import BytesIO
from collections import OrderedDict
from configparser import RawConfigParser

from .base import Utils
from .error import ConstError


class Result(dict):
    """返回结果类
    """

    def __init__(self, code=0, data=None, extra=None):

        super().__init__(code=code)

        if data is not None:
            self.__setitem__(r'data', data)

        if extra is not None:
            self.__setitem__(r'extra', extra)

    def __bool__(self):

        return self.code == 0

    @property
    def code(self):

        return self.get(r'code')

    @property
    def data(self):

        return self.get(r'data', None)

    @property
    def extra(self):

        return self.get(r'extra', None)


class NullData:
    """NULL类，用于模拟None对象的行为
    """

    def __int__(self):

        return 0

    def __bool__(self):

        return False

    def __float__(self):

        return 0.0

    def __len__(self):

        return 0

    def __repr__(self):

        return r''

    def __eq__(self, obj):

        return not bool(obj)

    def __nonzero__(self):

        return False

    def __cmp__(self, val):

        if val is None:
            return 0
        else:
            return 1


class ThreadList(threading.local):
    """多线程安全的列表
    """

    __slots__ = [r'data']

    def __init__(self):

        self.data = []


class ThreadDict(threading.local):
    """多线程安全的字典
    """

    __slots__ = [r'data']

    def __init__(self):

        self.data = {}


class Const(OrderedDict):
    """常量类
    """

    def __getattr__(self, key):

        return self.__getitem__(key)

    def __setattr__(self, key, val):

        if key[:1] == r'_':
            super().__setattr__(key, val)
        else:
            self.__setitem__(key, val)

    def __delattr__(self, key):

        if key[:1] == r'_':
            super().__delattr__(key)
        else:
            raise ConstError()

    def __setitem__(self, key, val):

        if key in self:
            raise ConstError()
        else:
            super().__setitem__(key, val)

    def __delitem__(self, key):

        raise ConstError()

    def exist(self, val):

        return val in self.values()


class ByteArray(BytesIO):
    """扩展的BytesIO类
    """

    NETWORK = r'!'
    NATIVE = r'='
    NATIVE_ALIGNMENT = r'@'
    LITTLE_ENDIAN = r'<'
    BIG_ENDIAN = r'>'

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self._endian = self.NETWORK

    def get_endian(self):

        return self._endian

    def set_endian(self, val):

        self._endian = val

    def read_pad_byte(self, _len):

        struct.unpack(f'{self._endian}{_len}x', self.read(_len))

    def write_pad_byte(self, _len):

        self.write(struct.pack(f'{self._endian}{_len}x'))

    def read_char(self):

        return struct.unpack(f'{self._endian}c', self.read(1))[0]

    def write_char(self, val):

        self.write(struct.pack(f'{self._endian}c', val))

    def read_signed_char(self):

        return struct.unpack(f'{self._endian}b', self.read(1))[0]

    def write_signed_char(self, val):

        self.write(struct.pack(f'{self._endian}b', val))

    def read_unsigned_char(self):

        return struct.unpack(f'{self._endian}B', self.read(1))[0]

    def write_unsigned_char(self, val):

        self.write(struct.pack(f'{self._endian}B', val))

    def read_bool(self):

        return struct.unpack(f'{self._endian}?', self.read(1))[0]

    def write_bool(self, val):

        self.write(struct.pack(f'{self._endian}?', val))

    def read_short(self):

        return struct.unpack(f'{self._endian}h', self.read(2))[0]

    def write_short(self, val):

        self.write(struct.pack(f'{self._endian}h', val))

    def read_unsigned_short(self):

        return struct.unpack(f'{self._endian}H', self.read(2))[0]

    def write_unsigned_short(self, val):

        self.write(struct.pack(f'{self._endian}H', val))

    def read_int(self):

        return struct.unpack(f'{self._endian}i', self.read(4))[0]

    def write_int(self, val):

        self.write(struct.pack(f'{self._endian}i', val))

    def read_unsigned_int(self):

        return struct.unpack(f'{self._endian}I', self.read(4))[0]

    def write_unsigned_int(self, val):

        self.write(struct.pack(f'{self._endian}I', val))

    def read_long(self):

        return struct.unpack(f'{self._endian}l', self.read(8))[0]

    def write_long(self, val):

        self.write(struct.pack(f'{self._endian}l', val))

    def read_unsigned_long(self):

        return struct.unpack(f'{self._endian}L', self.read(8))[0]

    def write_unsigned_long(self, val):

        self.write(struct.pack(f'{self._endian}L', val))

    def read_long_long(self):

        return struct.unpack(f'{self._endian}q', self.read(8))[0]

    def write_long_long(self, val):

        self.write(struct.pack(f'{self._endian}q', val))

    def read_unsigned_long_long(self):

        return struct.unpack(f'{self._endian}Q', self.read(8))[0]

    def write_unsigned_long_long(self, val):

        self.write(struct.pack(f'{self._endian}Q', val))

    def read_float(self):

        return struct.unpack(f'{self._endian}f', self.read(4))[0]

    def write_float(self, val):

        self.write(struct.pack(f'{self._endian}f', val))

    def read_double(self):

        return struct.unpack(f'{self._endian}d', self.read(8))[0]

    def write_double(self, val):

        self.write(struct.pack(f'{self._endian}d', val))

    def read_bytes(self, _len):

        return struct.unpack(f'{self._endian}{_len}s', self.read(_len))[0]

    def write_bytes(self, val):

        self.write(struct.pack(f'{self._endian}{len(val)}s', val))

    def read_string(self, _len):

        return self.read_bytes(_len).decode()

    def write_string(self, val):

        self.write_bytes(val.encode())

    def read_pascal_bytes(self, _len):

        return struct.unpack(f'{self._endian}{_len}p', self.read(_len))[0]

    def write_pascal_bytes(self, val):

        self.write(struct.pack(f'{self._endian}{len(val)}p', val))

    def read_pascal_string(self, _len):

        return self.read_pascal_bytes(_len).decode()

    def write_pascal_string(self, val):

        self.write_pascal_bytes(val.encode())

    def read_python_int(self, _len):

        return struct.unpack(f'{self._endian}{_len}P', self.read(_len))[0]

    def write_python_int(self, val):

        self.write(struct.pack(f'{self._endian}{len(val)}P', val))


class ConfigParser(RawConfigParser):
    """配置解析类
    """

    def getstr(self, section, option, default=None, **kwargs):

        val = self.get(section, option, **kwargs)

        return val if val else default

    def getjson(self, section, option, **kwargs):

        val = self.get(section, option, **kwargs)

        result = Utils.json_encode(val)

        return result

    def _split_host(self, val):

        if val.find(r':') > 0:
            host, port = val.split(r':', 2)
            return host.strip(), int(port.strip())
        else:
            return None

    def get_split_host(self, section, option, **kwargs):

        val = self.get(section, option, **kwargs)

        return self._split_host(val)

    def get_split_str(self, section, option, sep=r'|', **kwargs):

        val = self.get(section, option, **kwargs)

        return Utils.split_str(val, sep)

    def get_split_int(self, section, option, sep=r',', **kwargs):

        val = self.get(section, option, **kwargs)

        return Utils.split_int(val, sep)

    def split_float(self, val, sep=r','):

        result = tuple(float(item.strip()) for item in val.split(sep))

        return result

    def get_split_float(self, section, option, sep=r',', **kwargs):

        val = self.get(section, option, **kwargs)

        return self.split_float(val, sep)


class Configure(Const):
    """配置类
    """

    def __init__(self):

        super().__init__()

        self._parser = ConfigParser()

    def _init_options(self):

        self.clear()

    def get_option(self, section, option):

        return self._parser.get(section, option)

    def get_options(self, section):

        parser = self._parser

        options = {}

        for option in parser.options(section):
            options[option] = parser.get(section, option)

        return options

    def set_options(self, section, **options):

        if not self._parser.has_section(section):
            self._parser.add_section(section)

        for option, value in options.items():
            self._parser.set(section, option, value)

        self._init_options()

    def read(self, files):

        self._parser.clear()
        self._parser.read(files, r'utf-8')

        self._init_options()

    def read_str(self, val):

        self._parser.clear()
        self._parser.read_string(val)

        self._init_options()

    def read_dict(self, val):

        self._parser.clear()
        self._parser.read_dict(val)

        self._init_options()


class KeyLowerDict(dict):

    _PATTERN = re.compile(r'(?<=[a-z])([A-Z])')

    def __init__(self, _dict):

        super().__init__(
            {
                KeyLowerDict._PATTERN.sub(r'_\1', key).lower(): KeyLowerDict(val) if isinstance(val, dict) else val
                for key, val in _dict.items()
            }
        )
