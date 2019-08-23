# -*- coding: utf-8 -*-

import json
import struct
import threading

from io import BytesIO
from collections import OrderedDict
from tempfile import TemporaryFile
from configparser import RawConfigParser


class Result(dict):

    def __init__(self, code=0, msg=r'', data=None, extra=None):

        super().__init__(code=code, msg=msg)

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
    def msg(self):

        return self.get(r'msg')

    @property
    def data(self):

        return self.get(r'data', None)

    @property
    def extra(self):

        return self.get(r'extra', None)


class NullData:

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

        return bool(obj) == False

    def __nonzero__(self):

        return False

    def __cmp__(self, val):

        if val is None:
            return 0
        else:
            return 1


class ErrorData(NullData):

    __slots__ = [r'data']

    def __init__(self, data=None):

        self.data = data if isinstance(data, str) else str(data)

    def __repr__(self):

        return self.data


class ThreadList(threading.local):

    __slots__ = [r'data']

    def __init__(self):

        self.data = []


class ThreadDict(threading.local):

    __slots__ = [r'data']

    def __init__(self):

        self.data = {}


class Const(OrderedDict):

    class _Predefine(NullData):
        pass

    class _ConstError(TypeError):
        pass

    def __init__(self):

        super().__init__()

    def __getattr__(self, key):

        if key[:1] == r'_':
            return super().__getattr__(key)
        else:
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
            self.__delitem__(key)

    def __setitem__(self, key, val):

        if key in self and not isinstance(self.__getitem__(key), Const._Predefine):
            raise Const._ConstError()
        else:
            super().__setitem__(key, val)

    def __delitem__(self, key):

        raise Const._ConstError()

    def exist(self, val):

        return val in self.values()


class ByteArray(BytesIO):

    NETWORK = r'!'
    NATIVE = r'='
    NATIVE_ALIGNMENT = r'@'
    LITTLE_ENDIAN = r'<'
    BIG_ENDIAN = r'>'

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self._endian = self.NETWORK

    def _fmt_str(self, val):

        return r'{0:s}{1:s}'.format(self._endian, val)

    def get_endian(self):

        return self._endian

    def set_endian(self, val):

        self._endian = val

    def read_pad_byte(self, _len):

        struct.unpack(self._fmt_str(r'{0:d}x'.format(_len)), self.read(_len))

    def write_pad_byte(self, _len):

        self.write(struct.pack(self._fmt_str(r'{0:d}x'.format(_len))))

    def read_char(self):

        return struct.unpack(self._fmt_str(r'c'), self.read(1))[0]

    def write_char(self, val):

        self.write(struct.pack(self._fmt_str(r'c'), val))

    def read_signed_char(self):

        return struct.unpack(self._fmt_str(r'b'), self.read(1))[0]

    def write_signed_char(self, val):

        self.write(struct.pack(self._fmt_str(r'b'), val))

    def read_unsigned_char(self):

        return struct.unpack(self._fmt_str(r'B'), self.read(1))[0]

    def write_unsigned_char(self, val):

        self.write(struct.pack(self._fmt_str(r'B'), val))

    def read_bool(self):

        return struct.unpack(self._fmt_str(r'?'), self.read(1))[0]

    def write_bool(self, val):

        self.write(struct.pack(self._fmt_str(r'?'), val))

    def read_short(self):

        return struct.unpack(self._fmt_str(r'h'), self.read(2))[0]

    def write_short(self, val):

        self.write(struct.pack(self._fmt_str(r'h'), val))

    def read_unsigned_short(self):

        return struct.unpack(self._fmt_str(r'H'), self.read(2))[0]

    def write_unsigned_short(self, val):

        self.write(struct.pack(self._fmt_str(r'H'), val))

    def read_int(self):

        return struct.unpack(self._fmt_str(r'i'), self.read(4))[0]

    def write_int(self, val):

        self.write(struct.pack(self._fmt_str(r'i'), val))

    def read_unsigned_int(self):

        return struct.unpack(self._fmt_str(r'I'), self.read(4))[0]

    def write_unsigned_int(self, val):

        self.write(struct.pack(self._fmt_str(r'I'), val))

    def read_long(self):

        return struct.unpack(self._fmt_str(r'l'), self.read(8))[0]

    def write_long(self, val):

        self.write(struct.pack(self._fmt_str(r'l'), val))

    def read_unsigned_long(self):

        return struct.unpack(self._fmt_str(r'L'), self.read(8))[0]

    def write_unsigned_long(self, val):

        self.write(struct.pack(self._fmt_str(r'L'), val))

    def read_long_long(self):

        return struct.unpack(self._fmt_str(r'q'), self.read(8))[0]

    def write_long_long(self, val):

        self.write(struct.pack(self._fmt_str(r'q'), val))

    def read_unsigned_long_long(self):

        return struct.unpack(self._fmt_str(r'Q'), self.read(8))[0]

    def write_unsigned_long_long(self, val):

        self.write(struct.pack(self._fmt_str(r'Q'), val))

    def read_float(self):

        return struct.unpack(self._fmt_str(r'f'), self.read(4))[0]

    def write_float(self, val):

        self.write(struct.pack(self._fmt_str(r'f'), val))

    def read_double(self):

        return struct.unpack(self._fmt_str(r'd'), self.read(8))[0]

    def write_double(self, val):

        self.write(struct.pack(self._fmt_str(r'd'), val))

    def read_bytes(self, _len):

        return struct.unpack(self._fmt_str(r'{0:d}s'.format(_len)), self.read(_len))[0]

    def write_bytes(self, val):

        self.write(struct.pack(self._fmt_str(r'{0:d}s'.format(len(val))), val))

    def read_string(self, _len):

        return self.read_bytes(_len).decode()

    def write_string(self, val):

        self.write_bytes(val.encode())

    def read_pascal_bytes(self, _len):

        return struct.unpack(self._fmt_str(r'{0:d}p'.format(_len)), self.read(_len))[0]

    def write_pascal_bytes(self, val):

        self.write(struct.pack(self._fmt_str(r'{0:d}p'.format(len(val))), val))

    def read_pascal_string(self, _len):

        return self.read_pascal_bytes(_len).decode()

    def write_pascal_string(self, val):

        self.write_pascal_bytes(val.encode())

    def read_python_int(self, _len):

        return struct.unpack(self._fmt_str(r'{0:d}P'.format(_len)), self.read(_len))[0]

    def write_python_int(self, val):

        self.write(struct.pack(self._fmt_str(r'{0:d}P'.format(len(val))), val))


class ConfigParser(RawConfigParser):

    def getstr(self, section, option, default=None, **kwargs):

        val = self.get(section, option, **kwargs)

        return val if val else default

    def getjson(self, section, option, **kwargs):

        val = self.get(section, option, **kwargs)

        result = json.loads(val)

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

    def _split_str(self, val, sep=r'|'):

        result = tuple(temp.strip() for temp in val.split(sep))

        return result

    def get_split_str(self, section, option, sep=r'|', **kwargs):

        val = self.get(section, option, **kwargs)

        return self._split_str(val, sep)

    def _split_int(self, val, sep=r','):

        result = tuple(int(temp.strip()) for temp in val.split(sep))

        return result

    def get_split_int(self, section, option, sep=r',', **kwargs):

        val = self.get(section, option, **kwargs)

        return self._split_int(val, sep)

    def split_float(self, val, sep=r','):

        result = tuple(float(item.strip()) for item in val.split(sep))

        return result

    def get_split_float(self, section, option, sep=r',', **kwargs):

        val = self.get(section, option, **kwargs)

        return self.split_float(val, sep)


class Configure(Const):

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


class FileBuffer:

    def __init__(self, slice_size=0x20000):

        self._buffers = [TemporaryFile()]

        self._slice_size = slice_size

        self._read_offset = 0

    def write(self, data):

        buffer = self._buffers[-1]

        buffer.seek(0, 2)
        buffer.write(data)
        buffer.flush()

        if buffer.tell() >= self._slice_size:
            self._buffers.append(TemporaryFile())

    def read(self, size=None):

        buffer = self._buffers[0]

        buffer.seek(self._read_offset, 0)

        result = buffer.read(size)

        if len(result) == 0 and len(self._buffers) > 1:
            self._buffers.pop(0).close()
            self._read_offset = 0
        else:
            self._read_offset = buffer.tell()

        return result
