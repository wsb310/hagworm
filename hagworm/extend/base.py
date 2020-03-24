# -*- coding: utf-8 -*-

import os
import re
import sys
import platform
import uuid
import time
import math
import string
import random
import hashlib
import base64
import hmac
import urllib
import textwrap
import itertools
import copy
import pytz
import pickle
import psutil
import functools
import binascii
import json
import zlib
import socket
import unicodedata
import calendar
import jwt
import loguru
import xmltodict
import xml.dom.minidom

from datetime import datetime, timedelta
from contextlib import contextmanager, closing
from collections import Iterable, OrderedDict
from zipfile import ZipFile, ZIP_DEFLATED

from stdnum import luhn
from cacheout import LRUCache


class Utils:
    """基础工具类

    集成常用的工具函数

    """

    _BYTES_TYPES = (bytes, type(None))

    _STRING_TYPES = (str, type(None))

    _FALSE_VALUES = {r'null', r'none', r'nil', r'false', r'0', r'', False, 0}

    SAFE_STRING_BASE = r'2346789BCEFGHJKMPQRTVWXY'

    math = math
    random = random
    textwrap = textwrap
    itertools = itertools

    path = os.path

    log = loguru.logger
    urlparse = urllib.parse

    deepcopy = staticmethod(copy.deepcopy)

    func_partial = staticmethod(functools.partial)

    randint = staticmethod(random.randint)
    randstr = staticmethod(random.sample)

    @classmethod
    def environment(cls):

        return {
            r'python': sys.version,
            r'system': [platform.system(), platform.release(), platform.version(), platform.machine()],
        }

    @classmethod
    def utf8(cls, val):

        if isinstance(val, cls._BYTES_TYPES):
            return val

        if isinstance(val, str):
            return val.encode(r'utf-8')

        raise TypeError(
            r'Expected str, bytes, or None; got %r' % type(val)
        )

    @classmethod
    def basestring(cls, val):

        if isinstance(val, cls._STRING_TYPES):
            return val

        if isinstance(val, bytes):
            return val.decode(r'utf-8')

        raise TypeError(
            r'Expected str, bytes, or None; got %r' % type(val)
        )

    @classmethod
    def json_encode(cls, val):

        return json.dumps(val).replace(r'</', r'<\\/')

    @classmethod
    def json_decode(cls, val):

        return json.loads(cls.basestring(val))

    @classmethod
    def today(cls, origin=False):

        result = datetime.today()

        return result.replace(hour=0, minute=0, second=0, microsecond=0) if origin else result

    @classmethod
    def yesterday(cls, origin=False):

        result = datetime.today() - timedelta(days=1)

        return result.replace(hour=0, minute=0, second=0, microsecond=0) if origin else result

    @classmethod
    def utcnow(cls, origin=False):

        result = datetime.utcnow()

        return result.replace(hour=0, minute=0, second=0, microsecond=0) if origin else result

    @classmethod
    def timestamp(cls, msec=False):

        if msec:
            return int(time.time() * 1000)
        else:
            return int(time.time())

    @classmethod
    def convert_bool(cls, val):

        if isinstance(val, str):
            val = val.lower()

        return val not in cls._FALSE_VALUES

    @classmethod
    def convert_int(cls, val, default=0):

        result = default

        try:
            if not isinstance(val, float):
                result = int(val)
        except BaseException:
            pass

        return result

    @classmethod
    def convert_float(cls, val, default=0.0):

        result = default

        try:
            if not isinstance(val, float):
                result = float(val)
        except BaseException:
            pass

        return result

    @classmethod
    def interval_limit(cls, val, min_val, max_val):

        result = val

        if min_val is not None:
            result = max(result, min_val)

        if max_val is not None:
            result = min(result, max_val)

        return result

    @classmethod
    def split_int(cls, val, sep=r',', minsplit=0, maxsplit=-1):

        result = [int(item.strip()) for item in val.split(sep, maxsplit) if item.strip().isdigit()]

        fill = minsplit - len(result)

        if fill > 0:
            result.extend(0 for _ in range(fill))

        return result

    @classmethod
    def join_int(cls, iterable, sep=r','):

        result = []

        for item in iterable:

            cls = type(item)

            if cls is int:

                result.append(str(item))

            elif cls is str:

                item = item.strip()

                if item.isdigit():
                    result.append(item)

        return sep.join(result)

    @classmethod
    def split_str(cls, val, sep=r'|', minsplit=0, maxsplit=-1):

        if val:
            result = [item.strip() for item in val.split(sep, maxsplit)]
        else:
            result = []

        fill = minsplit - len(result)

        if fill > 0:
            result.extend(r'' for _ in range(fill))

        return result

    @classmethod
    def join_str(cls, iterable, sep=r'|'):

        return sep.join(str(val).replace(sep, r'') for val in iterable)

    @classmethod
    def list_extend(cls, iterable, val):

        if cls.is_iterable(val, True):
            iterable.extend(val)
        else:
            iterable.append(val)

    @classmethod
    def str_len(cls, str_val):

        result = 0

        for val in str_val:

            if unicodedata.east_asian_width(val) in (r'A', r'F', r'W'):
                result += 2
            else:
                result += 1

        return result

    @classmethod
    def sub_str(cls, str_val, length, suffix=r'...'):

        result = []
        strlen = 0

        for val in str_val:

            if unicodedata.east_asian_width(val) in (r'A', r'F', r'W'):
                strlen += 2
            else:
                strlen += 1

            if strlen > length:

                if suffix:
                    result.append(suffix)

                break

            result.append(val)

        return r''.join(result)

    @classmethod
    def re_match(cls, pattern, value):

        result = re.match(pattern, value)

        return True if result else False

    @classmethod
    def randhit(cls, iterable, prob):

        if callable(prob):
            prob = [prob(val) for val in iterable]

        prob_sum = sum(prob)

        if prob_sum > 0:

            prob_hit = 0
            prob_sum = cls.randint(0, prob_sum)

            for index in range(0, len(prob)):

                prob_hit += prob[index]

                if prob_hit >= prob_sum:
                    return iterable[index]

        return cls.random.choice(iterable)

    @classmethod
    def get_host_ip(cls):

        result = None

        with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as _socket:
            _socket.connect((r'8.8.8.8', 53))
            result = _socket.getsockname()[0]

        return result

    @classmethod
    def ip2int(cls, val):

        try:
            return int(binascii.hexlify(socket.inet_aton(val)), 16)
        except socket.error:
            return int(binascii.hexlify(socket.inet_pton(socket.AF_INET6, val)), 16)

    @classmethod
    def int2ip(cls, val):

        try:
            return socket.inet_ntoa(binascii.unhexlify(r'%08x' % val))
        except socket.error:
            return socket.inet_ntop(socket.AF_INET6, binascii.unhexlify(r'%032x' % val))

    @classmethod
    def time2stamp(cls, time_str, format_type=r'%Y-%m-%d %H:%M:%S', timezone=None):

        if timezone is None:
            return int(datetime.strptime(time_str, format_type).timestamp())
        else:
            return int(datetime.strptime(time_str, format_type).replace(tzinfo=pytz.timezone(timezone)).timestamp())

    @classmethod
    def stamp2time(cls, time_int=None, format_type=r'%Y-%m-%d %H:%M:%S', timezone=None):

        if time_int is None:
            time_int = cls.timestamp()

        if timezone is None:
            return time.strftime(format_type, datetime.fromtimestamp(time_int).timetuple())
        else:
            return time.strftime(format_type, datetime.fromtimestamp(time_int, pytz.timezone(timezone)).timetuple())

    @classmethod
    def radix24(cls, val):

        base = cls.SAFE_STRING_BASE

        num = int(val)

        if num <= 0:
            return base[num]

        result = ''

        while num > 0:
            num, rem = divmod(num, 24)
            result = base[rem] + result

        return result

    @classmethod
    def radix36(cls, val, align=0):

        base = string.digits + string.ascii_uppercase

        num = int(val)

        if num <= 0:
            return base[num]

        result = ''

        while num > 0:
            num, rem = divmod(num, 36)
            result = base[rem] + result

        return r'{0:0>{1:d}s}'.format(result, align)

    @classmethod
    def radix62(cls, val, align=0):

        base = string.digits + string.ascii_letters

        num = int(val)

        if num <= 0:
            return base[num]

        result = ''

        while num > 0:
            num, rem = divmod(num, 62)
            result = base[rem] + result

        return r'{0:0>{1:d}s}'.format(result, align)

    @classmethod
    def xml_encode(cls, dict_val, root_tag=r'root'):

        xml_doc = xml.dom.minidom.Document()

        root_node = xml_doc.createElement(root_tag)
        xml_doc.appendChild(root_node)

        def _convert(_doc, _node, _dict):

            for key, val in _dict.items():

                temp = _doc.createElement(key)

                if isinstance(val, dict):
                    _convert(_doc, temp, val)
                else:
                    temp.appendChild(_doc.createTextNode(str(val)))

                _node.appendChild(temp)

        _convert(xml_doc, root_node, dict_val)

        return xml_doc

    @classmethod
    def xml_decode(cls, val):

        return xmltodict.parse(cls.utf8(val))

    @classmethod
    def b32_encode(cls, val, standard=False):

        val = cls.utf8(val)

        result = base64.b32encode(val)

        if not standard:
            result = result.rstrip(b'=')

        return cls.basestring(result)

    @classmethod
    def b32_decode(cls, val, standard=False, for_bytes=False):

        val = cls.utf8(val)

        if not standard:

            num = len(val) % 8

            if num > 0:
                val = val + b'=' * (8 - num)

        result = base64.b32decode(val)

        if for_bytes:
            return result
        else:
            return cls.basestring(result)

    @classmethod
    def b64_encode(cls, val, standard=False):

        val = cls.utf8(val)

        if standard:

            result = base64.b64encode(val)

        else:

            result = base64.urlsafe_b64encode(val)

            result = result.rstrip(b'=')

        return cls.basestring(result)

    @classmethod
    def b64_decode(cls, val, standard=False, for_bytes=False):

        val = cls.utf8(val)

        if standard:

            result = base64.b64decode(val)

        else:

            num = len(val) % 4

            if num > 0:
                val = val + b'=' * (4 - num)

            result = base64.urlsafe_b64decode(val)

        if for_bytes:
            return result
        else:
            return cls.basestring(result)

    @classmethod
    def jwt_encode(cls, val, key):

        result = jwt.encode(val, key)

        return cls.basestring(result)

    @classmethod
    def jwt_decode(cls, val, key):

        val = cls.utf8(val)

        return jwt.decode(val, key)

    @classmethod
    def pickle_dumps(cls, val):

        stream = pickle.dumps(val)

        result = zlib.compress(stream)

        return result

    @classmethod
    def pickle_loads(cls, val):

        stream = zlib.decompress(val)

        result = pickle.loads(stream)

        return result

    @classmethod
    def uuid1(cls, node=None, clock_seq=None):

        return uuid.uuid1(node, clock_seq).hex

    @classmethod
    def crc32(cls, val):

        val = cls.utf8(val)

        return binascii.crc32(val)

    @classmethod
    def md5(cls, val):

        val = cls.utf8(val)

        return hashlib.md5(val).hexdigest()

    @classmethod
    def md5_u32(cls, val):

        val = cls.utf8(val)

        return int(hashlib.md5(val).hexdigest(), 16) >> 96

    @classmethod
    def md5_u64(cls, val):

        val = cls.utf8(val)

        return int(hashlib.md5(val).hexdigest(), 16) >> 64

    @classmethod
    def sha1(cls, val):

        val = cls.utf8(val)

        return hashlib.sha1(val).hexdigest()

    @classmethod
    def sha256(cls, val):

        val = cls.utf8(val)

        return hashlib.sha256(val).hexdigest()

    @classmethod
    def sha512(cls, val):

        val = cls.utf8(val)

        return hashlib.sha512(val).hexdigest()

    @classmethod
    def hmac_md5(cls, key, msg):

        key = cls.utf8(key)
        msg = cls.utf8(msg)

        result = hmac.new(key, msg, r'md5').digest()

        result = base64.b64encode(result)

        return cls.basestring(result)

    @classmethod
    def hmac_sha1(cls, key, msg):

        key = cls.utf8(key)
        msg = cls.utf8(msg)

        result = hmac.new(key, msg, r'sha1').digest()

        result = base64.b64encode(result)

        return cls.basestring(result)

    @classmethod
    def ordered_dict(cls, val=None):

        if val is None:
            return OrderedDict()
        else:
            return OrderedDict(sorted(val.items(), key=lambda x: x[0]))

    @classmethod
    def is_iterable(cls, obj, standard=False):

        if standard:
            result = isinstance(obj, Iterable)
        else:
            result = isinstance(obj, (list, tuple,))

        return result

    @classmethod
    def luhn_valid(cls, val):

        return luhn.is_valid(val)

    @classmethod
    def luhn_sign(cls, val):

        result = None

        if val.isdigit():
            result = val + luhn.calc_check_digit(val)

        return result

    @classmethod
    def identity_card(cls, val):

        result = False

        val = val.strip().upper()

        if len(val) == 18:

            weight_factor = (7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2, 1,)

            verify_list = r'10X98765432'

            check_sum = 0

            for index in range(17):
                check_sum += int(val[index]) * weight_factor[index]

            result = (verify_list[check_sum % 11] == val[17])

        return result

    @classmethod
    def params_join(cls, params, filters=[]):

        if filters:
            params = {key: val for key,
                      val in params.items() if key not in filters}

        return r'&'.join(f'{key}={val}' for key, val in sorted(params.items(), key=lambda x: x[0]))

    @classmethod
    def params_sign(cls, *args, **kwargs):

        result = []

        if args:
            result.extend(str(val) for val in args)

        if kwargs:
            result.append(cls.params_join(kwargs))

        return cls.md5(r'&'.join(result))

    @classmethod
    def get_today_region(cls, today=None):

        if today is None:
            today = cls.today(True)

        start_date = today
        end_date = today + timedelta(days=1)

        start_time = int(time.mktime(start_date.timetuple()))
        end_time = int(time.mktime(end_date.timetuple())) - 1

        return start_time, end_time

    @classmethod
    def get_month_region(cls, today=None):

        if today is None:
            today = cls.today(True)

        start_date = today.replace(day=1)

        _, days_in_month = calendar.monthrange(
            start_date.year, start_date.month)

        end_date = start_date + timedelta(days=days_in_month)

        start_time = int(time.mktime(start_date.timetuple()))
        end_time = int(time.mktime(end_date.timetuple())) - 1

        return start_time, end_time

    @classmethod
    def get_week_region(cls, today=None):

        if today is None:
            today = cls.today(True)

        week_pos = today.weekday()

        start_date = today - timedelta(days=week_pos)
        end_date = today + timedelta(days=(7 - week_pos))

        start_time = int(time.mktime(start_date.timetuple()))
        end_time = int(time.mktime(end_date.timetuple())) - 1

        return start_time, end_time

    @classmethod
    def zip_file(cls, zip_file, *file_paths):

        def _add_to_zip(zf, path, zippath):

            if os.path.isfile(path):

                zf.write(path, zippath, ZIP_DEFLATED)

            elif os.path.isdir(path):

                if zippath:
                    zf.write(path, zippath)

                for nm in os.listdir(path):
                    _add_to_zip(zf, os.path.join(path, nm),
                                os.path.join(zippath, nm))

        with ZipFile(zip_file, r'w') as _zf:

            for path in file_paths:

                zippath = os.path.basename(path)

                if not zippath:
                    zippath = os.path.basename(os.path.dirname(path))
                if zippath in (r'', os.curdir, os.pardir):
                    zippath = r''

                _add_to_zip(_zf, path, zippath)

    @classmethod
    def unzip_file(cls, zip_file, file_paths):

        with ZipFile(zip_file, r'r') as _zf:
            _zf.extractall(file_paths)

    @classmethod
    def get_cpu_percent(cls):

        return psutil.cpu_percent()

    @classmethod
    def kill_process(cls, *args):

        process_names = [val.lower() for val in args]

        for pid in psutil.pids():

            process = psutil.Process(pid)

            if process.name().lower() in process_names:
                process.kill()


class Ignore(Exception):
    """可忽略的异常

    用于with语句块跳出，或者需要跳出多层逻辑的情况

    """

    def __init__(self, data=None, layers=1):

        self._data = data
        self._layers = layers

        if self._data:
            Utils.log.warning(self._data)

    def throw(self):

        if self._layers > 0:
            self._layers -= 1

        return self._layers > 0


@contextmanager
def catch_error():
    """异常捕获

    通过with语句捕获异常，代码更清晰，还可以使用Ignore异常安全的跳出with代码块

    """

    try:

        yield

    except Ignore as err:

        if err.throw():
            raise err

    except Exception as err:

        Utils.log.exception(err)


class HeartbeatChecker:

    def __init__(self, name=None, timeout=60):

        self._name = name if name else Utils.uuid1()
        self._timeout = timeout

        self._heartbeat_time = 0

    def __bool__(self):

        return Utils.timestamp() < (self._heartbeat_time + self._timeout)

    @property
    def name(self):

        return self._name

    @property
    def timeout(self):

        return self._timeout

    @property
    def heartbeat_time(self):

        return self._heartbeat_time

    def refresh(self):

        self._heartbeat_time = Utils.timestamp()


class ContextManager:
    """上下文资源管理器

    子类通过实现_context_release接口，方便的实现with语句管理上下文资源释放

    """

    def __enter__(self):

        return self

    def __exit__(self, exc_type, exc_value, traceback):

        self._context_release()

        if exc_type is Ignore:

            return not exc_value.throw()

        elif exc_value:

            Utils.log.exception(exc_value)

            return True

    def _context_release(self):

        raise NotImplementedError()


class FuncWrapper:
    """函数包装器

    将多个函数包装成一个可调用对象

    """

    def __init__(self):

        self._callables = set()

    def __call__(self, *args, **kwargs):

        for func in self._callables:
            try:
                func(*args, **kwargs)
            except Exception as err:
                Utils.log.error(err)

    @property
    def is_valid(self):

        return len(self._callables) > 0

    def add(self, func):

        if func in self._callables:
            return False
        else:
            self._callables.add(func)
            return True

    def remove(self, func):

        if func in self._callables:
            self._callables.remove(func)
            return True
        else:
            return False


class StackCache:
    """堆栈缓存

    使用运行内存作为高速缓存，可有效提高并发的处理能力

    """

    def __init__(self, maxsize=0xff, ttl=None):

        self._cache = LRUCache(maxsize, ttl)

    def has(self, key):

        return self._cache.has(key)

    def get(self, key, default=None):

        return self._cache.get(key, default)

    def set(self, key, val, ttl=None):

        self._cache.set(key, val, ttl)

    def delete(self, key):

        return self._cache.delete(key)

    def size(self):

        return self._cache.size()
