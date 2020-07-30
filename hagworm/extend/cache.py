# -*- coding: utf-8 -*-

from cachetools import TTLCache

from hagworm.extend.base import Utils
from hagworm.extend.transaction import Transaction


class StackCache:
    """堆栈缓存

    使用运行内存作为高速缓存，可有效提高并发的处理能力

    """

    def __init__(self, maxsize=0xff, ttl=60):

        self._cache = TTLCache(maxsize, ttl)

    def has(self, key):

        return key in self._cache

    def get(self, key, default=None):

        return self._cache.get(key, default)

    def set(self, key, val):

        self._cache[key] = val

    def incr(self, key, val=1):

        res = self.get(key, 0) + val

        self.set(key, res)

        return res

    def decr(self, key, val=1):

        res = self.get(key, 0) - val

        self.set(key, res)

        return res

    def delete(self, key):

        del self._cache[key]

    def size(self):

        return len(self._cache)

    def clear(self):

        return self._cache.clear()


class PeriodCounter:

    MIN_EXPIRE = 60

    def __init__(self, time_slice, key_prefix=r'', maxsize=0xffff):

        self._time_slice = time_slice
        self._key_prefix = key_prefix

        # 缓存对象初始化，key最小过期时间60秒
        self._cache = StackCache(maxsize, max(time_slice, self.MIN_EXPIRE))

    def _get_key(self, key=None):

        time_period = Utils.math.floor(Utils.timestamp() / self._time_slice)

        if key is None:
            return f'{self._key_prefix}_{time_period}'
        else:
            return f'{self._key_prefix}_{key}_{time_period}'

    def get(self, key=None):

        _key = self._get_key(key)

        return self._cache.get(_key, 0)

    def incr(self, val, key=None):

        _key = self._get_key(key)

        return self._cache.incr(_key, val)

    def incr_with_trx(self, val, key=None):

        _key = self._get_key(key)

        trx = Transaction()
        trx.add_rollback_callback(self._cache.decr, _key, val)

        return self._cache.incr(_key, val), trx

    def decr(self, val, key=None):

        _key = self._get_key(key)

        return self._cache.decr(_key, val)

    def decr_with_trx(self, val, key=None):

        _key = self._get_key(key)

        trx = Transaction()
        trx.add_rollback_callback(self._cache.incr, _key, val)

        return self._cache.decr(_key, val), trx
