# -*- coding: utf-8 -*-

import aioredis

from aioredis.util import _NOTSET
from aioredis.commands.string import StringCommandsMixin

import aiotask_context as context

from .base import Utils, AsyncContextManager
from .event import DistributedEvent


class RedisPool:

    def __init__(self, address, password=None, *, minsize=8, maxsize=32, db=0, expire=3600, key_prefix=r'', **settings):

        self._pool = None
        self._expire = expire
        self._key_prefix = key_prefix

        self._settings = settings

        self._settings[r'address'] = address
        self._settings[r'password'] = password

        self._settings[r'minsize'] = minsize
        self._settings[r'maxsize'] = maxsize

        self._settings[r'db'] = db

    async def initialize(self):

        self._pool = await aioredis.create_pool(**self._settings)

        Utils.log.info(r'Redis {0} initialized'.format(self._settings[r'address']))

    def get_client(self):

        client = None

        if self._pool is not None:
            client = MCache(self._pool, self._expire, self._key_prefix)

        return client


class RedisDelegate:

    def __init__(self):

        self._redis_pool = None

        self._redis_context_key = r'cache_client_{0}'.format(Utils.uuid1())

    async def async_init_redis(self, *args, **kwargs):

        self._redis_pool = RedisPool(*args, **kwargs)

        await self._redis_pool.initialize()

    async def cache_health(self):

        result = False

        try:

            cache = self._redis_pool.get_client()

            result = bool(await cache.time())

        except Exception as err:

            Utils.log.error(err)

        return result

    def get_cache_client(self, *, alone=False):

        client = None

        if alone:

            client = self._redis_pool.get_client()

        else:

            client = context.get(self._redis_context_key, None)

            if client is None:

                client = self._redis_pool.get_client()

                if client:
                    context.set(self._redis_context_key, client)

        return client

    def share_cache(self, cache, ckey):

        return ShareCache(cache, ckey)

    def event_dispatcher(self, channel_name, channel_count):

        return DistributedEvent(self._redis_pool, channel_name, channel_count)


class MCache(AsyncContextManager, aioredis.Redis):

    def __init__(self, pool, expire, key_prefix):

        aioredis.Redis.__init__(self, None)

        self._pool = pool

        self._expire = expire

        self._key_prefix = key_prefix

    async def execute(self, command, *args, **kwargs):

        if self._pool_or_conn is None and self._pool:

            if self._pool.freesize < 8:
                Utils.log.warning(r'Redis connection pool not enough: {0}'.format(self._pool.freesize))

            self._pool_or_conn = await self._pool.acquire()

        return await super().execute(command, *args, **kwargs)

    async def _context_release(self):

        self.destroy()

    def _val_encode(self, val):

        return Utils.pickle_dumps(val)

    def _val_decode(self, val):

        return Utils.pickle_loads(val)

    def destroy(self):

        if self._pool and self._pool_or_conn:
            self._pool.release(self._pool_or_conn)

        self._pool = None
        self._pool_or_conn = None

    def key(self, key, *args, **kwargs):

        if self._key_prefix:
            key = r'{0}_{1}'.format(self._key_prefix, key)

        if not args and not kwargs:
            return key

        sign = Utils.params_sign(*args, **kwargs)

        return r'{0}_{1}'.format(key, sign)

    def allocate_lock(self, key, expire=60):

        return MLock(self, key, expire)

    def mutex_lock(self, key, expire=60):

        return MutexLock(self, key, expire)

    # OVERRIDE COMMANDS

    async def get(self, key):

        result = await super().get(key)

        if result is not None:
            result = self._val_decode(result)

        return result

    def _get(self, key, *, encoding=_NOTSET):

        return super().get(key, encoding=encoding)

    async def set(self, key, value, expire=0):

        if expire <= 0:
            expire = self._expire

        value = self._val_encode(value)

        result = await super().set(key, value, expire=expire)

        return result

    def _set(self, key, value, *, expire=0, pexpire=0, exist=None):

        return super().set(key, value, expire=expire, pexpire=pexpire, exist=exist)

    async def delete(self, *keys):

        _keys = []

        for key in keys:

            if key.find(r'*') < 0:
                _keys.append(key)
            else:
                _keys.extend((await self.keys(key)))

        result = (await super().delete(*_keys)) if len(_keys) > 0 else 0

        return result

    def _delete(self, key, *keys):

        return super().delete(key, *keys)


class MLock(AsyncContextManager):

    _unlock_script = '''
if redis.call("get",KEYS[1]) == ARGV[1] then
    return redis.call("del",KEYS[1])
else
    return 0
end
'''

    def __init__(self, cache, key, expire):

        self._cache = cache
        self._expire = expire

        self._lock_tag = r'process_lock_{0}'.format(key)
        self._lock_val = Utils.uuid1().encode()

        self._locked = False

    async def _context_release(self):

        await self.release()

    async def _do_acquire(self, repeat):

        params = {
            r'key': self._lock_tag,
            r'value': self._lock_val,
            r'expire': self._expire,
            r'exist': StringCommandsMixin.SET_IF_NOT_EXIST,
        }

        result = await self._cache._set(**params)

        if not result and repeat:

            while not result:

                await Utils.wait_frame()

                result = await self._cache._set(**params)

        return result

    async def _do_wait_unlock(self):

        result = await self._cache.exists(self._lock_tag)

        while result:

            await Utils.wait_frame()

            result = await self._cache.exists(self._lock_tag)

        return False

    async def acquire(self):

        if not self._locked:
            self._locked = await self._do_acquire(True)

        return self._locked

    async def try_lock(self):

        if not self._locked:
            self._locked = await self._do_acquire(False)

        return self._locked

    async def wait_unlock(self):

        if not self._locked:
            self._locked = await self._do_acquire(False)

        if not self._locked:
            await self._do_wait_unlock()

        return self._locked

    async def release(self):

        if self._locked:

            if self._cache:
                await self._cache.eval(self._unlock_script, [self._lock_tag], [self._lock_val])

            self._locked = False

        self._cache = self._lock_tag = None


class MutexLock:

    def __init__(self, cache, key, expire):

        self._cache = cache
        self._expire = expire

        self._lock_tag = r'mutex_lock_{0}'.format(key)
        self._lock_val = Utils.uuid1().encode()

        self._locked = False

    async def acquire(self):

        self._locked = False

        try:

            res = await self._cache._get(self._lock_tag)

            if res:

                if res == self._lock_val:
                    await self._cache.expire(self._lock_tag, self._expire)
                    self._locked = True
                else:
                    self._locked = False

            else:

                params = {
                    r'key': self._lock_tag,
                    r'value': self._lock_val,
                    r'expire': self._expire,
                    r'exist': StringCommandsMixin.SET_IF_NOT_EXIST,
                }

                res = await self._cache._set(**params)

                if res:
                    self._locked = True
                else:
                    self._locked = False

        except Exception as err:

            self._cache.destroy()

            Utils.log.error(err)

        return self._locked


class ShareCache(AsyncContextManager):

    def __init__(self, cache, ckey):

        self._cache = cache
        self._ckey = ckey

        self._locker = None
        self._locked = False

        self.result = None

    async def _context_release(self):

        await self.release()

    async def get(self):

        result = await self._cache.get(self._ckey)

        if result is None:

            self._locker = self._cache.allocate_lock(self._ckey)
            self._locked = await self._locker.wait_unlock()

            if not self._locked:
                result = await self._cache.get(self._ckey)

        return result

    async def set(self, value, expire=0):

        result = await self._cache.set(self._ckey, value, expire)

        return result

    async def release(self):

        if self._locked:

            self._locked = False

            if self._locker:
                await self._locker.release()

        self._cache = self._locker = None
