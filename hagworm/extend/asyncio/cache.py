# -*- coding: utf-8 -*-

import aioredis

from aioredis.util import _NOTSET
from aioredis.commands.string import StringCommandsMixin
from aioredis.errors import ReplyError, MaxClientsError, AuthError, ReadOnlyError

from contextlib import asynccontextmanager

from .base import Utils, WeakContextVar, AsyncContextManager, AsyncCirculator
from .event import DistributedEvent


REDIS_ERROR_RETRY_COUNT = 0x1f
REDIS_POOL_WATER_LEVEL_WARNING_LINE = 0x08


class RedisPool:
    """Redis连接管理
    """

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

    def __await__(self):

        self._pool = yield from aioredis.create_pool(**self._settings).__await__()

        Utils.log.info(f"Redis {self._settings[r'address']} initialized")

        return self

    def get_client(self):

        client = None

        if self._pool is not None:
            client = CacheClient(self._pool, self._expire, self._key_prefix)

        return client


class RedisDelegate:
    """Redis功能组件
    """

    def __init__(self):

        self._redis_pool = None

        self._cache_client_context = WeakContextVar(f'cache_client_{Utils.uuid1()}')

    async def async_init_redis(self, *args, **kwargs):

        self._redis_pool = await RedisPool(*args, **kwargs)

    async def cache_health(self):

        result = False

        async with self.get_cache_client() as cache:
            result = bool(await cache.time())

        return result

    def get_cache_client(self, *, alone=False):

        client = None

        if alone:

            client = self._redis_pool.get_client()

        else:

            client = self._cache_client_context.get()

            if client is None:

                client = self._redis_pool.get_client()

                if client:
                    self._cache_client_context.set(client)

        return client

    def share_cache(self, cache, ckey):

        return ShareCache(cache, ckey)

    def event_dispatcher(self, channel_name, channel_count):

        return DistributedEvent(self._redis_pool, channel_name, channel_count)


class CacheClient(aioredis.Redis, AsyncContextManager):
    """Redis客户端对象，使用with进行上下文管理

    将连接委托给客户端对象管理，提高了整体连接的使用率

    """

    def __init__(self, pool, expire, key_prefix):

        aioredis.Redis.__init__(self, None)

        self._pool = pool

        self._expire = expire

        self._key_prefix = key_prefix

    async def _init_conn(self):

        global REDIS_POOL_WATER_LEVEL_WARNING_LINE

        if self._pool_or_conn is None and self._pool:

            if self._pool.freesize < REDIS_POOL_WATER_LEVEL_WARNING_LINE:
                Utils.log.warning(
                    f'Redis connection pool not enough: {self._pool.freesize}({self._pool.size}/{self._pool.maxsize})'
                )

            self._pool_or_conn = await self._pool.acquire()

    async def _close_conn(self, discard=False):

        if self._pool and self._pool_or_conn:

            connection, self._pool_or_conn = self._pool_or_conn, None

            if discard:
                connection.close()
                await connection.wait_closed()

            self._pool.release(connection)

    async def _context_release(self):

        await self._close_conn()

    async def release(self):

        await self._close_conn()

    @asynccontextmanager
    async def catch_error(self):

        try:

            yield

        except Exception as err:

            Utils.log.exception(err)

            await self._close_conn(True)

    async def execute(self, command, *args, **kwargs):

        global REDIS_ERROR_RETRY_COUNT

        result = None

        async for times in AsyncCirculator(max_times=REDIS_ERROR_RETRY_COUNT):

            try:

                await self._init_conn()

                result = await super().execute(command, *args, **kwargs)

            except (ReplyError, MaxClientsError, AuthError, ReadOnlyError) as err:

                Utils.log.exception(err)

                await self._close_conn(True)

                raise err

            except Exception as err:

                # 记录异常，如果不重新尝试会继续抛出异常

                Utils.log.exception(err)

                await self._close_conn(True)

                if times >= REDIS_ERROR_RETRY_COUNT:
                    raise err

            else:

                break

        return result

    def _val_encode(self, val):

        return Utils.pickle_dumps(val)

    def _val_decode(self, val):

        return Utils.pickle_loads(val)

    def key(self, key, *args, **kwargs):

        if self._key_prefix:
            key = f'{self._key_prefix}_{key}'

        if not args and not kwargs:
            return key

        sign = Utils.params_sign(*args, **kwargs)

        return f'{key}_{sign}'

    def allocate_lock(self, key, expire=60):

        return MLock(self, key, expire)

    # PUB/SUB COMMANDS

    async def subscribe(self, channel, *channels):

        async with self.catch_error():

            await self._init_conn()

            return await super().subscribe(channel, *channels)

    async def unsubscribe(self, channel, *channels):

        async with self.catch_error():

            await self._init_conn()

            return await super().unsubscribe(channel, *channels)

    async def psubscribe(self, pattern, *patterns):

        async with self.catch_error():

            await self._init_conn()

            return await super().psubscribe(pattern, *patterns)

    async def punsubscribe(self, pattern, *patterns):

        async with self.catch_error():

            await self._init_conn()

            return await super().punsubscribe(pattern, *patterns)

    @property
    def channels(self):

        if self._pool_or_conn is None:
            return None

        return self._pool_or_conn.pubsub_channels

    @property
    def patterns(self):

        if self._pool_or_conn is None:
            return None

        return self._pool_or_conn.pubsub_patterns

    @property
    def in_pubsub(self):

        if self._pool_or_conn is None:
            return None

        return self._pool_or_conn.in_pubsub

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
    """基于Redis实现的分布式锁，使用with进行上下文管理
    """

    _renew_script = '''
if redis.call("get",KEYS[1]) == ARGV[1] and redis.call("ttl",KEYS[1]) > 0 then
    return redis.call("expire",KEYS[1],ARGV[2])
else
    return 0
end
'''

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

        self._lock_tag = f'process_lock_{key}'
        self._lock_val = Utils.uuid1().encode()

        self._locked = False

    @property
    def locked(self):

        return self._locked

    async def _context_release(self):

        await self.release()

    async def exists(self):

        if await self._cache.exists(self._lock_tag):
            return True
        else:
            return False

    async def acquire(self, timeout=0):

        if self._locked:

            await self.renew()

        else:

            params = {
                r'key': self._lock_tag,
                r'value': self._lock_val,
                r'expire': self._expire,
                r'exist': StringCommandsMixin.SET_IF_NOT_EXIST,
            }

            async for _ in AsyncCirculator(timeout):

                if await self._cache._set(**params):
                    self._locked = True

                if self._locked or timeout == 0:
                    break

        return self._locked

    async def wait(self, timeout=0):

        async for _ in AsyncCirculator(timeout):

            if not await self.exists():
                return True
        else:

            return False

    async def renew(self):

        if self._locked:

            if await self._cache.eval(self._renew_script, [self._lock_tag], [self._lock_val, self._expire]):
                self._locked = True
            else:
                self._locked = False

        return self._locked

    async def release(self):

        if self._locked:
            await self._cache.eval(self._unlock_script, [self._lock_tag], [self._lock_val])
            self._locked = False


class ShareCache(AsyncContextManager):
    """共享缓存，使用with进行上下文管理

    基于分布式锁实现的一个缓存共享逻辑，保证在分布式环境下，同一时刻业务逻辑只执行一次，其运行结果会通过缓存被共享

    """

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
            self._locked = await self._locker.acquire()

            if not self._locked:
                await self._locker.wait()
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
