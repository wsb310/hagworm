# -*- coding: utf-8 -*-

import aioredis

from aioredis.util import _NOTSET
from aioredis.commands.string import StringCommandsMixin
from aioredis.commands.transaction import Pipeline, MultiExec
from aioredis.errors import ReplyError, MaxClientsError, AuthError, ReadOnlyError

from .base import Utils, WeakContextVar, AsyncContextManager, AsyncCirculator
from .event import DistributedEvent
from .ntp import NTPClient
from .transaction import Transaction


REDIS_ERROR_RETRY_COUNT = 0x1f
REDIS_POOL_WATER_LEVEL_WARNING_LINE = 0x08


class RedisPool:
    """Redis连接管理
    """

    def __init__(self, address, password=None, *, minsize=8, maxsize=32, db=0, expire=0, key_prefix=None, **settings):

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

        Utils.log.info(f"Redis {self._settings[r'address']} initialized: {self._pool.size}/{self._pool.maxsize}")

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

    @property
    def redis_pool(self):

        return self._redis_pool

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

    def period_counter(self, time_slice: int, key_prefix: str = r'', ntp_client: NTPClient = None):

        return PeriodCounter(self._redis_pool, time_slice, key_prefix, ntp_client)


class CacheClient(aioredis.Redis, AsyncContextManager):
    """Redis客户端对象，使用with进行上下文管理

    将连接委托给客户端对象管理，提高了整体连接的使用率

    """

    def __init__(self, pool, expire, key_prefix):

        super().__init__(None)

        self._pool = pool

        self._expire = expire

        self._key_prefix = key_prefix

    async def _init_conn(self):

        global REDIS_POOL_WATER_LEVEL_WARNING_LINE

        if self._pool_or_conn is None and self._pool:

            if (self._pool.maxsize - self._pool.size + self._pool.freesize) < REDIS_POOL_WATER_LEVEL_WARNING_LINE:
                Utils.log.warning(
                    f'Redis connection pool not enough: '
                    f'{self._pool.freesize}({self._pool.size}/{self._pool.maxsize})'
                )
            else:
                Utils.log.debug(
                    f'Redis connection pool info: '
                    f'{self._pool.freesize}({self._pool.size}/{self._pool.maxsize})'
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

    async def _safe_execute(self, func, *args, **kwargs):

        global REDIS_ERROR_RETRY_COUNT

        result = None

        async for times in AsyncCirculator(max_times=REDIS_ERROR_RETRY_COUNT):

            try:

                await self._init_conn()

                result = await func(*args, **kwargs)

            except (ReplyError, MaxClientsError, AuthError, ReadOnlyError) as err:

                await self._close_conn(True)

                raise err

            except Exception as err:

                await self._close_conn(True)

                if times < REDIS_ERROR_RETRY_COUNT:
                    Utils.log.exception(err)
                else:
                    raise err

            else:

                break

        return result

    async def execute(self, command, *args, **kwargs):

        return await self._safe_execute(super().execute, command, *args, **kwargs)

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

    # TRANSACTION COMMANDS

    async def unwatch(self):

        return await self._safe_execute(super().unwatch)

    async def watch(self, key, *keys):

        return await self._safe_execute(super().watch, key, *keys)

    def multi_exec(self):

        return MultiExec(self._pool, aioredis.Redis)

    def pipeline(self):

        return Pipeline(self._pool, aioredis.Redis)

    # PUB/SUB COMMANDS

    async def subscribe(self, channel, *channels):

        return await self._safe_execute(super().subscribe, channel, *channels)

    async def unsubscribe(self, channel, *channels):

        return await self._safe_execute(super().unsubscribe, channel, *channels)

    async def psubscribe(self, pattern, *patterns):

        return await self._safe_execute(super().psubscribe, pattern, *patterns)

    async def punsubscribe(self, pattern, *patterns):

        return await self._safe_execute(super().punsubscribe, pattern, *patterns)

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

    # GENERIC COMMANDS

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

    async def keys(self, pattern):

        result = await super().keys(pattern)

        if result is not None:
            result = [Utils.basestring(key) for key in result]

        return result

    def _keys(self, pattern, *, encoding=_NOTSET):

        return super().keys(pattern, encoding=encoding)

    async def randomkey(self):

        result = await super().randomkey()

        if result is not None:
            result = Utils.basestring(result)

        return result

    def _randomkey(self, *, encoding=_NOTSET):

        return super().randomkey(encoding=encoding)

    async def scan(self, cursor=0, match=None, count=None):

        result = await super().scan(cursor, match, count)

        if result is not None:
            result = (result[0], [Utils.basestring(val) for val in result[1]])

        return result

    def _scan(self, cursor=0, match=None, count=None):

        return super().scan(cursor, match, count)

    # STRING COMMANDS

    async def get(self, key):

        result = await super().get(key)

        if result is not None:
            result = self._val_decode(result)

        return result

    def _get(self, key, *, encoding=_NOTSET):

        return super().get(key, encoding=encoding)

    async def getset(self, key, value):

        _value = self._val_encode(value)

        result = await super().getset(key, _value)

        if result is not None:
            result = self._val_decode(result)

        return result

    def _getset(self, key, value, *, encoding=_NOTSET):

        return super().getset(key, value, encoding=encoding)

    async def mget(self, key, *keys):

        result = await super().mget(key, *keys)

        if result is not None:
            result = [self._val_decode(val) for val in result]

        return result

    def _mget(self, key, *keys, encoding=_NOTSET):

        return super().mget(key, *keys, encoding=encoding)

    async def set(self, key, value, expire=0):

        _value = self._val_encode(value)
        _expire = expire if expire > 0 else self._expire

        result = await super().set(key, _value, expire=_expire)

        return result

    def _set(self, key, value, *, expire=0, pexpire=0, exist=None):

        return super().set(key, value, expire=expire, pexpire=pexpire, exist=exist)

    async def mset(self, key, value, *pairs):

        _value = self._val_encode(value)
        _pairs = [item if index % 2 == 0 else self._val_encode(item) for index, item in enumerate(pairs)]

        result = await super().mset(key, _value, *_pairs)

        return result

    def _mset(self, key, value, *pairs):

        return super().mset(key, value, *pairs)

    async def msetnx(self, key, value, *pairs):

        _value = self._val_encode(value)
        _pairs = [item if index % 2 == 0 else self._val_encode(item) for index, item in enumerate(pairs)]

        result = await super().msetnx(key, _value, *_pairs)

        return result

    def _msetnx(self, key, value, *pairs):

        return super().msetnx(key, value, *pairs)

    async def psetex(self, key, milliseconds, value):

        _value = self._val_encode(value)

        result = await super().psetex(key, milliseconds, _value)

        return result

    def _psetex(self, key, milliseconds, value):

        return super().psetex(key, milliseconds, value)

    async def setex(self, key, seconds, value):

        _value = self._val_encode(value)

        result = await super().setex(key, seconds, _value)

        return result

    def _setex(self, key, seconds, value):

        return super().setex(key, seconds, value)

    async def setnx(self, key, value):

        _value = self._val_encode(value)

        result = await super().setnx(key, _value)

        return result

    def _setnx(self, key, value):

        return super().setnx(key, value)

    # SET COMMANDS

    async def sdiff(self, key, *keys):

        result = await super().sdiff(key, *keys)

        if result is not None:
            result = [Utils.basestring(val) for val in result]

        return result

    def _sdiff(self, key, *keys):

        return super().sdiff(key, *keys)

    async def sinter(self, key, *keys):

        result = await super().sinter(key, *keys)

        if result is not None:
            result = [Utils.basestring(val) for val in result]

        return result

    def _sinter(self, key, *keys):

        return super().sinter(key, *keys)

    async def smembers(self, key):

        result = await super().smembers(key)

        if result is not None:
            result = [Utils.basestring(val) for val in result]

        return result

    def _smembers(self, key, *, encoding=_NOTSET):

        return super().smembers(key, encoding=encoding)

    async def spop(self, key):

        result = await super().spop(key)

        if result is not None:
            result = Utils.basestring(result)

        return result

    def _spop(self, key, *, encoding=_NOTSET):

        return super().spop(key, encoding=encoding)

    async def srandmember(self, key, count=1):

        result = await super().srandmember(key, count)

        if result is not None:
            result = [Utils.basestring(val) for val in result]

        return result

    def _srandmember(self, key, count=None):

        return super().srandmember(key, count)

    async def sunion(self, key, *keys):

        result = await super().sunion(key, *keys)

        if result is not None:
            result = [Utils.basestring(val) for val in result]

        return result

    def _sunion(self, key, *keys):

        return super().sunion(key, *keys)

    async def sscan(self, key, cursor=0, match=None, count=None):

        result = await super().sscan(key, cursor, match, count)

        if result is not None:
            result = (result[0], [Utils.basestring(val) for val in result[1]])

        return result

    def _sscan(self, key, cursor=0, match=None, count=None):

        return super().sscan(key, cursor, match, count)

    # HASH COMMANDS

    async def hget(self, key, field):

        result = await super().hget(key, field)

        if result is not None:
            result = self._val_decode(result)

        return result

    def _hget(self, key, field, *, encoding=_NOTSET):

        return super().hget(key, field, encoding=encoding)

    async def hgetall(self, key):

        result = await super().hgetall(key)

        if result is not None:
            result = {Utils.basestring(key): self._val_decode(val) for key, val in result.items()}

        return result

    def _hgetall(self, key, *, encoding=_NOTSET):

        return super().hgetall(key, encoding=encoding)

    async def hkeys(self, key):

        result = await super().hkeys(key)

        if result is not None:
            result = [Utils.basestring(key) for key in result]

        return result

    def _hkeys(self, key, *, encoding=_NOTSET):

        return super().hkeys(key, encoding=encoding)

    async def hmget(self, key, field, *fields):

        result = await super().hmget(key, field, *fields)

        if result is not None:
            result = [self._val_decode(val) for val in result]

        return result

    def _hmget(self, key, field, *fields, encoding=_NOTSET):

        return super().hmget(key, field, *fields, encoding=encoding)

    async def hmset(self, key, field, value, *pairs):

        _value = self._val_encode(value)
        _pairs = [item if index % 2 == 0 else self._val_encode(item) for index, item in enumerate(pairs)]

        result = await super().hmset(key, field, _value, *_pairs)

        return result

    def _hmset(self, key, field, value, *pairs):

        return super().hmset(key, field, value, *pairs)

    async def hmset_dict(self, key, *args, **kwargs):

        for _arg in args:
            if isinstance(_arg, dict):
                kwargs.update({key: self._val_encode(val) for key, val in _arg.items()})

        return await super().hmset_dict(key, kwargs)

    def _hmset_dict(self, key, *args, **kwargs):

        return super().hmset_dict(key, *args, **kwargs)

    async def hset(self, key, field, value):

        _value = self._val_encode(value)

        result = await super().hset(key, field, _value)

        return result

    def _hset(self, key, field, value):

        return super().hset(key, field, value)

    async def hsetnx(self, key, field, value):

        _value = self._val_encode(value)

        result = await super().hsetnx(key, field, _value)

        return result

    def _hsetnx(self, key, field, value):

        return super().hsetnx(key, field, value)

    async def hvals(self, key):

        result = await super().hvals(key)

        if result is not None:
            result = [self._val_decode(val) for val in result]

        return result

    def _hvals(self, key, *, encoding=_NOTSET):

        return super().hvals(key, encoding=encoding)

    async def hscan(self, key, cursor=0, match=None, count=None):

        result = await super().hscan(key, cursor, match, count)

        if result is not None:
            result = (result[0], [(Utils.basestring(key), self._val_decode(val), ) for key, val in result[1]])

        return result

    def _hscan(self, key, cursor=0, match=None, count=None):

        return super().hscan(key, cursor, match, count)

    # LIST COMMANDS

    async def blpop(self, key, *, timeout=0):

        result = await super().blpop(key, timeout=timeout)

        if result is not None:
            result = self._val_decode(result[1])

        return result

    def _blpop(self, key, *keys, timeout=0, encoding=_NOTSET):

        return super().blpop(key, *keys, timeout=timeout, encoding=encoding)

    async def brpop(self, key, *, timeout=0):

        result = await super().brpop(key, timeout=timeout)

        if result is not None:
            result = self._val_decode(result[1])

        return result

    def _brpop(self, key, *keys, timeout=0, encoding=_NOTSET):

        return super().brpop(key, *keys, timeout=timeout, encoding=encoding)

    async def brpoplpush(self, sourcekey, destkey, timeout=0):

        result = await super().brpoplpush(sourcekey, destkey, timeout)

        if result is not None:
            result = self._val_decode(result)

        return result

    def _brpoplpush(self, sourcekey, destkey, timeout=0, encoding=_NOTSET):

        return super().brpoplpush(sourcekey, destkey, timeout, encoding)

    async def lindex(self, key, index):

        result = await super().lindex(key, index, encoding=encoding)

        if result is not None:
            result = self._val_decode(result)

        return result

    def _lindex(self, key, index, *, encoding=_NOTSET):

        return super().lindex(key, index, encoding=encoding)

    async def linsert(self, key, pivot, value, before=False):

        _pivot = self._val_encode(pivot)
        _value = self._val_encode(value)

        result = await super().linsert(key, _pivot, _value, before=before)

        return result

    def _linsert(self, key, pivot, value, before=False):

        return super().linsert(key, pivot, value, before=before)

    async def lpop(self, key):

        result = await super().lpop(key)

        if result is not None:
            result = self._val_decode(result)

        return result

    def _lpop(self, key, *, encoding=_NOTSET):

        return super().lpop(key, encoding=encoding)

    async def lpush(self, key, value, *values):

        _value = self._val_encode(value)
        _values = [self._val_encode(val) for val in values]

        result = await super().lpush(key, _value, *_values)

        return result

    def _lpush(self, key, value, *values):

        return super().lpush(key, value, *values)

    async def lpushx(self, key, value):

        _value = self._val_encode(value)

        result = await super().lpushx(key, _value)

        return result

    def _lpushx(self, key, value):

        return super().lpushx(key, value)

    async def lrange(self, key, start, stop):

        result = await super().lrange(key, start, stop)

        if result is not None:
            result = [self._val_decode(val) for val in result]

        return result

    def _lrange(self, key, start, stop, *, encoding=_NOTSET):

        return super().lrange(key, start, stop, encoding=encoding)

    async def lrem(self, key, count, value):

        _value = self._val_encode(value)

        result = await super().lrem(key, count, _value)

        return result

    def _lrem(self, key, count, value):

        return super().lrem(key, count, value)

    async def lset(self, key, index, value):

        _value = self._val_encode(value)

        result = await super().lset(key, index, _value)

        return result

    def _lset(self, key, index, value):

        return super().lset(key, index, value)

    async def rpop(self, key):

        result = await super().rpop(key)

        if result is not None:
            result = self._val_decode(result)

        return result

    def _rpop(self, key, *, encoding=_NOTSET):

        return super().rpop(self, key, encoding=encoding)

    async def rpoplpush(self, sourcekey, destkey):

        result = await super().rpoplpush(sourcekey, destkey)

        if result is not None:
            result = self._val_decode(result)

        return result

    def _rpoplpush(self, sourcekey, destkey, *, encoding=_NOTSET):

        return super().rpoplpush(sourcekey, destkey, encoding=encoding)

    async def rpush(self, key, value, *values):

        _value = self._val_encode(value)
        _values = [self._val_encode(val) for val in values]

        result = await super().rpush(key, _value, *_values)

        return result

    def _rpush(self, key, value, *values):

        return super().rpush(key, value, *values)

    async def rpushx(self, key, value):

        _value = self._val_encode(value)

        result = await super().rpushx(key, _value)

        return result

    def _rpushx(self, key, value):

        return super().rpushx(key, value)


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


class PeriodCounter:

    MIN_EXPIRE = 60

    def __init__(self, cache_pool: RedisPool, time_slice: int, key_prefix: str = r'', ntp_client: NTPClient = None):

        self._cache_pool = cache_pool

        self._time_slice = time_slice
        self._key_prefix = key_prefix

        self._ntp_client = ntp_client

    def _get_key(self, key: str = None) -> str:

        timestamp = Utils.timestamp() if self._ntp_client is None else self._ntp_client.timestamp

        time_period = Utils.math.floor(timestamp / self._time_slice)

        if key is None:
            return f'{self._key_prefix}_{time_period}'
        else:
            return f'{self._key_prefix}_{key}_{time_period}'

    async def _incr(self, key: int, val: str) -> int:

        res = None

        async with self._cache_pool.get_client() as cache:
            pipeline = cache.pipeline()
            pipeline.incrby(key, val)
            pipeline.expire(key, max(self._time_slice, self.MIN_EXPIRE))
            res, _ = await pipeline.execute()

        return res

    async def _decr(self, key: int, val: str) -> int:

        res = None

        async with self._cache_pool.get_client() as cache:
            pipeline = cache.pipeline()
            pipeline.decrby(key, val)
            pipeline.expire(key, max(self._time_slice, self.MIN_EXPIRE))
            res, _ = await pipeline.execute()

        return res

    async def incr(self, val: int, key: str = None):

        _key = self._get_key(key)

        res = await self._incr(_key, val)

        return res

    async def incr_with_trx(self, val: int, key: str = None) -> (int, Transaction):

        _key = self._get_key(key)

        res = await self._incr(_key, val)

        if res is not None:
            trx = Transaction()
            trx.add_rollback_callback(self._decr, _key, val)
        else:
            trx = None

        return res, trx

    async def decr(self, val: int, key: str = None) -> int:

        _key = self._get_key(key)

        res = await self._decr(_key, val)

        return res

    async def decr_with_trx(self, val: int, key: str = None) -> (int, Transaction):

        _key = self._get_key(key)

        res = await self._decr(_key, val)

        if res is not None:
            trx = Transaction()
            trx.add_rollback_callback(self._incr, _key, val)
        else:
            trx = None

        return res, trx
