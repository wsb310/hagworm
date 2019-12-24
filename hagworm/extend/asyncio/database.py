# -*- coding: utf-8 -*-

import asyncio
import aiomysql

from aiomysql.sa import SAConnection, Engine
from aiomysql.sa.engine import _dialect as dialect

from pymysql.err import Warning, DataError, IntegrityError, ProgrammingError

from sqlalchemy.sql.selectable import Select
from sqlalchemy.sql.dml import Insert, Update, Delete

from motor.motor_asyncio import AsyncIOMotorClient

from .base import Utils, WeakContextVar, AsyncContextManager, AsyncCirculator


MYSQL_POLL_WATER_LEVEL_WARNING_LINE = 8


class MongoPool:
    """Mongo连接管理
    """

    def __init__(self, host, username=None, password=None, *, min_pool_size=8, max_pool_size=32, **settings):

        settings[r'host'] = host
        settings[r'minPoolSize'] = min_pool_size
        settings[r'maxPoolSize'] = max_pool_size

        if username and password:
            settings[r'username'] = username
            settings[r'password'] = password

        self._pool = AsyncIOMotorClient(**settings)

        Utils.log.info(f'MongoDB {host} initialized')

    def get_database(self, db_name):

        result = None

        try:
            result = self._pool[db_name]
        except Exception as err:
            Utils.log.exception(err)

        return result


class MongoDelegate:
    """Mongo功能组件
    """

    def __init__(self, *args, **kwargs):

        self._mongo_pool = MongoPool(*args, **kwargs)

    async def mongo_health(self):

        result = False

        try:

            result = bool(await self._mongo_pool._pool.server_info())

        except Exception as err:

            Utils.log.error(err)

        return result

    def get_mongo_database(self, db_name):

        return self._mongo_pool.get_database(db_name)

    def get_mongo_collection(self, db_name, collection):

        return self.get_mongo_database(db_name)[collection]


class MySQLPool:
    """MySQL连接管理
    """

    class _Connection(SAConnection):

        async def destroy(self):

            if self._connection is None:
                return

            if self._transaction is not None:
                await self._transaction.rollback()
                self._transaction = None

            self._connection.close()

            self._engine.release(self)
            self._connection = None
            self._engine = None

    def __init__(self, host, port, db, user, password, *, minsize=8, maxsize=32, charset=r'utf8', autocommit=True, cursorclass=aiomysql.DictCursor, readonly=False, **settings):

        self._pool = None
        self._engine = None
        self._readonly = readonly

        self._settings = settings

        self._settings[r'host'] = host
        self._settings[r'port'] = port
        self._settings[r'db'] = db

        self._settings[r'user'] = user
        self._settings[r'password'] = password

        self._settings[r'minsize'] = minsize
        self._settings[r'maxsize'] = maxsize

        self._settings[r'charset'] = charset
        self._settings[r'autocommit'] = autocommit
        self._settings[r'cursorclass'] = cursorclass

    async def initialize(self):

        self._pool = await aiomysql.create_pool(**self._settings)
        self._engine = Engine(dialect, self._pool)

        Utils.log.info(
            r'MySQL {0}:{1} {2}({3}) initialized'.format(
                self._settings[r'host'], self._settings[r'port'], self._settings[r'db'],
                r'ro' if self._readonly else r'rw'
            )
        )

    async def get_sa_conn(self):

        global MYSQL_POLL_WATER_LEVEL_WARNING_LINE

        if self._pool.freesize < MYSQL_POLL_WATER_LEVEL_WARNING_LINE:
            Utils.log.warning(
                f'MySQL connection pool not enough: {self._pool.freesize}({self._pool.size}/{self._pool.maxsize})'
            )

        conn = await self._pool.acquire()

        return self._Connection(conn, self._engine)

    def get_client(self):

        result = None

        try:
            result = DBClient(self, self._readonly)
        except Exception as err:
            Utils.log.exception(err)

        return result

    def get_transaction(self):

        result = None

        try:
            result = DBTransaction(self)
        except Exception as err:
            Utils.log.exception(err)

        return result


class MySQLDelegate:
    """MySQL功能组件
    """

    def __init__(self):

        self._mysql_rw_pool = None
        self._mysql_ro_pool = None

        context_uuid = Utils.uuid1()

        self._mysql_rw_client_context = WeakContextVar(f'mysql_rw_client_{context_uuid}')
        self._mysql_ro_client_context = WeakContextVar(f'mysql_ro_client_{context_uuid}')

    async def async_init_mysql_rw(self, *args, **kwargs):

        self._mysql_rw_pool = MySQLPool(*args, **kwargs)

        await self._mysql_rw_pool.initialize()

    async def async_init_mysql_ro(self, *args, **kwargs):

        self._mysql_ro_pool = MySQLPool(*args, **kwargs)

        await self._mysql_ro_pool.initialize()

    async def mysql_health(self):

        result = False

        if self._mysql_rw_pool:
            async with self.get_db_client() as _client:
                _proxy = await _client.execute(r'select version();')
                result = bool(await _proxy.cursor.fetchone())
                await _proxy.close()

        if self._mysql_ro_pool:
            async with self.get_db_client(True) as _client:
                _proxy = await _client.execute(r'select version();')
                result &= bool(await _proxy.cursor.fetchone())
                await _proxy.close()

        return result

    def get_db_client(self, readonly=False, *, alone=False):

        client = None

        if alone:

            if readonly:
                if self._mysql_ro_pool is not None:
                    client = self._mysql_ro_pool.get_client()
                else:
                    client = self._mysql_rw_pool.get_client()
                    client._readonly = True
            else:
                client = self._mysql_rw_pool.get_client()

        else:

            if readonly:

                _client = self._mysql_rw_client_context.get()

                if _client is not None:
                    Utils.create_task(_client.release())

                client = self._mysql_ro_client_context.get()

                if client is None:

                    if self._mysql_ro_pool is not None:
                        client = self._mysql_ro_pool.get_client()
                    else:
                        client = self._mysql_rw_pool.get_client()
                        client._readonly = True

                    self._mysql_ro_client_context.set(client)

            else:

                _client = self._mysql_ro_client_context.get()

                if _client is not None:
                    Utils.create_task(_client.release())

                client = self._mysql_rw_client_context.get()

                if client is None:
                    client = self._mysql_rw_pool.get_client()
                    self._mysql_rw_client_context.set(client)

        return client

    def get_db_transaction(self):

        _client = self._mysql_rw_client_context.get()

        if _client is not None:
            Utils.create_task(_client.release())

        _client = self._mysql_ro_client_context.get()

        if _client is not None:
            Utils.create_task(_client.release())

        return self._mysql_rw_pool.get_transaction()


class _ClientBase:
    """MySQL客户端基类
    """

    class ReadOnly(Exception):
        pass

    @staticmethod
    def safestr(val):

        cls = type(val)

        if cls is str:
            val = aiomysql.escape_string(val)
        elif cls is dict:
            val = aiomysql.escape_dict(val)
        else:
            val = str(val)

        return val

    def __init__(self, readonly=False):

        self._readonly = readonly

    @property
    def readonly(self):

        return self._readonly

    @property
    def insert_id(self):

        raise NotImplementedError()

    def _get_conn(self):

        raise NotImplementedError()

    async def execute(self, clause):

        raise NotImplementedError()

    async def select(self, clause):

        result = []

        if not isinstance(clause, Select):
            raise TypeError(r'Not sqlalchemy.sql.selectable.Select object')

        proxy = await self.execute(clause)

        if proxy is not None:

            records = await proxy.cursor.fetchall()

            if records:
                result.extend(records)

            if not proxy.closed:
                await proxy.close()

        return result

    async def find(self, clause):

        result = None

        if not isinstance(clause, Select):
            raise TypeError(r'Not sqlalchemy.sql.selectable.Select object')

        proxy = await self.execute(clause)

        if proxy is not None:

            record = await proxy.cursor.fetchone()

            if record:
                result = record

            if not proxy.closed:
                await proxy.close()

        return result

    async def insert(self, clause):

        result = 0

        if self._readonly:
            raise self.ReadOnly()

        if not isinstance(clause, Insert):
            raise TypeError(r'Not sqlalchemy.sql.dml.Insert object')

        proxy = await self.execute(clause)

        if proxy is not None:

            result = self.insert_id

            if not proxy.closed:
                await proxy.close()

        return result

    async def update(self, clause):

        result = 0

        if self._readonly:
            raise self.ReadOnly()

        if not isinstance(clause, Update):
            raise TypeError(r'Not sqlalchemy.sql.dml.Update object')

        proxy = await self.execute(clause)

        if proxy is not None:

            result = proxy.rowcount

            if not proxy.closed:
                await proxy.close()

        return result

    async def delete(self, clause):

        result = 0

        if self._readonly:
            raise self.ReadOnly()

        if not isinstance(clause, Delete):
            raise TypeError(r'Not sqlalchemy.sql.dml.Delete object')

        proxy = await self.execute(clause)

        if proxy is not None:

            result = proxy.rowcount

            if not proxy.closed:
                await proxy.close()

        return result


class DBClient(_ClientBase, AsyncContextManager):
    """MySQL客户端对象，使用with进行上下文管理

    将连接委托给客户端对象管理，提高了整体连接的使用率

    """

    def __init__(self, pool, readonly):

        super().__init__(readonly)

        self._lock = asyncio.Lock()

        self._pool = pool
        self._conn = None

    @property
    def insert_id(self):

        return self._conn.connection.insert_id()

    async def _get_conn(self):

        if self._conn is None and self._pool:
            self._conn = await self._pool.get_sa_conn()

        return self._conn

    async def _close_conn(self, discard=False):

        if self._conn is not None:

            _conn, self._conn = self._conn, None

            if discard:
                await _conn.destroy()
            else:
                await _conn.close()

    async def _context_release(self):

        await self._close_conn(self._lock.locked())

    async def release(self):

        async with self._lock:

            await self._close_conn()

    async def execute(self, clause):

        result = None

        async with self._lock:

            async for _ in AsyncCirculator(max_times=0x1f):

                try:

                    conn = await self._get_conn()

                    result = await conn.execute(clause)

                except (Warning, DataError, IntegrityError, ProgrammingError) as err:

                    Utils.log.exception(err)

                    await self._close_conn(True)

                    break

                except Exception as err:

                    Utils.log.exception(err)

                    await self._close_conn(True)

                else:

                    break

        return result


class DBTransaction(DBClient):
    """MySQL客户端事务对象，使用with进行上下文管理

    将连接委托给客户端对象管理，提高了整体连接的使用率

    """

    def __init__(self, pool):

        super().__init__(pool, False)

        self._trx = None

    async def _get_conn(self):

        if self._conn is None and self._pool:
            self._conn = await self._pool.get_sa_conn()
            self._trx = await self._conn.begin()

        return self._conn

    async def _context_release(self):

        await self.rollback()

    async def release(self):

        await self.rollback()

    async def execute(self, clause):

        result = None

        async with self._lock:

            try:

                conn = await self._get_conn()

                result = await conn.execute(clause)

            except Exception as err:

                await self._close_conn(True)

                Utils.log.exception(err)

        return result

    async def commit(self):

        async with self._lock:

            if self._trx:
                _trx, self._trx = self._trx, None
                await _trx.commit()
                await _trx.close()

            await self._close_conn()

    async def rollback(self):

        async with self._lock:

            if self._trx:
                _trx, self._trx = self._trx, None
                await _trx.rollback()
                await _trx.close()

            await self._close_conn()
