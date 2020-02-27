# -*- coding: utf-8 -*-

from hagworm.extend.base import Ignore, catch_error
from hagworm.extend.metaclass import Singleton
from hagworm.extend.asyncio.base import Utils, FuncCache, MultiTasks, async_adapter
from hagworm.extend.asyncio.cache import RedisDelegate
from hagworm.extend.asyncio.database import MongoDelegate, MySQLDelegate

from setting import ConfigStatic, ConfigDynamic


class DataSource(Singleton, RedisDelegate, MongoDelegate, MySQLDelegate):

    def __init__(self):

        RedisDelegate.__init__(self)

        MongoDelegate.__init__(
            self,
            ConfigStatic.MongoHost, ConfigStatic.MongoUser, ConfigStatic.MongoPasswd,
            min_pool_size=ConfigStatic.MongoMinConn, max_pool_size=ConfigStatic.MongoMaxConn
        )

        MySQLDelegate.__init__(self)

    @classmethod
    async def initialize(cls):

        inst = cls()

        await inst.async_init_redis(
            ConfigStatic.RedisHost, ConfigStatic.RedisPasswd,
            minsize=ConfigStatic.RedisMinConn, maxsize=ConfigStatic.RedisMaxConn,
            db=ConfigStatic.RedisBase, expire=ConfigStatic.RedisExpire,
            key_prefix=ConfigStatic.RedisKeyPrefix
        )

        if ConfigStatic.MySqlMasterServer:

            await inst.async_init_mysql_rw(
                ConfigStatic.MySqlMasterServer[0], ConfigStatic.MySqlMasterServer[1], ConfigStatic.MySqlName,
                ConfigStatic.MySqlUser, ConfigStatic.MySqlPasswd,
                minsize=ConfigStatic.MySqlMasterMinConn, maxsize=ConfigStatic.MySqlMasterMaxConn,
                echo=ConfigDynamic.Debug, pool_recycle=21600, conn_life=43200
            )

        if ConfigStatic.MySqlSlaveServer:

            await inst.async_init_mysql_ro(
                ConfigStatic.MySqlSlaveServer[0], ConfigStatic.MySqlSlaveServer[1], ConfigStatic.MySqlName,
                ConfigStatic.MySqlUser, ConfigStatic.MySqlPasswd,
                minsize=ConfigStatic.MySqlSlaveMinConn, maxsize=ConfigStatic.MySqlSlaveMaxConn,
                echo=ConfigDynamic.Debug, pool_recycle=21600, readonly=True, conn_life=43200
            )

    @FuncCache()
    async def health(self):

        result = False

        with catch_error():

            tasks = MultiTasks()

            tasks.append(self.cache_health())
            tasks.append(self.mysql_health())
            tasks.append(self.mongo_health())

            await tasks

            result = all(tasks)

        return result


class _ModelBase(Singleton, Utils):

    def __init__(self):

        self._data_source = DataSource()

        self._event_dispatcher = self._data_source.event_dispatcher(r'channel_test', 5)
        self._event_dispatcher.add_listener(r'event_test', self._event_listener)

    def Break(self, data=None, layers=1):

        raise Ignore(data, layers)

    async def dispatch_event(self, *args):

        await self._event_dispatcher.dispatch(r'event_test', *args)

    @async_adapter
    async def _event_listener(self, *args):

        self.log.info(str(args))
