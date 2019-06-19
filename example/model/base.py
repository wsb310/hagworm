# -*- coding: utf-8 -*-

from hagworm.extend.base import Ignore
from hagworm.extend.metaclass import Singleton
from hagworm.extend.asyncio.base import Utils, async_adapter
from hagworm.extend.asyncio.cache import RedisDelegate
from hagworm.extend.asyncio.database import MongoDelegate, MySQLDelegate

from setting import config


class DataSource(Singleton, RedisDelegate, MongoDelegate, MySQLDelegate):

    def __init__(self):

        RedisDelegate.__init__(self)

        MongoDelegate.__init__(
            self,
            config.MongoHost, config.MongoUser, config.MongoPasswd,
            min_pool_size=config.MongoMinConn, max_pool_size=config.MongoMaxConn
        )

        MySQLDelegate.__init__(self)

    async def initialize(self):

        await self.async_init_redis(
            config.RedisHost, config.RedisPasswd,
            minsize=config.RedisMinConn, maxsize=config.RedisMaxConn,
            db=config.RedisBase, expire=config.RedisExpire,
            key_prefix=config.RedisKeyPrefix
        )

        if config.MySqlMasterServer:

            await self.async_init_mysql_rw(
                config.MySqlMasterServer[0], config.MySqlMasterServer[1], config.MySqlName,
                config.MySqlUser, config.MySqlPasswd,
                minsize=config.MySqlMasterMinConn, maxsize=config.MySqlMasterMaxConn
            )

        if config.MySqlSlaveServer:

            await self.async_init_mysql_ro(
                config.MySqlSlaveServer[0], config.MySqlSlaveServer[1], config.MySqlName,
                config.MySqlUser, config.MySqlPasswd,
                minsize=config.MySqlSlaveMinConn, maxsize=config.MySqlSlaveMaxConn,
                readonly=True
            )


class _ModelBase(Singleton, Utils):

    def __init__(self):

        self._data_source = DataSource()

        self._event_dispatcher = self._data_source.event_dispatcher(r'channel_test', 5)
        self._event_dispatcher.add_listener(r'event_test', self._event_listener)

    def Break(self, msg=None):

        raise Ignore(msg)

    async def dispatch_event(self, *args):

        await self._event_dispatcher.dispatch(r'event_test', *args)

    @async_adapter
    async def _event_listener(self, *args):

        self.log.info(str(args))
