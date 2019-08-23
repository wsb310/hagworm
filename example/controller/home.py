# -*- coding: utf-8 -*-

from hagworm.extend.asyncio.base import MultiTasks
from hagworm.frame.tornado.web import HttpBasicAuth, SocketBaseHandler, RequestBaseHandler, DownloadAgent

from model.base import DataSource, _ModelBase


class Default(RequestBaseHandler):

    async def get(self):

        data_source = DataSource()

        tasks = MultiTasks()

        tasks.append(data_source.cache_health())
        tasks.append(data_source.mysql_health())
        tasks.append(data_source.mongo_health())

        await tasks

        return self.render(
            r'home/default.html',
            cache_health=tasks[0],
            mysql_health=tasks[1],
            mongo_health=tasks[2]
        )


class Download(DownloadAgent):

    @HttpBasicAuth(r'Test', r'tester', r'123456')
    async def get(self):

        await self.transmit(r'https://www.baidu.com/img/bd_logo.png')


class Event(RequestBaseHandler):

    async def get(self):

        model = _ModelBase()

        await model.dispatch_event(r'test')


class Socket(SocketBaseHandler):

    async def open(self, channel):

        self.log.debug(r'WebSocket opened: {0}'.format(channel))

    async def on_message(self, message):

        self.write_message(message)

        self.log.debug(r'WebSocket message: {0}'.format(message))

    async def on_ping(self, data):

        self.log.debug(r'WebSocket ping: {0}'.format(data))

    async def on_close(self):

        self.log.debug(r'WebSocket closed')
