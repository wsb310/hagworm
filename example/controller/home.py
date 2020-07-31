# -*- coding: utf-8 -*-

from hagworm.extend.asyncio.base import Utils
from hagworm.frame.tornado.web import SocketBaseHandler, RequestBaseHandler, DownloadAgent,\
    HttpBasicAuth, LogRequestMixin

from service.base import DataSource


class Default(RequestBaseHandler, LogRequestMixin):

    async def head(self):

        await DataSource().health()

    async def get(self):

        return self.render(
            r'home/default.html',
            online=DataSource().online
        )

    def delete(self):

        Utils.call_soon(DataSource().reset_mysql_pool)
        Utils.call_soon(DataSource().reset_mongo_pool)


class Download(DownloadAgent):

    @HttpBasicAuth(r'Test', r'tester', r'123456')
    async def get(self):

        await self.transmit(r'https://www.baidu.com/img/bd_logo.png')


class Socket(SocketBaseHandler):

    async def open(self, channel):

        self.log.debug(f'WebSocket opened: {channel}')

    async def on_message(self, message):

        self.write_message(message)

        self.log.debug(f'WebSocket message: {message}')

    async def on_ping(self, data):

        self.log.debug(f'WebSocket ping: {data}')

    async def on_close(self):

        self.log.debug(r'WebSocket closed')
