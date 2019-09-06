# -*- coding: utf-8 -*-

from wtforms import validators, StringField
from wtforms_tornado import Form

from hagworm.extend.struct import Result
from hagworm.frame.tornado.web import HttpBasicAuth, MethodWrapper, SocketBaseHandler, RequestBaseHandler, DownloadAgent

from model.base import DataSource, _ModelBase


class Default(RequestBaseHandler):

    async def get(self):

        result = await DataSource().health()

        return self.render(
            r'home/default.html',
            health=result
        )


class Download(DownloadAgent):

    @HttpBasicAuth(r'Test', r'tester', r'123456')
    async def get(self):

        await self.transmit(r'https://www.baidu.com/img/bd_logo.png')



class Event(RequestBaseHandler):

    class _TestForm(Form):
        event_data = StringField(r'事件数据', [validators.required()])

    @MethodWrapper(_TestForm)
    async def get(self):

        model = _ModelBase()

        await model.dispatch_event(self.data[r'event_data'])

        return Result()


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
