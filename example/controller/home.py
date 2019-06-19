# -*- coding: utf-8 -*-

from tornado.gen import coroutine

from hagworm.extend.struct import Result
from hagworm.frame.tornado.web import HttpBasicAuth, SocketBaseHandler, RequestBaseHandler, DownloadAgent

from model.base import DataSource, _ModelBase


class Default(RequestBaseHandler):

    @coroutine
    def get(self):

        data_source = DataSource()

        resp = {
            r'cache_health': (yield data_source.cache_health()),
            r'mysql_health': (yield data_source.mysql_health()),
            r'mongo_health': (yield data_source.mongo_health()),
        }

        return self.write_json(Result(data=resp))


class Download(DownloadAgent):

    @HttpBasicAuth(r'Test', r'tester', r'123456')
    @coroutine
    def get(self):

        yield self.transmit(r'https://www.baidu.com/img/bd_logo.png')


class Event(RequestBaseHandler):

    @coroutine
    def get(self):

        model = _ModelBase()

        yield model.dispatch_event(r'test')


class Socket(SocketBaseHandler):

    @coroutine
    def open(self, channel):

        self.log.debug(r'WebSocket opened: {0}'.format(channel))

    @coroutine
    def on_message(self, message):

        self.write_message(message)

        self.log.debug(r'WebSocket message: {0}'.format(message))

    @coroutine
    def on_ping(self, data):

        self.log.debug(r'WebSocket ping: {0}'.format(data))

    @coroutine
    def on_close(self):

        self.log.debug(r'WebSocket closed')
