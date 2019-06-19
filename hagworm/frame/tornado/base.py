# -*- coding: utf-8 -*-

import signal
import asyncio
import logging

import aiotask_context as context

from tornado.web import Application
from tornado.options import options
from tornado.process import cpu_count, fork_processes
from tornado.netutil import bind_sockets
from tornado.httpserver import HTTPServer
from tornado.platform.asyncio import AsyncIOMainLoop

from hagworm.extend.base import Utils
from hagworm.extend.interface import TaskInterface

from .template import Jinja2Loader


class _InterceptHandler(logging.Handler):

    def emit(self, record):

        Utils.log.opt(
            depth=6,
            exception=record.exc_info
        ).log(
            record.levelname,
            record.getMessage()
        )


class Launcher(TaskInterface):

    def __init__(self, router, port=80, **kwargs):

        process_num = kwargs.get(r'process_num', 1)
        async_initialize = kwargs.get(r'async_initialize', None)
        background_service = kwargs.get(r'background_service', None)

        self._settings = {
            r'handlers': router,
            r'template_loader': Jinja2Loader(r'view'),
            r'debug': kwargs.get(r'debug', False),
            r'gzip': kwargs.get(r'gzip', False),
            r'log_function': self._log_request,
        }

        if r'static_path' in kwargs:
            self._settings[r'static_path'] = kwargs[r'static_path']

        if r'cookie_secret' in kwargs:
            self._settings[r'cookie_secret'] = kwargs[r'cookie_secret']

        self._process_id = 0
        self._process_num = process_num if process_num > 0 else cpu_count()

        self._sockets = bind_sockets(port)

        if background_service is None:
            self._background_service = None
        elif isinstance(background_service, TaskInterface):
            self._process_num += 1
            self._background_service = background_service
        else:
            raise TypeError(r'Not Implemented Task Interface')

        if self._process_num > 1:
            self._process_id = fork_processes(self._process_num)

        log_level = kwargs.get(r'log_level', r'info').upper()
        log_file_path = kwargs.get(r'log_file_path', None)

        if log_file_path:

            log_file_path = Utils.path.join(
                kwargs[r'log_file_path'],
                r'runtime_{time}.log'
            )

            Utils.log.add(
                sink=log_file_path,
                level=log_level,
                enqueue=True,
                backtrace=self._settings[r'debug'],
                rotation=r'00:00',
                retention=kwargs.get(r'log_file_num_backups', 7)
            )

        else:

            Utils.log.level(log_level)

        logging.getLogger(None).addHandler(_InterceptHandler())

        options.parse_command_line()

        AsyncIOMainLoop().install()

        self._event_loop = asyncio.get_event_loop()
        self._event_loop.set_task_factory(context.task_factory)

        self._server = HTTPServer(Application(**self._settings))

        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

        if async_initialize:
            self._event_loop.run_until_complete(async_initialize())

    def start(self):

        if self._process_id == 0 and self._background_service is not None:
            self._background_service.start()
        else:
            self._server.add_sockets(self._sockets)

        Utils.log.success(
            r'Startup http server process_{0}'.format(
                self._process_id
            )
        )

        self._event_loop.run_forever()

    def stop(self, code=0, frame=None):

        if self._process_id == 0 and self._background_service is not None:
            self._background_service.stop()

        self._event_loop.stop()

        Utils.log.success(
            r'Shutdown http server process_{0}: {1}'.format(
                self._process_id,
                code
            )
        )

    def is_running(self):

        return self._event_loop.is_running()

    def _log_request(self, handler):

        status = handler.get_status()

        if status < 400:
            if self._settings[r'debug']:
                log_method = Utils.log.info
            else:
                return
        elif status < 500:
            log_method = Utils.log.warning
        else:
            log_method = Utils.log.error

        log_method(
            r'%d %s %.2fms' % (
                handler.get_status(),
                handler._request_summary(),
                1000.0 * handler.request.request_time()
            )
        )
