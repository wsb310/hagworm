# -*- coding: utf-8 -*-

import signal
import asyncio
import logging

import jinja2

from tornado_jinja2 import Jinja2Loader

from tornado.web import Application
from tornado.options import options
from tornado.process import cpu_count, fork_processes
from tornado.netutil import bind_sockets
from tornado.httpserver import HTTPServer
from tornado.platform.asyncio import AsyncIOMainLoop

from hagworm import package_slogan
from hagworm import __version__ as package_version
from hagworm.extend.base import Utils
from hagworm.extend.interface import TaskInterface


class _InterceptHandler(logging.Handler):
    """日志监听器
    """

    def emit(self, record):

        Utils.log.opt(
            depth=6,
            exception=record.exc_info
        ).log(
            record.levelname,
            record.getMessage()
        )


class _LauncherBase(TaskInterface):
    """启动器基类
    """

    def _initialize(self, **kwargs):

        self._process_num = kwargs.get(r'process_num', 1)
        self._async_initialize = kwargs.get(r'async_initialize', None)

        self._background_service = kwargs.get(r'background_service', None)
        self._background_process = kwargs.get(r'background_process', None)

        self._process_id = 0
        self._process_num = self._process_num if self._process_num > 0 else cpu_count()

        if self._background_service is None:
            pass
        elif not isinstance(self._background_service, TaskInterface):
            raise TypeError(r'Background Service Dot Implemented Task Interface')

        if self._background_process is None:
            pass
        elif isinstance(self._background_process, TaskInterface):
            self._process_num += 1
        else:
            raise TypeError(r'Background Process Dot Implemented Task Interface')

        log_level = kwargs.get(r'log_level', r'info').upper()
        log_file_path = kwargs.get(r'log_file_path', None)

        if log_file_path:

            Utils.log.remove()

            log_file_path = Utils.path.join(
                kwargs[r'log_file_path'],
                r'runtime_{time}.log'
            )

            Utils.log.add(
                sink=log_file_path,
                level=log_level,
                enqueue=True,
                backtrace=kwargs.get(r'debug', False),
                rotation=r'00:00',
                retention=kwargs.get(r'log_file_num_backups', 7)
            )

        else:

            Utils.log.level(log_level)

        logging.getLogger(None).addHandler(_InterceptHandler())

        Utils.log.info(f'{package_slogan}\nhagworm version {package_version}\n{Utils.environment()}')

    def start(self):

        if self._background_service is not None:
            self._background_service.start()
            Utils.log.success(f'Background service no.{self._process_id} running...')

        if self._process_id == 0 and self._background_process is not None:
            self._background_process.start()
            Utils.log.success(f'Background process no.{self._process_id} running...')
        else:
            self._server.add_sockets(self._sockets)

        Utils.log.success(f'Startup server no.{self._process_id}')

        self._event_loop.run_forever()

    def stop(self, code=0, frame=None):

        if self._background_service is not None:
            self._background_service.stop()

        if self._process_id == 0 and self._background_process is not None:
            self._background_process.stop()

        self._event_loop.stop()

        Utils.log.success(f'Shutdown server no.{self._process_id}: code.{code}')

    def is_running(self):

        return self._event_loop.is_running()


class Launcher(_LauncherBase):
    """TornadoHttp的启动器

    用于简化和统一程序的启动操作

    """

    def __init__(self, router, port=80, **kwargs):

        self._initialize(**kwargs)

        self._settings = {
            r'handlers': router,
            r'debug': kwargs.get(r'debug', False),
            r'gzip': kwargs.get(r'gzip', False),
            r'log_function': self._log_request,
        }

        if r'template_path' in kwargs:
            self._settings[r'template_loader'] = Jinja2Loader(
                jinja2.Environment(
                    loader=jinja2.FileSystemLoader(kwargs[r'template_path'])
                )
            )

        if r'static_path' in kwargs:
            self._settings[r'static_path'] = kwargs[r'static_path']

        if r'cookie_secret' in kwargs:
            self._settings[r'cookie_secret'] = kwargs[r'cookie_secret']

        self._sockets = bind_sockets(port)

        if self._process_num > 1:
            self._process_id = fork_processes(self._process_num)

        options.parse_command_line()

        AsyncIOMainLoop().install()

        self._event_loop = asyncio.get_event_loop()
        self._event_loop.set_debug(self._settings[r'debug'])

        self._server = HTTPServer(Application(**self._settings))

        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

        if self._async_initialize:
            self._event_loop.run_until_complete(self._async_initialize())

    def _log_request(self, handler):

        status = handler.get_status()

        if status < 400:
            if self._settings[r'debug']:
                log_method = Utils.log.debug
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
