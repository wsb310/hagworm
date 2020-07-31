# -*- coding: utf-8 -*-

import signal
import asyncio

from tornado.process import fork_processes
from tornado.netutil import bind_sockets
from tornado.tcpserver import TCPServer
from tornado.iostream import StreamClosedError
from tornado.platform.asyncio import AsyncIOMainLoop

from hagworm.extend.base import Utils
from hagworm.frame.tornado.base import _LauncherBase


class _TCPServer(TCPServer):
    """TCPServer实现类
    """

    def __init__(self, protocol, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self._protocol = protocol

    async def handle_stream(self, stream, address):

        try:

            await self._protocol(stream, address)

        except Exception as err:

            Utils.log.error(err)

        finally:

            if not stream.closed():
                await stream.close()


class Protocol:
    """Protocol实现类
    """

    def __init__(self, stream, address):

        self._stream = stream
        self._address = address

    def __await__(self):

        try:

            yield from self.connection_made().__await__()

            while True:

                try:
                    yield from self._read_bytes().__await__()
                except StreamClosedError:
                    break
                except Exception as err:
                    Utils.log.error(err)

        except Exception as err:

            Utils.log.error(err)

        finally:

            yield from self.connection_lost().__await__()

        return self

    async def _read_bytes(self):

        await self.data_received(
            await self._stream.read_bytes(65536, True)
        )

    @property
    def closed(self):

        self._stream.closed()

    @property
    def client_ip(self):

        return self._address[0]

    @property
    def client_port(self):

        return self._address[1]

    @property
    def client_address(self):

        return Utils.join_str(self._address, r':')

    async def close(self):

        self._stream.close()

    async def connection_made(self):

        raise NotImplementedError()

    async def connection_lost(self):

        raise NotImplementedError()

    async def data_received(self, chunk):

        raise NotImplementedError()

    async def data_write(self, chunk):

        await self._stream.write(chunk)


class Launcher(_LauncherBase):
    """TornadoTCP的启动器

    用于简化和统一程序的启动操作

    """

    def __init__(self, protocol, port, **kwargs):

        super().__init__(**kwargs)

        if not issubclass(protocol, Protocol):
            raise TypeError(r'Dot Implemented Protocol Interface')

        self._settings = {
            r'ssl_options': kwargs.get(r'ssl_options', None),
            r'max_buffer_size': kwargs.get(r'max_buffer_size', None),
            r'read_chunk_size': kwargs.get(r'read_chunk_size', None),
        }

        self._sockets = bind_sockets(port)

        if self._process_num > 1:
            self._process_id = fork_processes(self._process_num)

        AsyncIOMainLoop().install()

        self._event_loop = asyncio.get_event_loop()
        self._event_loop.set_debug(self._debug)

        self._event_loop.add_signal_handler(signal.SIGINT, self.stop)
        self._event_loop.add_signal_handler(signal.SIGTERM, self.stop)

        self._server = _TCPServer(protocol, **self._settings)

        if self._async_initialize:
            self._event_loop.run_until_complete(self._async_initialize())
