# -*- coding: utf-8 -*-

import zmq

from concurrent.futures import CancelledError
from zmq.asyncio import Context

from hagworm.extend.base import ContextManager
from hagworm.extend.asyncio.base import Utils, AsyncCirculator
from hagworm.extend.asyncio.buffer import QueueBuffer


class _SocketBase(ContextManager):

    def __init__(self, name, socket_type, address, bind_mode):

        self._name = name if name else Utils.uuid1()[:8]

        self._context = Context.instance()
        self._socket = self._context.socket(socket_type)

        self._address = address
        self._bind_mode = bind_mode

    def _context_initialize(self):

        self.open()

    def _context_release(self):

        self.close()

    def open(self):

        if self._bind_mode:
            self._socket.bind(self._address)
        else:
            self._socket.connect(self._address)

    def close(self):

        if not self._socket.closed:
            self._socket.close()

    def set_hwm(self, val):

        self._socket.set_hwm(val)


class Subscriber(_SocketBase):

    def __init__(self, address, bind_mode=False, *, name=None, topic=r''):

        super().__init__(name, zmq.SUB, address, bind_mode)

        self._msg_listen_task = None

        self._socket.setsockopt_string(zmq.SUBSCRIBE, topic)

    async def _message_listener(self):

        while not self._socket.closed:
            try:
                await self._message_handler(
                    await self._socket.recv_pyobj()
                )
            except CancelledError as _:
                pass
            except Exception as err:
                Utils.log.error(err)

    async def _message_handler(self, data):

        raise NotImplementedError()

    def open(self):

        super().open()

        self._msg_listen_task = Utils.create_task(self._message_listener())

    def close(self):

        super().close()

        if self._msg_listen_task is not None:
            self._msg_listen_task.cancel()


class Publisher(_SocketBase):

    def __init__(self, address, bind_mode=False, *, name=None):

        super().__init__(name, zmq.PUB, address, bind_mode)

    async def send(self, data):

        await self._socket.send_pyobj(data)


class PublisherWithBuffer(_SocketBase, QueueBuffer):

    def __init__(self, address, bind_mode=False, *, name=None, buffer_maxsize=0xffff, buffer_timeout=1):

        _SocketBase.__init__(self, name, zmq.PUB, address, bind_mode)
        QueueBuffer.__init__(self, buffer_maxsize, buffer_timeout)

    async def _run(self, data_list):

        await self._socket.send_pyobj(data_list)

    async def safe_close(self, timeout=0):

        async for _ in AsyncCirculator(timeout):
            if len(self._data_list) == 0:
                super().close()
                break
