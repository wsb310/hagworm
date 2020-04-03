# -*- coding: utf-8 -*-

from tempfile import SpooledTemporaryFile

from .base import Utils
from .task import IntervalTask

from hagworm.extend.interface import ObjectFactoryInterface


class QueueBuffer:

    def __init__(self, maxsize, timeout=0):

        self._maxsize = maxsize

        self._timer = IntervalTask.create(timeout, False, self._check) if timeout > 0 else None

        self._data_list = []

    async def _check(self):

        if len(self._data_list) == 0:
            return

        data_list, self._data_list = self._data_list, []

        await self._run(data_list)

    async def _run(self, data_list):
        raise NotImplementedError()

    def append(self, data):

        self._data_list.append(data)

        if len(self._data_list) >= self._maxsize:
            data_list, self._data_list = self._data_list, []
            Utils.call_soon(self._run, data_list)


class FileBuffer:
    """文件缓存类
    """

    class DefaultFactory(ObjectFactoryInterface):

        def __init__(self, max_size=0x10000):

            self._max_size = max_size

        def create(self):

            return SpooledTemporaryFile(self._max_size)

    def __init__(self, slice_size=0x1000000, file_factory=None):

        self._factory = self.DefaultFactory() if file_factory is None else file_factory

        self._buffers = []

        self._slice_size = slice_size

        self._read_offset = 0

        self._append_buffer()

    def _append_buffer(self):

        self._buffers.append(self._factory.create())

    def fileno(self):

        return self._buffers[-1].fileno()

    def write(self, data):

        buffer = self._buffers[-1]

        buffer.seek(0, 2)
        buffer.write(data)
        buffer.flush()

        if buffer.tell() >= self._slice_size:
            self._append_buffer()

    def read(self, size=None):

        buffer = self._buffers[0]

        buffer.seek(self._read_offset, 0)

        result = buffer.read(size)

        if len(result) == 0 and len(self._buffers) > 1:
            self._buffers.pop(0).close()
            self._read_offset = 0
        else:
            self._read_offset = buffer.tell()

        return result
