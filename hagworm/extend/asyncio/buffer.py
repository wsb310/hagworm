# -*- coding: utf-8 -*-

from tempfile import TemporaryFile

from .base import Utils
from .task import IntervalTask

from hagworm.extend.base import ContextManager


class QueueBuffer:

    def __init__(self, maxsize, timeout=0):

        self._maxsize = maxsize

        self._timer = IntervalTask.create(timeout, False, self._handle_buffer) if timeout > 0 else None

        self._data_list = []

    def _handle_buffer(self):

        if len(self._data_list) > 0:
            data_list, self._data_list = self._data_list, []
            Utils.call_soon(self._run, data_list)

    async def _run(self, data_list):
        raise NotImplementedError()

    def append(self, data):

        self._data_list.append(data)

        if len(self._data_list) >= self._maxsize:
            self._handle_buffer()

    def extend(self, data_list):

        self._data_list.extend(data_list)

        if len(self._data_list) >= self._maxsize:
            self._handle_buffer()


class FileBuffer(ContextManager):
    """文件缓存类
    """

    def __init__(self, slice_size=0x1000000):

        self._buffers = []

        self._slice_size = slice_size

        self._read_offset = 0

        self._append_buffer()

    def _context_release(self):

        self.close()

    def _append_buffer(self):

        self._buffers.append(TemporaryFile())

    def close(self):

        while len(self._buffers) > 0:
            self._buffers.pop(0).close()

        self._read_offset = 0

    def write(self, data):

        buffer = self._buffers[-1]

        buffer.seek(0, 2)
        buffer.write(data)

        if buffer.tell() >= self._slice_size:
            buffer.flush()
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
