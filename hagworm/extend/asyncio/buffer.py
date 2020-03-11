# -*- coding: utf-8 -*-

from .base import Utils
from .task import IntervalTask


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
