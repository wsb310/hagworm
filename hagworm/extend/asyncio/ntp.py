# -*- coding: utf-8 -*-

import time
import numpy

from ntplib import NTPClient

from .base import Utils, MultiTasks
from .task import IntervalTask
from .future import ThreadPool

from hagworm.extend.error import NTPCalibrateError
from hagworm.extend.interface import TaskInterface


class _Interface(TaskInterface):
    """NTP客户端接口定义
    """

    def start(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    def is_running(self):
        raise NotImplementedError()

    def calibrate_offset(self):
        raise NotImplementedError()

    @property
    def offset(self):
        raise NotImplementedError()

    @property
    def timestamp(self):
        raise NotImplementedError()


class AsyncNTPClient(_Interface):
    """异步NTP客户端类
    """

    @classmethod
    async def create(cls, host):

        client = cls(host)

        await client.calibrate_offset()

        client.start()

        return client

    def __init__(self, host, *, version=2, port=r'ntp', timeout=5, interval=3600, sampling=5):

        self._settings = {
            r'host': host,
            r'version': version,
            r'port': port,
            r'timeout': timeout,
        }

        self._client = NTPClient()
        self._offset = 0

        self._thread_pool = ThreadPool(1)
        self._sync_task = IntervalTask(self.calibrate_offset, interval)

        self._sampling = sampling

    def start(self):

        return self._sync_task.start()

    def stop(self):

        return self._sync_task.stop()

    def is_running(self):

        return self._sync_task.is_running()

    async def calibrate_offset(self):

        return await self._thread_pool.run(self._calibrate_offset)

    def _calibrate_offset(self):

        samples = []
        host_name = self._settings[r'host']

        # 多次采样取中位数，减少抖动影响
        for _ in range(self._sampling):
            try:
                resp = self._client.request(**self._settings)
                samples.append(resp.offset)
            except Exception as err:
                Utils.log.error(f'NTP server {host_name} request error: {err}')

        if samples:
            self._offset = float(numpy.median(samples))
            Utils.log.debug(f'NTP server {host_name} offset median {self._offset} samples: {samples}')
        else:
            raise NTPCalibrateError(f'NTP server {host_name} not available, timestamp uncalibrated')

    @property
    def offset(self):

        return self._offset

    @property
    def timestamp(self):

        return time.time() + self._offset


class AsyncNTPClientPool(_Interface):
    """异步NTP客户端池，多节点取中位数实现高可用
    """

    @classmethod
    async def create(cls, hosts):

        client_pool = cls()

        for host in hosts:
            client_pool.append(host)

        await client_pool.calibrate_offset()

        client_pool.start()

        return client_pool

    def __init__(self):

        self._clients = []
        self._running = False

    def append(self, host, *, version=2, port='ntp', timeout=5, interval=3600, sampling=5):

        client = AsyncNTPClient(host, version=version, port=port, timeout=timeout, interval=interval, sampling=sampling)

        self._clients.append(client)

        if self._running:
            client.start()

    def start(self):

        for client in self._clients:
            client.start()

        self._running = True

    def stop(self):

        for client in self._clients:
            client.stop()

        self._running = False

    def is_running(self):

        return self._running

    async def calibrate_offset(self):

        tasks = MultiTasks()

        for client in self._clients:
            tasks.append(client.calibrate_offset())

        await tasks

    @property
    def offset(self):

        samples = []

        for client in self._clients:
            samples.append(client.offset)

        return float(numpy.median(samples))

    @property
    def timestamp(self):

        return time.time() + self.offset
