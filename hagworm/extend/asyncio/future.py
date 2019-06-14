# -*- coding: utf-8 -*-

import asyncio
import subprocess

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from hagworm.extend.interface import TaskInterface

from .base import Utils


class ThreadPool:

    def __init__(self, max_workers):

        self._thread_pool = ThreadPoolExecutor(max_workers)

    async def run(self, _callable, *args, **kwargs):
        """线程转协程，不支持协程函数
        """

        future = self._thread_pool.submit(_callable, *args, **kwargs)

        return await asyncio.wrap_future(future)


class ProcessPool:

    def __init__(self, max_workers):

        self._process_pool = ProcessPoolExecutor(max_workers)

    async def run(self, _callable, *args, **kwargs):
        """进程转协程，不支持协程函数
        """

        future = self._process_pool.submit(_callable, *args, **kwargs)

        return await asyncio.wrap_future(future)


class SubProcess(TaskInterface):

    @classmethod
    def create(self, cls, cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE):

        process = cls(cmd, stdout=stdout, stderr=stderr)
        process.start()

        return process

    def __init__(self, commend, stdout=subprocess.PIPE, stderr=subprocess.PIPE):

        self._commend = commend

        self._stdout = stdout
        self._stderr = stderr

        self._process = None
        self._process_id = None

        self._begin_time = 0

    def is_running(self):

        return self._process is not None and self._process.poll() is None

    def start(self):

        self._process = subprocess.Popen(
            self._commend,
            shell=True,
            stdout=self._stdout,
            stderr=self._stderr
        )
        self._process_id = self._process.pid

        self._begin_time = Utils.timestamp()

    def stop(self):

        if self._process is None:
            return

        if self._process.poll() is None:
            self._process.kill()

    async def wait(self, timeout=0, callback=None, *args, **kwargs):

        if timeout > 0:

            deadline = self._begin_time + timeout

            while Utils.timestamp() < deadline:

                if self.is_running():
                    await Utils.wait_frame()
                else:
                    break

            else:

                self.stop()

        else:

            while self.is_running():
                await Utils.wait_frame()

        if callback is not None:

            result = callback(*args, **kwargs)

            if isinstance(result, asyncio.Future):
                await result
