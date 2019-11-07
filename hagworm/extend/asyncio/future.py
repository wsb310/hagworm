# -*- coding: utf-8 -*-

import asyncio
import functools

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from hagworm.extend.interface import TaskInterface

from .base import Utils


class ThreadPool:
    """线程池，桥接线程与协程
    """

    def __init__(self, max_workers):

        self._thread_pool = ThreadPoolExecutor(max_workers)

    async def run(self, _callable, *args, **kwargs):
        """线程转协程，不支持协程函数
        """

        future = self._thread_pool.submit(_callable, *args, **kwargs)

        return await asyncio.wrap_future(future)


class ThreadWorker:
    """通过线程转协程实现普通函数非阻塞的装饰器
    """

    def __init__(self, max_workers):

        self._thread_pool = ThreadPool(max_workers)

    def __call__(self, func):

        @functools.wraps(func)
        def _wrapper(*args, **kwargs):
            return self._thread_pool.run(func, *args, **kwargs)

        return _wrapper


class ProcessPool:
    """进程池，桥接进程与协程
    """

    def __init__(self, max_workers):

        self._process_pool = ProcessPoolExecutor(max_workers)

    async def run(self, _callable, *args, **kwargs):
        """进程转协程，不支持协程函数
        """

        future = self._process_pool.submit(_callable, *args, **kwargs)

        return await asyncio.wrap_future(future)


class ProcessWorker:
    """通过进程转协程实现普通函数非阻塞的装饰器
    """

    def __init__(self, max_workers):

        self._process_pool = ProcessPool(max_workers)

    def __call__(self, func):

        @functools.wraps(func)
        def _wrapper(*args, **kwargs):
            return self._process_pool.run(func, *args, **kwargs)

        return _wrapper


class SubProcess(TaskInterface):
    """子进程管理，通过command方式启动子进程
    """

    @classmethod
    def create(cls, cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE):

        process = cls(cmd, stdout=stdout, stderr=stderr)
        process.start()

        return process

    def __init__(self, commend, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE):

        self._commend = commend

        self._stdout = stdout
        self._stderr = stderr

        self._process = None
        self._process_id = None

    def is_running(self):

        return self._process is not None and self._process.returncode is None

    async def start(self):

        if self._process is not None:
            return False

        self._process = await asyncio.create_subprocess_shell(
            self._commend,
            stdout=self._stdout,
            stderr=self._stderr
        )

        self._process_id = self._process.pid

        return True

    def stop(self):

        if self._process is None:
            return False

        self._process.kill()

        return True

    async def wait(self, timeout=0):

        try:

            await asyncio.wait_for(self._process.wait(), timeout=timeout)

        except Exception as err:

            self._process.kill()

            Utils.log.error(err)
