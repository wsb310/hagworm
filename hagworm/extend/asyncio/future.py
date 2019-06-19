# -*- coding: utf-8 -*-

import asyncio
import inspect
import subprocess

from crontab import CronTab

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from hagworm.extend.interface import TaskInterface

from .base import Utils, async_adapter


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


class LoopTask(TaskInterface):

    @classmethod
    def create(cls, interval, promptly, _callable, *args, **kwargs):

        if args or kwargs:
            _callable = Utils.func_partial(_callable, *args, **kwargs)

        task = cls(_callable, interval)

        task.start(promptly)

        return task

    def __init__(self, _callable, interval):

        self._running = False
        self._next_timeout = 0

        self._event_loop = None
        self._timeout = None

        self._callable = _callable
        self._interval = interval

    def start(self, promptly=False, *, event_loop=None):

        if event_loop:
            self._event_loop = event_loop
        else:
            self._event_loop = asyncio.get_event_loop()

        self._running = True

        if promptly:
            self._event_loop.call_soon(self._run)
        else:
            self._schedule_next()

    def stop(self):

        self._running = False

        if self._event_loop is not None and self._timeout is not None:
            self._event_loop.remove_timeout(self._timeout)
            self._event_loop = None
            self._timeout = None

    def is_running(self):

        return self._running

    @async_adapter
    async def _run(self):

        if not self._running:
            return

        try:

            future = self._callable()

            if inspect.isawaitable(future):
                await future

        except Exception as err:

            Utils.log.error(err)

        finally:

            self._schedule_next()

    def _schedule_next(self):

        if self._running:
            self._timeout = self._event_loop.call_at(self._update_next(), self._run)

    def _update_next(self):

        self._next_timeout = self._event_loop.time() + self._interval

        return self._next_timeout


class CronTask(LoopTask, CronTab):

    @classmethod
    def create(cls, crontab, promptly, _callable, *args, **kwargs):

        if args or kwargs:
            _callable = Utils.func_partial(_callable, *args, **kwargs)

        task = cls(_callable, crontab)

        task.start(promptly)

        return task

    def __init__(self, _callable, crontab):

        LoopTask.__init__(self, _callable, 0)
        CronTab.__init__(self, crontab)

    def _update_next(self):

        self._next_timeout = self._event_loop.time() + self.next(default_utc=True)

        return self._next_timeout
