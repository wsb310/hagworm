# -*- coding: utf-8 -*-

import asyncio

from crontab import CronTab

from hagworm.extend.interface import TaskInterface

from .base import Utils


class _BaseTask(TaskInterface):
    """异步任务基类
    """

    @classmethod
    def create(cls, promptly, _callable, *args, **kwargs):

        if args or kwargs:
            _callable = Utils.func_partial(_callable, *args, **kwargs)

        task = cls(_callable)

        task.start(promptly)

        return task

    def __init__(self, _callable):

        self._running = False
        self._next_timeout = 0

        self._event_loop = None
        self._timeout = None

        self._callable = _callable

    def start(self, promptly=False, *, event_loop=None):

        if event_loop:
            self._event_loop = event_loop
        else:
            self._event_loop = asyncio.get_event_loop()

        self._running = True

        if promptly:
            self._timeout = Utils.call_soon(self._run)
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

    async def _run(self):

        if not self._running:
            return

        try:

            await Utils.awaitable_wrapper(
                self._callable()
            )

        except Exception as err:

            Utils.log.error(err)

        finally:

            self._schedule_next()

    def _schedule_next(self):

        if self._running:
            self._timeout = Utils.call_at(self._update_next(), self._run)

    def _update_next(self):

        self._next_timeout = self._event_loop.time()

        return self._next_timeout


class LoopTask(_BaseTask):
    """循环任务类
    """

    @classmethod
    def create(cls, limit_time, promptly, _callable, *args, **kwargs):

        if args or kwargs:
            _callable = Utils.func_partial(_callable, *args, **kwargs)

        task = cls(_callable, limit_time)

        task.start(promptly)

        return task

    def __init__(self, _callable, limit_time):

        super().__init__(_callable)

        self._limit_time = limit_time

    def _update_next(self):

        now_time = self._event_loop.time()
        next_timeout = self._next_timeout + self._limit_time

        if next_timeout < now_time:
            self._next_timeout = now_time
        else:
            self._next_timeout = next_timeout

        return self._next_timeout


class IntervalTask(_BaseTask):
    """间隔任务类
    """

    @classmethod
    def create(cls, interval, promptly, _callable, *args, **kwargs):

        if args or kwargs:
            _callable = Utils.func_partial(_callable, *args, **kwargs)

        task = cls(_callable, interval)

        task.start(promptly)

        return task

    def __init__(self, _callable, interval):

        super().__init__(_callable)

        self._interval = interval

    def _update_next(self):

        self._next_timeout = self._event_loop.time() + self._interval

        return self._next_timeout


class CronTask(_BaseTask, CronTab):
    """定时任务类
    """

    @classmethod
    def create(cls, crontab, promptly, _callable, *args, **kwargs):

        if args or kwargs:
            _callable = Utils.func_partial(_callable, *args, **kwargs)

        task = cls(_callable, crontab)

        task.start(promptly)

        return task

    def __init__(self, _callable, crontab):

        _BaseTask.__init__(self, _callable)
        CronTab.__init__(self, crontab)

    def _update_next(self):

        self._next_timeout = self._event_loop.time() + self.next(default_utc=True)

        return self._next_timeout
