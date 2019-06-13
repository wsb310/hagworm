# -*- coding: utf-8 -*-

from crontab import CronTab

from tornado.gen import coroutine
from tornado.concurrent import Future
from tornado.ioloop import IOLoop, PeriodicCallback

from hagworm.extend.base import Utils


class AsyncTasks:

    def __init__(self):

        self._io_loop = IOLoop.current()

        self._interval = {}
        self._schedule = {}

    @coroutine
    def _wrapper(self, _callable, *args, **kwargs):

        result = _callable(*args, **kwargs)

        if isinstance(result, Future):
            yield result

    def add_timeout(self, delay, _callable, *args, **kwargs):
        """添加延时执行，支持协程函数和同步函数
        """

        result = self._io_loop.call_later(
            delay, self._wrapper, _callable, *args, **kwargs
        )

        return result

    def remove_timeout(self, timeout):
        """移除延时执行
        """

        result = self._io_loop.remove_timeout(timeout)

        return result

    def add_interval(self, interval, _callable, *args, promptly=False, **kwargs):
        """添加间隔任务，支持协程函数和同步函数
        """

        if args or kwargs:
            _callable = Utils.func_partial(_callable, *args, **kwargs)

        task = LoopTask(_callable, interval)

        self._interval[task.name] = task

        task.start(promptly)

        return task

    def remove_interval(self, name):
        """移除间隔任务
        """

        if name in self._interval:
            self._interval.pop(name).stop()

    def remove_all_interval(self):
        """移除全部间隔任务
        """

        for task in self._interval.values():
            task.stop()

        self._interval.clear()

    def add_schedule(self, crontab, _callable, *args, **kwargs):
        """添加计划任务，支持协程函数和同步函数
        """

        if args or kwargs:
            _callable = Utils.func_partial(_callable, *args, **kwargs)

        task = CronTask(crontab, _callable)

        self._schedule[task.name] = task

        task.start()

        return task

    def remove_schedule(self, name):
        """移除计划任务
        """

        if name in self._schedule:
            self._schedule.pop(name).stop()

    def remove_all_schedule(self):
        """移除全部计划任务
        """

        for task in self._schedule.values():
            task.stop()

        self._schedule.clear()


class LoopTask(PeriodicCallback):

    def __init__(self, _callable, interval):

        super().__init__(_callable, interval)

        self._name = Utils.uuid1()

    @coroutine
    def _run(self):

        if not self._running:
            return

        try:

            result = self.callback

            if isinstance(result, Future):
                yield result

        except Exception:

            self.io_loop.handle_callback_exception(self.callback)

        finally:

            self._schedule_next()

    def _schedule_next(self):

        if self._running:
            self._next_timeout = self.io_loop.time() + self.callback_time

            self._timeout = self.io_loop.add_timeout(
                self._next_timeout, self._run)

    @property
    def name(self):

        return self._name


class CronTask(CronTab, LoopTask):

    def __init__(self, crontab, _callable):
        CronTab.__init__(self, crontab)
        LoopTask.__init__(self, _callable, self.next())

    def _schedule_next(self):
        if self._running:
            self.callback_time = self.next()

            super()._schedule_next()
