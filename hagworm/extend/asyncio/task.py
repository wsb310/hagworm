# -*- coding: utf-8 -*-

import asyncio

from crontab import CronTab
from collections import OrderedDict

from hagworm.extend.interface import TaskInterface

from .base import Utils, FutureWithTask


class _BaseTask(TaskInterface):
    """异步任务基类
    """

    def __init__(self, _callable):

        self._running = False
        self._next_timeout = 0

        self._event_loop = None
        self._timeout_handle = None

        self._callable = _callable

    def start(self, promptly=False, *, event_loop=None):

        if event_loop:
            self._event_loop = event_loop
        else:
            self._event_loop = asyncio.get_event_loop()

        self._running = True

        if promptly:
            self._timeout_handle = Utils.call_soon(self._run)
        else:
            self._schedule_next()

    def stop(self):

        self._running = False

        if self._timeout_handle is not None:
            self._timeout_handle.cancel()
            self._timeout_handle = None

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
            self._timeout_handle = Utils.call_at(self._update_next(), self._run)

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

    def __init__(self, _callable, crontab, default_now=None, default_utc=None):

        _BaseTask.__init__(self, _callable)
        CronTab.__init__(self, crontab)

        self._default_now = default_now
        self._default_utc = default_utc

    def _update_next(self):

        params = {}

        if self._default_now is not None:
            params[r'now'] = self._default_now

        if self._default_utc is not None:
            params[r'default_utc'] = self._default_utc

        self._next_timeout = self._event_loop.time() + self.next(**params)

        return self._next_timeout


class RateLimiter:
    """流量控制器，用于对计算资源的保护
    添加任务append函数如果成功会返回Future对象，可以通过await该对象等待执行结果
    进入队列的任务，如果触发限流行为会通过在Future上引发CancelledError传递出来
    """

    def __init__(self, running_limit, waiting_limit=0, timeout=0):

        self._running_limit = running_limit
        self._waiting_limit = waiting_limit

        self._timeout = timeout

        self._running_tasks = OrderedDict()
        self._waiting_tasks = OrderedDict()

    @property
    def running_tasks(self):

        return list(self._running_tasks.values())

    @property
    def running_length(self):

        return len(self._running_tasks)

    @property
    def waiting_tasks(self):

        return list(self._waiting_tasks.values())

    @property
    def waiting_length(self):

        return len(self._waiting_tasks)

    def _create_task(self, name, func, *args, **kwargs):

        if len(args) == 0 and len(kwargs) == 0:
            return FutureWithTask(func, name)
        else:
            return FutureWithTask(Utils.func_partial(func, *args, **kwargs), name)

    def append(self, func, *args, **kwargs):

        return self._append(None, func, *args, **kwargs)

    def append_with_name(self, name, func, *args, **kwargs):

        return self._append(name, func, *args, **kwargs)

    def _append(self, name, func, *args, **kwargs):

        task = None

        task_tag = f"{name or r''} {func} {args or r''} {kwargs or r''}"

        if name is None or ((name not in self._running_tasks) and (name not in self._waiting_tasks)):

            if self._check_running_limit():

                task = self._create_task(name, func, *args, **kwargs)
                self._add_running_tasks(task)

                Utils.log.debug(f'rate limit add running tasks: {task_tag}')

            elif self._check_waiting_limit():

                task = self._create_task(name, func, *args, **kwargs)
                self._add_waiting_tasks(task)

                Utils.log.debug(f'rate limit add waiting tasks: {task_tag}')

            else:

                Utils.log.warning(
                    f'rate limit: {task_tag}\n'
                    f'running: {self.running_length}/{self.running_limit}\n'
                    f'waiting: {self.waiting_length}/{self.waiting_limit}'
                )

        else:

            Utils.log.warning(f'rate limit duplicate: {task_tag}')

        return task

    @property
    def running_limit(self):

        return self._running_limit

    @running_limit.setter
    def running_limit(self, val):

        self._running_limit = val

        self._recover_waiting_tasks()

    def _check_running_limit(self):

        return (self._running_limit <= 0) or (len(self._running_tasks) < self._running_limit)

    @property
    def waiting_limit(self):

        return self._waiting_limit

    @waiting_limit.setter
    def waiting_limit(self, val):

        self._waiting_limit = val

        if len(self._waiting_tasks) > self._waiting_limit:
            self._waiting_tasks = self._waiting_tasks[:self._waiting_limit]

    def _check_waiting_limit(self):

        return (self._waiting_limit <= 0) or (len(self._waiting_tasks) < self._waiting_limit)

    @property
    def timeout(self):

        return self._timeout

    @timeout.setter
    def timeout(self, val):

        self._timeout = val

    def _check_timeout(self, task):

        return (self._timeout <= 0) or ((Utils.loop_time() - task.build_time) < self._timeout)

    def _add_running_tasks(self, task):

        if not self._check_timeout(task):
            task.cancel()
            Utils.log.warning(f'rate limit timeout: {task.name} build_time:{task.build_time}')
        elif task.name in self._running_tasks:
            task.cancel()
            Utils.log.warning(f'rate limit duplicate: {task.name}')
        else:
            task.add_done_callback(self._done_callback)
            self._running_tasks[task.name] = task.run()

    def _add_waiting_tasks(self, task):

        if task.name not in self._waiting_tasks:
            self._waiting_tasks[task.name] = task
        else:
            task.cancel()
            Utils.log.warning(f'rate limit duplicate: {task.name}')

    def _recover_waiting_tasks(self):

        for _ in range(len(self._waiting_tasks)):

            if self._check_running_limit():
                item = self._waiting_tasks.popitem(False)
                self._add_running_tasks(item[1])
            else:
                break

    def _done_callback(self, task):

        if task.name in self._running_tasks:
            self._running_tasks.pop(task.name)

        self._recover_waiting_tasks()
