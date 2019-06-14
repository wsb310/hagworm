# -*- coding: utf-8 -*-

import types
import inspect
import asyncio
import functools

import aiotask_context as context

from hagworm.extend import base


class Launcher:

    def __init__(self, log_file_path=None, log_level=r'INFO', log_file_num_backups=7, debug=False):

        if log_file_path:

            _log_file_path = Utils.path.join(
                log_file_path,
                r'runtime_{time}.log'
            )

            Utils.log.add(
                sink=_log_file_path,
                level=log_level,
                enqueue=True,
                backtrace=debug,
                rotation=r'00:00',
                retention=log_file_num_backups
            )

        else:

            Utils.log.level(log_level)

        self._event_loop = asyncio.get_event_loop()
        self._event_loop.set_task_factory(context.task_factory)

    def run(self, future):

        self._event_loop.run_until_complete(future)


class Utils(base.Utils):

    sleep = staticmethod(asyncio.sleep)

    @staticmethod
    @types.coroutine
    def wait_frame(count=10):

        for _ in range(max(1, count)):
            yield

    @staticmethod
    def call_soon(callback, *args):

        loop = asyncio.events.get_event_loop()

        return loop.call_soon(callback, *args)

    @staticmethod
    def call_later(delay, callback, *args):

        loop = asyncio.events.get_event_loop()

        return loop.call_later(delay, callback, *args)

    @staticmethod
    def call_at(when, callback, *args):

        loop = asyncio.events.get_event_loop()

        return loop.call_at(when, callback, *args)

    @staticmethod
    def ensure_future(future):

        return asyncio.ensure_future(future)

    @staticmethod
    def run_until_complete(future):

        loop = asyncio.events.get_event_loop()

        return loop.run_until_complete(future)


class FutureWithTimeout(asyncio.Future):

    def __init__(self, delay):

        super().__init__()

        self._timeout_handle = Utils.call_later(
            delay,
            self.set_result,
            None
        )

        self.add_done_callback(self._clear_timeout)

    def _clear_timeout(self, *_):

        if self._timeout_handle is not None:
            self._timeout_handle.cancel()
            self._timeout_handle = None


class MultiTasks:

    def __init__(self):

        self._tasks = []

    def __await__(self):

        if len(self._tasks) > 0:
            yield from asyncio.wait(self._tasks).__await__()

        return self

    def __len__(self):

        return self._tasks.__len__()

    def __iter__(self):

        for task in self._tasks:
            yield task.result()

    def append(self, future):

        return self._tasks.append(asyncio.ensure_future(future))

    def extend(self, futures):

        return self._tasks.extend(asyncio.ensure_future(future) for future in futures)

    def clear(self):

        return self._tasks.clear()


class AsyncContextManager:

    def __del__(self):

        Utils.ensure_future(self._context_release())

    async def __aenter__(self):

        return self

    async def __aexit__(self, *args):

        await self._context_release()

        if args[0] is base.Ignore:

            return True

        elif args[1]:

            Utils.log.exception(args[1], exc_info=args[2])

            return True

    async def _context_release(self):

        raise NotImplementedError()


class Transaction(AsyncContextManager):

    def __init__(self, *, commit_callback=None, rollback_callback=None):

        self._commit_callbacks = set()
        self._rollback_callbacks = set()

        if commit_callback:
            self._commit_callbacks.add(commit_callback)

        if rollback_callback:
            self._rollback_callbacks.add(rollback_callback)

    async def _context_release(self):

        await self.rollback()

    def add_commit_callback(self, _callable, *arg, **kwargs):

        self._commit_callbacks.add(Utils.func_partial(_callable, *arg, **kwargs))

    def add_rollback_callback(self, _callable, *arg, **kwargs):

        self._rollback_callbacks.add(Utils.func_partial(_callable, *arg, **kwargs))

    async def commit(self):

        if self._commit_callbacks is None:
            return

        for _callable in self._commit_callbacks:

            _res = _callable()

            if inspect.isawaitable(_res):
                await _res

        self._commit_callbacks.clear()
        self._commit_callbacks = None

    async def rollback(self):

        if self._rollback_callbacks is None:
            return

        for _callable in self._rollback_callbacks:

            _res = _callable()

            if inspect.isawaitable(_res):
                await _res

        self._rollback_callbacks.clear()
        self._rollback_callbacks = None


class FuncCache:

    def __init__(self, maxsize=0xff, ttl=10):

        self._cache = base.StackCache(maxsize, ttl)

    def __call__(self, func):

        @functools.wraps(func)
        async def _wrapper(*args, **kwargs):

            func_sign = Utils.params_sign(func, *args, **kwargs)

            result = self._cache.get(func_sign)

            if result is None:

                result = await func(*args, **kwargs)

                self._cache.set(func_sign, result)

            return result

        return _wrapper


class ShareFuture:

    def __init__(self):

        self._future = {}

    def __call__(self, func):

        @functools.wraps(func)
        async def _wrapper(*args, **kwargs):

            future = None

            func_sign = Utils.params_sign(func, *args, **kwargs)

            if func_sign in self._future:

                future = asyncio.Future()

                self._future[func_sign].append(future)

            else:

                future = func(*args, **kwargs)

                if not inspect.isawaitable(future):
                    TypeError(r'Need awaitable object')

                self._future[func_sign] = [future]

                future.add_done_callback(
                    Utils.func_partial(self._clear_future, func_sign)
                )

            return await future

        return _wrapper

    def _clear_future(self, func_sign, _):

        if func_sign not in self._future:
            return

        futures = self._future.pop(func_sign)

        result = futures.pop(0).result()

        for future in futures:
            future.set_result(Utils.deepcopy(result))
