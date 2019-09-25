# -*- coding: utf-8 -*-

import types
import weakref
import inspect
import asyncio
import functools

from contextvars import Context, ContextVar

from hagworm.extend import base


class Launcher:
    """异步版本的启动器

    用于简化和统一程序的启动操作

    """

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
        self._event_loop.set_debug(debug)

    def run(self, future):

        self._event_loop.run_until_complete(future)


class Utils(base.Utils):
    """异步基础工具类

    集成常用的异步工具函数

    """

    sleep = staticmethod(asyncio.sleep)
    isawaitable = staticmethod(inspect.isawaitable)

    @staticmethod
    @types.coroutine
    def wait_frame(count=10):
        """暂停指定帧数
        """

        for _ in range(max(1, count)):
            yield

    @staticmethod
    def loop_time():
        """获取当前loop内部时钟
        """

        loop = asyncio.events.get_event_loop()

        return loop.time()

    @classmethod
    def call_soon(cls, callback, *args, **kwargs):
        """延时调用(能隔离上下文)
        """

        loop = asyncio.events.get_event_loop()

        if kwargs:

            return loop.call_soon(
                async_adapter(
                    cls.func_partial(
                        callback,
                        *args,
                        **kwargs
                    )
                ),
                context=Context()
            )

        else:

            return loop.call_soon(
                async_adapter(callback),
                *args,
                context=Context()
            )

    @classmethod
    def call_soon_threadsafe(cls, callback, *args, **kwargs):
        """延时调用(线程安全，能隔离上下文)
        """

        loop = asyncio.events.get_event_loop()

        if kwargs:

            return loop.call_soon_threadsafe(
                async_adapter(
                    cls.func_partial(
                        callback,
                        *args,
                        **kwargs
                    )
                ),
                context=Context()
            )

        else:

            return loop.call_soon_threadsafe(
                async_adapter(callback),
                *args,
                context=Context()
            )

    @classmethod
    def call_later(cls, delay, callback, *args, **kwargs):
        """延时指定秒数调用(能隔离上下文)
        """

        loop = asyncio.events.get_event_loop()

        if kwargs:

            return loop.call_later(
                delay,
                async_adapter(
                    cls.func_partial(
                        callback,
                        *args,
                        **kwargs
                    )
                ),
                context=Context()
            )

        else:

            return loop.call_later(
                delay,
                async_adapter(callback),
                *args,
                context=Context()
            )

    @classmethod
    def call_at(cls, when, callback, *args, **kwargs):
        """指定时间调用(能隔离上下文)
        """

        loop = asyncio.events.get_event_loop()

        if kwargs:

            return loop.call_at(
                when,
                async_adapter(
                    cls.func_partial(
                        callback,
                        *args,
                        **kwargs
                    )
                ),
                context=Context()
            )

        else:

            return loop.call_at(
                when,
                async_adapter(callback),
                *args,
                context=Context()
            )

    @staticmethod
    def create_task(coro):
        """将协程对象包装成task对象(兼容Future接口)
        """

        if asyncio.iscoroutine(coro):
            return asyncio.create_task(coro)
        else:
            return None

    @staticmethod
    def run_until_complete(future):
        """将协程对象包装成task对象(兼容Future接口)
        """

        loop = asyncio.events.get_event_loop()

        return loop.run_until_complete(future)


def async_adapter(func):
    """异步函数适配装饰器

    使异步函数可以在同步函数中调用，即非阻塞式的启动异步函数，同时会影响上下文资源的生命周期

    """

    @functools.wraps(func)
    def _wrapper(*args, **kwargs):

        return Utils.create_task(
            func(*args, **kwargs)
        )

    return _wrapper


class WeakContextVar:
    """弱引用版的上下文资源共享器
    """

    _instances = {}

    def __new__(cls, name):

        if name in cls._instances:
            inst = cls._instances[name]
        else:
            inst = cls._instances[name] = super().__new__(cls)

        return inst

    def __init__(self, name):

        self._context_var = ContextVar(name, default=None)

    def get(self):

        ref = self._context_var.get()

        return None if ref is None else ref()

    def set(self, value):

        return self._context_var.set(weakref.ref(value))


class FutureWithTimeout(asyncio.Future):
    """带超时功能的Future
    """

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
    """多任务并发管理器

    提供协程的多任务并发的解决方案

    tasks = MultiTasks()
    tasks.append(func1())
    tasks.append(func2())
    ...
    tasks.append(funcN())
    await tasks

    多任务中禁止使用上下文资源共享的对象(如mysql和redis等)
    同时需要注意类似这种不能同时为多个协程提供服务的对象会造成不可预期的问题

    """

    def __init__(self, *args):

        self._tasks = list(args)

    def __await__(self):

        if len(self._tasks) > 0:
            yield from asyncio.gather(*self._tasks).__await__()

        return self

    def __len__(self):

        return self._tasks.__len__()

    def __iter__(self):

        for task in self._tasks:
            yield task.result()

    def __getitem__(self, item):

        return self._tasks.__getitem__(item).result()

    def append(self, coro):

        return self._tasks.append(Utils.create_task(coro))

    def extend(self, coro_list):

        return self._tasks.extend(Utils.create_task(coro) for coro in coro_list)

    def clear(self):

        return self._tasks.clear()


class AsyncCirculator:
    """异步循环器

    提供一个循环体内的代码重复执行管理逻辑，可控制执行间隔和超时时间

    for index in AsyncCirculator():
        pass

    其中index为执行次数，从1开始

    """

    def __init__(self, timeout=0, interval=10):

        if timeout > 0:
            self._expire_time = Utils.timestamp() + timeout
        else:
            self._expire_time = 0

        self._interval = interval

        self._current = 0

    def __aiter__(self):

        return self

    async def __anext__(self):

        if self._current > 0:

            if self._expire_time > 0 and self._expire_time < Utils.timestamp():
                raise StopAsyncIteration()

            await Utils.wait_frame(self._interval)

        self._current += 1

        return self._current


class AsyncContextManager:
    """异步上下文资源管理器

    子类通过实现_context_release接口，方便的实现with语句管理上下文资源释放

    """

    async def __aenter__(self):

        return self

    async def __aexit__(self, *args):

        await self._context_release()

        if args[0] is base.Ignore:

            return True

        elif args[1]:

            Utils.log.exception(args[1])

            return True

    async def _context_release(self):

        raise NotImplementedError()


class AsyncFuncWrapper(base.FuncWrapper):
    """异步函数包装器

    将多个同步或异步函数包装成一个可调用对象

    """

    async def __call__(self, *args, **kwargs):

        for func in self._callables:

            future = func(*args, **kwargs)

            if Utils.isawaitable(future):
                await future


class Transaction(AsyncContextManager):
    """事务对象

    使用异步上下文实现的一个事务对象，可以设置commit和rollback回调
    未显示commit的情况下，会自动rollback

    """

    def __init__(self):

        self._commit_callbacks = set()
        self._rollback_callbacks = set()

    async def _context_release(self):

        await self.rollback()

    def add_commit_callback(self, _callable):

        if self._commit_callbacks is None:
            return

        self._commit_callbacks.add(_callable)

    def add_rollback_callback(self, _callable):

        if self._rollback_callbacks is None:
            return

        self._rollback_callbacks.add(_callable)

    async def commit(self):

        if self._commit_callbacks is None:
            return

        callbacks = self._commit_callbacks

        self._commit_callbacks = self._rollback_callbacks = None

        for _callable in callbacks:

            _res = _callable()

            if Utils.isawaitable(_res):
                await _res

    async def rollback(self):

        if self._rollback_callbacks is None:
            return

        callbacks = self._rollback_callbacks

        self._commit_callbacks = self._rollback_callbacks = None

        for _callable in callbacks:

            _res = _callable()

            if Utils.isawaitable(_res):
                await _res


class FuncCache:
    """函数缓存

    使用堆栈缓存实现的函数缓存，在有效期内函数签名一致就会命中缓存

    """

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
    """共享Future装饰器

    同一时刻并发调用函数时，使用该装饰器的函数签名一致的调用，会共享计算结果

    """

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

                future = Utils.create_task(
                    func(*args, **kwargs)
                )

                if future is None:
                    TypeError(r'Not Coroutine Object')

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
