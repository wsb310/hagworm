# -*- coding: utf-8 -*-

import types
import weakref
import inspect
import asyncio
import functools

from contextvars import Context, ContextVar

from hagworm import package_slogan
from hagworm import __version__ as package_version
from hagworm.extend import base
from hagworm.extend.cache import StackCache
from hagworm.extend.interface import RunnableInterface


def install_uvloop():
    """尝试安装uvloop
    """

    try:
        import uvloop
    except ModuleNotFoundError:
        Utils.log.warning(f'uvloop is not supported')
    else:
        uvloop.install()
        Utils.log.success(f'uvloop {uvloop.__version__} installed')


class Launcher(RunnableInterface):
    """异步版本的启动器

    用于简化和统一程序的启动操作

    """

    def __init__(self,
                 log_file_path=None, log_level=r'INFO', log_file_num_backups=7,
                 process_number=1, process_guardian=None,
                 debug=False
                 ):

        self._process_id = 0
        self._process_number = process_number

        if log_file_path:

            _log_file_path = Utils.path.join(
                log_file_path,
                r'runtime_{time}.log'
            )

            Utils.log.add(
                _log_file_path,
                level=log_level,
                enqueue=True,
                backtrace=debug,
                rotation=r'00:00',
                retention=log_file_num_backups
            )

        else:

            Utils.log.level(log_level)

        environment = Utils.environment()

        Utils.log.info(
            f'{package_slogan}'
            f'hagworm {package_version}\n'
            f'python {environment["python"]}\n'
            f'system {" ".join(environment["system"])}'
        )

        install_uvloop()

        if self._process_number > 1:
            self._process_id = base.fork_processes(self._process_number, process_guardian)

        self._event_loop = asyncio.get_event_loop()
        self._event_loop.set_debug(debug)

    @property
    def process_id(self):

        return self._process_id

    def run(self, func, *args, **kwargs):

        Utils.log.success(f'Start process no.{self._process_id}')

        self._event_loop.run_until_complete(func(*args, **kwargs))

        Utils.log.success(f'Stop process no.{self._process_id}')


class Utils(base.Utils):
    """异步基础工具类

    集成常用的异步工具函数

    """

    sleep = staticmethod(asyncio.sleep)

    @staticmethod
    async def awaitable_wrapper(obj):
        """自适应awaitable对象
        """

        if inspect.isawaitable(obj):
            return await obj
        else:
            return obj

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

    def __init__(self, delay, default=None):

        super().__init__()

        self._timeout_handle = Utils.call_later(
            delay,
            self.set_result,
            default
        )

        self.add_done_callback(self._clear_timeout)

    def _clear_timeout(self, *_):

        if self._timeout_handle is not None:
            self._timeout_handle.cancel()
            self._timeout_handle = None


class FutureWithTask(asyncio.Future, RunnableInterface):
    """Future实例可以被多个协程await，本类实现Future和Task的桥接
    """

    def __init__(self, func, name=None):

        super().__init__()

        self._name = Utils.uuid1() if name is None else name
        self._task = None
        self._callable = func
        self._build_time = Utils.loop_time()

    @property
    def name(self):

        return self._name

    @property
    def task(self):

        return self._task

    @property
    def build_time(self):

        return self._build_time

    def run(self):

        if self._task is None:
            self._task = asyncio.create_task(self._run())

        return self

    async def _run(self):

        result = None

        try:

            result = await self._callable()

            self.set_result(result)

        except Exception as err:

            self.set_exception(err)

        return result


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

        self._coro_list = list(args)
        self._task_list = []

    def __await__(self):

        if len(self._coro_list) > 0:
            self._task_list = [Utils.create_task(coro) for coro in self._coro_list]
            self._coro_list.clear()
            yield from asyncio.gather(*self._task_list).__await__()

        return [task.result() for task in self._task_list]

    def __len__(self):

        return self._coro_list.__len__()

    def __iter__(self):

        for task in self._task_list:
            yield task.result()

    def __getitem__(self, item):

        return self._task_list.__getitem__(item).result()

    def append(self, coro):

        return self._coro_list.append(coro)

    def extend(self, coro_list):

        return self._coro_list.extend(coro_list)

    def clear(self):

        self._coro_list.clear()
        self._task_list.clear()


class SliceTasks(MultiTasks):
    """多任务分片并发管理器

    继承自MultiTasks类，通过参数slice_num控制并发分片任务数

    """

    def __init__(self, slice_num, *args):

        super().__init__(*args)

        self._slice_num = max(1, slice_num)

    def __await__(self):

        if len(self._coro_list) > 0:

            for _ in range(Utils.math.ceil(len(self._coro_list) / self._slice_num)):

                tasks = []

                for _ in range(self._slice_num):
                    if len(self._coro_list) > 0:
                        tasks.append(Utils.create_task(self._coro_list.pop(0)))

                if len(tasks) > 0:
                    self._task_list.extend(tasks)
                    yield from asyncio.gather(*tasks).__await__()

        return [task.result() for task in self._task_list]


class QueueTasks(MultiTasks):
    """多任务队列管理器

    继承自MultiTasks类，通过参数queue_num控制队列长度

    """

    def __init__(self, queue_num, *args):

        super().__init__(*args)

        self._queue_num = max(1, queue_num)

        self._queue_future = None

    def __await__(self):

        if len(self._coro_list) > 0:

            self._queue_future = asyncio.Future()

            tasks = []

            for _ in range(self._queue_num):

                if len(self._coro_list) > 0:

                    task = Utils.create_task(self._coro_list.pop(0))
                    task.add_done_callback(self._done_callback)

                    tasks.append(task)

            if len(tasks) > 0:
                self._task_list.extend(tasks)

            yield from self._queue_future.__await__()

        return [task.result() for task in self._task_list]

    def _done_callback(self, _):

        if len(self._coro_list) > 0:

            task = Utils.create_task(self._coro_list.pop(0))
            task.add_done_callback(self._done_callback)

            self._task_list.append(task)

        elif self._queue_future is not None and not self._queue_future.done():

            if all(task.done() for task in self._task_list):
                self._queue_future.set_result(True)


class AsyncCirculator:
    """异步循环器

    提供一个循环体内的代码重复执行管理逻辑，可控制超时时间、执行间隔(LoopFrame)和最大执行次数

    async for index in AsyncCirculator():
        pass

    其中index为执行次数，从1开始

    """

    def __init__(self, timeout=0, interval=0xff, max_times=0):

        if timeout > 0:
            self._expire_time = Utils.loop_time() + timeout
        else:
            self._expire_time = 0

        self._interval = interval
        self._max_times = max_times

        self._current = 0

    def __aiter__(self):

        return self

    async def __anext__(self):

        if self._current > 0:

            if (self._max_times > 0) and (self._max_times <= self._current):
                raise StopAsyncIteration()

            if (self._expire_time > 0) and (self._expire_time <= Utils.loop_time()):
                raise StopAsyncIteration()

            await self._sleep()

        self._current += 1

        return self._current

    async def _sleep(self):

        await Utils.wait_frame(self._interval)


class AsyncCirculatorForSecond(AsyncCirculator):

    def __init__(self, timeout=0, interval=1, max_times=0):

        super().__init__(timeout, interval, max_times)

    async def _sleep(self):

        await Utils.sleep(self._interval)


class AsyncContextManager:
    """异步上下文资源管理器

    子类通过实现_context_release接口，方便的实现with语句管理上下文资源释放

    """

    async def __aenter__(self):

        await self._context_initialize()

        return self

    async def __aexit__(self, exc_type, exc_value, traceback):

        await self._context_release()

        if exc_type is base.Ignore:

            return not exc_value.throw()

        elif exc_value:

            Utils.log.exception(exc_value)

            return True

    async def _context_initialize(self):

        pass

    async def _context_release(self):

        raise NotImplementedError()


class FuncWrapper(base.FuncWrapper):
    """非阻塞异步函数包装器

    将多个同步或异步函数包装成一个可调用对象

    """

    def __call__(self, *args, **kwargs):

        for func in self._callables:
            Utils.call_soon(func, *args, **kwargs)


class AsyncConstructor:
    """实现了__ainit__异步构造函数
    """

    def __await__(self):

        yield from self.__ainit__().__await__()

        return self

    async def __ainit__(self):

        raise NotImplementedError()


class AsyncFuncWrapper(base.FuncWrapper):
    """阻塞式异步函数包装器

    将多个同步或异步函数包装成一个可调用对象

    """

    async def __call__(self, *args, **kwargs):

        for func in self._callables:

            try:

                await Utils.awaitable_wrapper(
                    func(*args, **kwargs)
                )

            except Exception as err:

                Utils.log.error(err)


class FuncCache:
    """函数缓存

    使用堆栈缓存实现的函数缓存，在有效期内函数签名一致就会命中缓存

    """

    def __init__(self, maxsize=0xff, ttl=10):

        self._cache = StackCache(maxsize, ttl)

    def __call__(self, func):

        @functools.wraps(func)
        async def _wrapper(*args, **kwargs):

            func_sign = Utils.params_sign(func, *args, **kwargs)

            result = self._cache.get(func_sign)

            if result is None:

                result = await Utils.awaitable_wrapper(
                    func(*args, **kwargs)
                )

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


class TimeDiff:

    def __init__(self):

        self._start_time = self._last_check_time = Utils.loop_time()

    def check(self):

        now_time = Utils.loop_time()

        diff_time_1 = now_time - self._start_time
        diff_time_2 = now_time - self._last_check_time

        self._last_check_time = now_time

        return diff_time_1, diff_time_2
