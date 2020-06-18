# -*- coding: utf-8 -*-

import pytest

from hagworm.extend.base import Ignore
from hagworm.extend.asyncio.base import Utils, MultiTasks, SliceTasks, QueueTasks, ShareFuture, async_adapter
from hagworm.extend.asyncio.base import FutureWithTimeout, AsyncConstructor, AsyncCirculator, AsyncCirculatorForSecond
from hagworm.extend.asyncio.base import AsyncContextManager, AsyncFuncWrapper, FuncCache, TimeDiff
from hagworm.extend.asyncio.transaction import Transaction


pytestmark = pytest.mark.asyncio
# pytest.skip(allow_module_level=True)


class TestUtils:

    async def test_context_manager_1(self):

        class _ContextManager(AsyncContextManager):
            async def _context_release(self):
                pass

        result = False

        try:

            async with _ContextManager():
                raise Ignore()

            result = True

        except Ignore:

            result = False

        return result

    async def test_context_manager_2(self):

        class _ContextManager(AsyncContextManager):
            async def _context_release(self):
                pass

        result = False

        try:

            async with _ContextManager():

                async with _ContextManager():
                    raise Ignore(layers=2)

                result = False

            result = True

        except Ignore:

            result = False

        return result

    async def test_multi_tasks_and_share_future(self):

        @ShareFuture()
        async def _do_acton():
            await Utils.sleep(1)
            return Utils.randint(0, 0xffff)

        assert (await _do_acton()) != (await _do_acton())

        tasks = MultiTasks()

        for _ in range(0xff):
            tasks.append(_do_acton())

        await tasks

        assert len(set(tasks)) == 1

    async def test_async_adapter_and_wait_frame(self):

        result = False

        async def _temp():
            nonlocal result
            result = True
            await Utils.wait_frame(0xf0)

        async_adapter(_temp)()

        await Utils.wait_frame(0xff)

        assert result

    async def test_future_with_timeout(self):

        time_diff = TimeDiff()

        await FutureWithTimeout(1)

        assert Utils.math.floor(time_diff.check()[0]) == 1

    async def test_slice_tasks(self):

        async def _do_acton():
            await Utils.sleep(1)

        time_diff = TimeDiff()

        tasks = SliceTasks(3)

        for _ in range(10):
            tasks.append(_do_acton())

        await tasks

        assert Utils.math.floor(time_diff.check()[0]) == 4

    async def test_queue_tasks(self):

        async def _do_acton(val):
            await Utils.sleep(val)

        time_diff = TimeDiff()

        tasks = QueueTasks(5)

        tasks.append(_do_acton(8))

        for _ in range(2):

            tasks.append(_do_acton(4))

            for _ in range(2):
                tasks.append(_do_acton(1))
                tasks.append(_do_acton(1))
                tasks.append(_do_acton(2))

        await tasks

        assert Utils.math.floor(time_diff.check()[0]) == 8

    async def test_async_constructor(self):

        result = False

        class _Temp(AsyncConstructor):

            async def __ainit__(self):
                nonlocal result
                result = True

        temp = await _Temp()

        assert isinstance(temp, _Temp)
        assert result

    async def test_async_circulator_1(self):

        time_diff = TimeDiff()

        async for _ in AsyncCirculator(1, 0xff):
            pass

        assert Utils.math.floor(time_diff.check()[0]) == 1

    async def test_async_circulator_2(self):

        async for index in AsyncCirculator(0, 0xff, 0xff):
            pass
        else:
            assert index == 0xff

    async def test_async_circulator_for_second_1(self):

        time_diff = TimeDiff()

        async for index in AsyncCirculatorForSecond(1, 0.1):
            pass

        assert (index >= 9) and (index <= 11)
        assert Utils.math.floor(time_diff.check()[0]) == 1

    async def test_async_circulator_for_second_2(self):

        time_diff = TimeDiff()

        async for index in AsyncCirculatorForSecond(0, 0.1, 10):
            pass
        else:
            assert index == 10

        check_time = time_diff.check()[0]

        assert (check_time >= 0.9) and (check_time <= 1.1)

    async def test_async_context_manager(self):

        result = False

        class _Temp(AsyncContextManager):

            async def _context_release(self):
                nonlocal result
                result = True
                await Utils.wait_frame(0xff)

        async with _Temp() as temp:
            pass

        assert result

    async def test_async_func_wrapper(self):

        result1 = False
        result2 = False

        async def _temp1():
            nonlocal result1
            result1 = True
            await Utils.wait_frame(0xff)

        async def _temp2():
            nonlocal result2
            result2 = True
            await Utils.wait_frame(0xff)

        wrapper = AsyncFuncWrapper()

        wrapper.add(_temp1)
        wrapper.add(_temp2)

        await wrapper()

        assert result1 and result2

    async def test_transaction(self):

        result = []

        def _temp1():
            nonlocal result
            result.append(True)

        async def _temp2():
            nonlocal result
            result.append(True)
            await Utils.wait_frame(0xff)

        async with Transaction() as trx1:
            trx1.add_commit_callback(_temp1)
            trx1.add_commit_callback(_temp2)
            await trx1.commit()

        assert all(result) and len(result) == 2

        async with Transaction() as trx2:
            trx2.add_rollback_callback(_temp1)
            trx2.add_rollback_callback(_temp2)
            await trx1.rollback()

        assert all(result) and len(result) == 4

        async with Transaction() as trx3:
            trx3.add_rollback_callback(_temp1)
            trx3.add_rollback_callback(_temp2)

        assert all(result) and len(result) == 6

    async def test_func_cache(self):

        @FuncCache(ttl=1)
        async def _do_acton():
            return Utils.randint(0, 0xffff)

        res1 = await _do_acton()
        res2 = await _do_acton()

        assert res1 == res2

        await Utils.sleep(1)
        res3 = await _do_acton()

        assert res1 != res3
