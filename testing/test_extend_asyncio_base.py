# -*- coding: utf-8 -*-

import pytest

from hagworm.extend.asyncio.base import Utils, MultiTasks, SliceTasks, QueueTasks, ShareFuture, async_adapter
from hagworm.extend.asyncio.base import FutureWithTimeout, AsyncCirculator, AsyncContextManager, AsyncFuncWrapper
from hagworm.extend.asyncio.base import Transaction, FuncCache


pytestmark = pytest.mark.asyncio


class TestUtils:

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

        time1 = Utils.loop_time()

        await FutureWithTimeout(1)

        time2 = Utils.loop_time()

        assert Utils.math.floor(time2 - time1) == 1

    async def test_slice_tasks(self):

        async def _do_acton():
            await Utils.sleep(1)

        time1 = Utils.loop_time()

        tasks = SliceTasks(3)

        for _ in range(10):
            tasks.append(_do_acton())

        await tasks

        time2 = Utils.loop_time()

        assert Utils.math.floor(time2 - time1) == 4

    async def test_queue_tasks(self):

        async def _do_acton(val):
            await Utils.sleep(val)

        time1 = Utils.loop_time()

        tasks = QueueTasks(5)

        tasks.append(_do_acton(8))

        for _ in range(2):

            tasks.append(_do_acton(4))

            for _ in range(2):
                tasks.append(_do_acton(1))
                tasks.append(_do_acton(1))
                tasks.append(_do_acton(2))

        await tasks

        time2 = Utils.loop_time()

        assert Utils.math.floor(time2 - time1) == 8

    async def test_async_circulator(self):

        time1 = Utils.loop_time()

        async for _ in AsyncCirculator(1, 0xff):
            pass

        time2 = Utils.loop_time()

        assert Utils.math.floor(time2 - time1) == 1

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
