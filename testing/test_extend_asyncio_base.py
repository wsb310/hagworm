# -*- coding: utf-8 -*-

import pytest

from hagworm.extend.asyncio.base import Utils, MultiTasks, ShareFuture


class TestMultiTasksAndShareFuture:

    @pytest.mark.asyncio
    async def test_multi_tasks_and_share_future(self):

        task = MultiTasks()

        for _ in range(0xff):
            task.append(self._do_acton())

        await task

        assert len(set(task)) == 1

    @ShareFuture()
    async def _do_acton(self):

        await Utils.sleep(1)

        return Utils.randint(0, 0xffff)
