# -*- coding: utf-8 -*-

import time
import pytest

from hagworm.extend.asyncio.base import Utils, TimeDiff
from hagworm.extend.asyncio.future import ThreadWorker


pytestmark = pytest.mark.asyncio
# pytest.skip(allow_module_level=True)


class TestWorker:

    @ThreadWorker(1)
    def _temp_for_thread_worker(self, *args, **kwargs):
        time.sleep(1)
        return True

    async def test_thread_worker(self):

        time_diff = TimeDiff()

        assert await self._temp_for_thread_worker()
        assert await self._temp_for_thread_worker(1, 2)
        assert await self._temp_for_thread_worker(1, 2, t1=1, t2=2)

        assert Utils.math.floor(time_diff.check()[0]) == 3
