# -*- coding: utf-8 -*-

import pytest

from hagworm.extend.asyncio.base import Utils, TimeDiff
from hagworm.extend.asyncio.task import RateLimiter


pytestmark = pytest.mark.asyncio
# pytest.skip(allow_module_level=True)


class TestHTTPClient:

    async def test_rate_limiter(self):

        async def _temp():
            await Utils.sleep(1)

        limiter = RateLimiter(2, 1)

        time_diff = TimeDiff()

        limiter.append(_temp)
        limiter.append(_temp)

        await limiter(_temp)

        assert Utils.math.floor(time_diff.check()[0]) == 2
