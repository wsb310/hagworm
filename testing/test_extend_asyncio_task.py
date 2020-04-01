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
            return True

        limiter = RateLimiter(2, 5, 2)

        time_diff = TimeDiff()

        _f1 = limiter.append_with_name(r'f1', _temp)
        _f2 = limiter.append_with_name(r'f2', _temp)
        _f3 = limiter.append_with_name(r'f3', _temp)
        _f4 = limiter.append_with_name(r'f4', _temp)
        _f5 = limiter.append_with_name(r'f4', _temp)
        _f6 = limiter.append_with_name(r'f6', _temp)

        await _f1
        await _f2
        await _f3
        await _f4
        assert _f5 is None

        try:
            await _f6
        except:
            assert True
        else:
            assert False

        assert Utils.math.floor(time_diff.check()[0]) == 2
