# -*- coding: utf-8 -*-

import pytest

from hagworm.extend.asyncio.base import Utils
from hagworm.extend.base import catch_error, Ignore, ContextManager, FuncWrapper, StackCache


pytestmark = pytest.mark.asyncio


class TestUtils:

    async def test_catch_error_1(self):

        result = False

        try:

            with catch_error():
                raise Ignore()

            result = True

        except Ignore:

            result = False

        return result

    async def test_catch_error_2(self):

        result = False

        try:

            with catch_error():

                with catch_error():
                    raise Ignore(layers=2)

                result = False

            result = True

        except Ignore:

            result = False

        return result

    async def test_context_manager_1(self):

        class _ContextManager(ContextManager):
            def _context_release(self):
                pass

        result = False

        try:

            with _ContextManager():
                raise Ignore()

            result = True

        except Ignore:

            result = False

        return result

    async def test_context_manager_2(self):

        class _ContextManager(ContextManager):
            def _context_release(self):
                pass

        result = False

        try:

            with _ContextManager():

                with _ContextManager():
                    raise Ignore(layers=2)

                result = False

            result = True

        except Ignore:

            result = False

        return result

    async def test_func_wrapper(self):

        result1 = False
        result2 = False

        def _temp1():
            nonlocal result1
            result1 = True

        def _temp2():
            nonlocal result2
            result2 = True

        wrapper = FuncWrapper()

        wrapper.add(_temp1)
        wrapper.add(_temp2)

        wrapper()

        assert result1 and result2

    async def test_stack_cache(self):

        cache = StackCache()

        ckey = Utils.uuid1()
        cval = Utils.uuid1()

        res1 = cache.has(ckey)
        res2 = cache.size()

        assert not res1 and res2 == 0

        cache.set(ckey, cval, 0.5)

        res3 = cache.has(ckey)
        res4 = cache.get(ckey)
        res5 = cache.size()

        assert res3 and res4 == cval and res5 == 1

        await Utils.sleep(1)

        res6 = cache.has(ckey)
        res7 = cache.get(ckey)
        res8 = cache.size()

        assert not res6 and res7 is None and res8 == 0
