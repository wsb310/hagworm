# -*- coding: utf-8 -*-

import pytest

from hagworm.extend.asyncio.base import Utils
from hagworm.extend.struct import KeyLowerDict


pytestmark = pytest.mark.asyncio
# pytest.skip(allow_module_level=True)


class TestStruct:

    async def test_key_lower_dict(self):

        temp1 = {
            r'MySQL1': {
                r'MySQL1': Utils.randint(0x10, 0xff),
                r'MYSQL2': Utils.randint(0x10, 0xff),
                r'Mysql3': Utils.randint(0x10, 0xff),
            },
            r'MYSQL2': {
                r'MySQL1': {
                    r'MySQL1': Utils.randint(0x10, 0xff),
                    r'MYSQL2': Utils.randint(0x10, 0xff),
                    r'Mysql3': Utils.randint(0x10, 0xff),
                },
                r'MYSQL2': {
                    r'MySQL1': Utils.randint(0x10, 0xff),
                    r'MYSQL2': Utils.randint(0x10, 0xff),
                    r'Mysql3': Utils.randint(0x10, 0xff),
                },
                r'Mysql3': {
                    r'MySQL1': Utils.randint(0x10, 0xff),
                    r'MYSQL2': Utils.randint(0x10, 0xff),
                    r'Mysql3': Utils.randint(0x10, 0xff),
                },
            },
            r'Mysql3': {
                r'MySQL1': {
                    r'MySQL1': {
                        r'MySQL1': Utils.randint(0x10, 0xff),
                        r'MYSQL2': Utils.randint(0x10, 0xff),
                        r'Mysql3': Utils.randint(0x10, 0xff),
                    },
                    r'MYSQL2': {
                        r'MySQL1': Utils.randint(0x10, 0xff),
                        r'MYSQL2': Utils.randint(0x10, 0xff),
                        r'Mysql3': Utils.randint(0x10, 0xff),
                    },
                    r'Mysql3': {
                        r'MySQL1': Utils.randint(0x10, 0xff),
                        r'MYSQL2': Utils.randint(0x10, 0xff),
                        r'Mysql3': Utils.randint(0x10, 0xff),
                    },
                },
                r'MYSQL2': {
                    r'MySQL1': {
                        r'MySQL1': Utils.randint(0x10, 0xff),
                        r'MYSQL2': Utils.randint(0x10, 0xff),
                        r'Mysql3': Utils.randint(0x10, 0xff),
                    },
                    r'MYSQL2': {
                        r'MySQL1': Utils.randint(0x10, 0xff),
                        r'MYSQL2': Utils.randint(0x10, 0xff),
                        r'Mysql3': Utils.randint(0x10, 0xff),
                    },
                    r'Mysql3': {
                        r'MySQL1': Utils.randint(0x10, 0xff),
                        r'MYSQL2': Utils.randint(0x10, 0xff),
                        r'Mysql3': Utils.randint(0x10, 0xff),
                    },
                },
                r'Mysql3': {
                    r'MySQL1': {
                        r'MySQL1': Utils.randint(0x10, 0xff),
                        r'MYSQL2': Utils.randint(0x10, 0xff),
                        r'Mysql3': Utils.randint(0x10, 0xff),
                    },
                    r'MYSQL2': {
                        r'MySQL1': Utils.randint(0x10, 0xff),
                        r'MYSQL2': Utils.randint(0x10, 0xff),
                        r'Mysql3': Utils.randint(0x10, 0xff),
                    },
                    r'Mysql3': {
                        r'MySQL1': Utils.randint(0x10, 0xff),
                        r'MYSQL2': Utils.randint(0x10, 0xff),
                        r'Mysql3': Utils.randint(0x10, 0xff),
                    },
                },
            },
        }

        temp2 = KeyLowerDict(temp1)

        assert r'my_sql1' in temp2 and r'mysql2' in temp2 and r'mysql3' in temp2
