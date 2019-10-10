# -*- coding: utf-8 -*-

from hagworm.extend.base import Utils, StackCache

from hagworm.extend.asyncio.net import HTTPClientPool
from hagworm.extend.asyncio.future import ThreadPool


class FileLoader:
    """带缓存的网络文件加载器
    """

    def __init__(self, maxsize=0xff, ttl=3600, thread=32):

        self._cache = StackCache(maxsize, ttl)

        self._thread_pool = ThreadPool(thread)
        self._http_client = HTTPClientPool(limit=thread)

    def _read(self, file):

        with open(file, r'rb') as stream:
            return stream.read()

    async def read(self, file):

        result = None

        try:

            if self._cache.exists(file):

                result = self._cache.get(file)

            else:

                result = await self._thread_pool.run(self._read, file)

                self._cache.set(file, result)

        except Exception as err:

            Utils.log.error(err)

        return result

    async def fetch(self, url, params=None, cookies=None, headers=None):

        result = None

        try:

            if self._cache.exists(url):

                result = self._cache.get(url)

            else:

                result = await self._http_client.get(url, params, cookies, headers)

                self._cache.set(url, result)

        except Exception as err:

            Utils.log.error(err)

        return result
