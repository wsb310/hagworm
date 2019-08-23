# -*- coding: utf-8 -*-

import os
import ssl
import aiohttp

from enum import Enum

from hagworm.extend.struct import FileBuffer

from .base import Utils


class _STATE(Enum):

    PENDING = 0x00
    FETCHING = 0x01
    FINISHED = 0x02


class _HTTPClient:

    def __init__(self, retry_count=5, read_timeout=60, conn_timeout=10, **kwargs):

        self._ssl_context = ssl.create_default_context(
            cafile=os.path.join(
                os.path.split(os.path.abspath(__file__))[0],
                r'../../static/cacert.pem'
            )
        )

        self._retry_count = retry_count

        self._session_config = kwargs
        self._session_config[r'read_timeout'] = read_timeout
        self._session_config[r'conn_timeout'] = conn_timeout

    def _sleep_for_retry(self, times):

        return Utils.sleep(times)

    async def request(self, method, url, data=None, params=None, **kwargs):

        headers = response = None

        if isinstance(data, dict):

            if headers is None:
                headers = {}

            headers.setdefault(
                r'Content-Type',
                r'application/x-www-form-urlencoded'
            )

        kwargs[r'data'] = data
        kwargs[r'params'] = params

        Utils.log.debug(
            r'{0} {1} => {2}'.format(
                method,
                url,
                str({key: val for key, val in kwargs.items() if isinstance(val, (str, list, dict))})
            )
        )

        kwargs.setdefault(r'ssl_context', self._ssl_context)

        for times in range(0, self._retry_count):

            if times > 0:
                Utils.log.debug(r'{0} {1} => retry:{2}'.format(method, url, times))

            try:

                async with aiohttp.ClientSession(**self._session_config) as _session:

                    async with _session.request(method, url, **kwargs) as _response:

                        headers = dict(_response.headers)

                        response = await self._handle_response(_response)

                Utils.log.info(
                    r'{0} {1} => status:{2}'.format(
                        method,
                        url,
                        _response.status
                    )
                )

            except aiohttp.ClientResponseError as err:

                Utils.log.error(err)

                if err.status < 500:
                    break
                else:
                    await self._sleep_for_retry(times)

            except aiohttp.ClientError as err:

                Utils.log.error(err)

                await self._sleep_for_retry(times)

            except Exception as err:

                Utils.log.error(err)

                break

            else:

                break

        return headers, response

    async def _handle_response(self, response):

        return await response.read()


class _HTTPTextMixin:

    async def _handle_response(self, response):

        return await response.text()


class _HTTPJsonMixin:

    async def _handle_response(self, response):

        return await response.json()


class _HTTPTouchMixin:

    async def _handle_response(self, response):

        return dict(response.headers)


class HTTPClient(_HTTPClient):

    async def get(self, url, params=None, *, cookies=None, headers=None):

        _, result = await self.request(aiohttp.hdrs.METH_GET, url, None, params, cookies=cookies, headers=headers)

        return result

    async def options(self, url, params=None, *, cookies=None, headers=None):

        _, result = await self.request(aiohttp.hdrs.METH_OPTIONS, url, None, params, cookies=cookies, headers=headers)

        return result

    async def head(self, url, params=None, *, cookies=None, headers=None):

        _, result = await self.request(aiohttp.hdrs.METH_HEAD, url, None, params, cookies=cookies, headers=headers)

        return result

    async def post(self, url, data=None, params=None, *, cookies=None, headers=None):

        _, result = await self.request(aiohttp.hdrs.METH_POST, url, data, params, cookies=cookies, headers=headers)

        return result

    async def put(self, url, data=None, params=None, *, cookies=None, headers=None):

        _, result = await self.request(aiohttp.hdrs.METH_PUT, url, data, params, cookies=cookies, headers=headers)

        return result

    async def patch(self, url, data=None, params=None, *, cookies=None, headers=None):

        _, result = await self.request(aiohttp.hdrs.METH_PATCH, url, data, params, cookies=cookies, headers=headers)

        return result

    async def delete(self, url, params=None, *, cookies=None, headers=None):

        _, result = await self.request(aiohttp.hdrs.METH_DELETE, url, None, params, cookies=cookies, headers=headers)

        return result


class HTTPTextClient(_HTTPTextMixin, HTTPClient):
    pass


class HTTPJsonClient(_HTTPJsonMixin, HTTPClient):
    pass


class HTTPTouchClient(_HTTPTouchMixin, HTTPClient):
    pass


class HTTPClientPool(HTTPClient):

    def __init__(self, retry_count=5, use_dns_cache=True, ttl_dns_cache=10, limit=100, limit_per_host=0, read_timeout=60, conn_timeout=10, **kwargs):

        super().__init__(retry_count, read_timeout, conn_timeout, **kwargs)

        self._tcp_connector = aiohttp.TCPConnector(
            use_dns_cache=use_dns_cache,
            ttl_dns_cache=ttl_dns_cache,
            ssl_context=self._ssl_context,
            limit=limit,
            limit_per_host=limit_per_host,
        )

        self._session_config[r'connector'] = self._tcp_connector


class HTTPTextClientPool(_HTTPTextMixin, HTTPClientPool):
    pass


class HTTPJsonClientPool(_HTTPJsonMixin, HTTPClientPool):
    pass


class HTTPTouchClientPool(_HTTPTouchMixin, HTTPClientPool):
    pass


class Downloader(_HTTPClient):

    def __init__(self, file, retry_count=5, read_timeout=65535, conn_timeout=60, **kwargs):

        super().__init__(retry_count, read_timeout, conn_timeout, **kwargs)

        self._file = file

        self._state = _STATE.PENDING

        self._response = None

    def _sleep_for_retry(self, times):

        return Utils.sleep(2 ** times)

    @property
    def finished(self):

        return self._state == _STATE.FINISHED

    @property
    def response(self):

        return self._response

    async def _handle_response(self, response):

        if self._state != _STATE.PENDING:
            return False

        result = False

        self._state = _STATE.FETCHING
        self._response = response

        with open(self._file, r'wb') as stream:

            while True:

                chunk = await response.content.read(65536)

                if chunk:
                    stream.write(chunk)
                else:
                    result = bool(response.status == 200)
                    break

        if not result and os.path.exists(self._file):
            os.remove(self._file)

        self._state = _STATE.FINISHED

        return result

    async def fetch(self, url, *, params=None, cookies=None, headers=None):

        _, result = await self.request(aiohttp.hdrs.METH_GET, url, None, params, cookies=cookies, headers=headers)

        return result


class DownloadBuffer(Downloader):

    def __init__(self, read_timeout=65535, conn_timeout=10, **kwargs):

        super().__init__(FileBuffer(), 1, read_timeout, conn_timeout, **kwargs)

    @property
    def buffer(self):

        return self._file

    async def _handle_response(self, response):

        if self._state != _STATE.PENDING:
            return False

        result = False

        self._state = _STATE.FETCHING
        self._response = response

        try:

            while True:

                chunk = await response.content.read(65536)

                if chunk:
                    self._file.write(chunk)
                else:
                    result = bool(response.status == 200)
                    break

        except Exception as err:

            Utils.log.error(err)

        self._state = _STATE.FINISHED

        return result

    async def fetch(self, url, *, params=None, cookies=None, headers=None):

        _, result = await self.request(aiohttp.hdrs.METH_GET, url, None, params, cookies=cookies, headers=headers)

        return result
