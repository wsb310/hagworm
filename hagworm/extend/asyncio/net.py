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


_DEFAULT_TIMEOUT = aiohttp.client.ClientTimeout(total=60, connect=5, sock_read=60, sock_connect=5)
_DOWNLOAD_TIMEOUT = aiohttp.client.ClientTimeout(total=600, connect=5, sock_read=600, sock_connect=5)

_CA_FILE = os.path.join(
    os.path.split(os.path.abspath(__file__))[0],
    r'../../static/cacert.pem'
)


class _HTTPClient:
    """HTTP客户端基类
    """

    def __init__(self, retry_count=5, timeout=_DEFAULT_TIMEOUT, **kwargs):

        self._ssl_context = ssl.create_default_context(cafile=_CA_FILE)

        self._retry_count = retry_count

        self._session_config = kwargs
        self._session_config[r'timeout'] = timeout

    async def _sleep_for_retry(self, times):

        return await Utils.sleep(times)

    async def _handle_response(self, response):

        return await response.read()

    def timeout(self, *, total=None, connect=None, sock_read=None, sock_connect=None):
        """生成超时配置对象

        Args:
            total: 总超时时间
            connect: 从连接池中等待获取连接的超时时间
            sock_read: Socket数据接收的超时时间
            sock_connect: Socket连接的超时时间

        """

        return aiohttp.client.ClientTimeout(
            total=total, connect=connect,
            sock_read=sock_read, sock_connect=sock_connect
        )

    async def send_request(self, method, url, data=None, params=None, **kwargs):

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

        kwargs.setdefault(r'ssl', self._ssl_context)

        for times in range(0, self._retry_count):

            if times > 0:
                Utils.log.debug(f'{method} {url} => retry:{times}')

            try:

                async with aiohttp.ClientSession(**self._session_config) as _session:

                    async with _session.request(method, url, **kwargs) as _response:

                        headers = dict(_response.headers)

                        response = await self._handle_response(_response)

                Utils.log.info(f'{method} {url} => status:{_response.status}')

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


class _HTTPTextMixin:
    """Text模式混入类
    """

    async def _handle_response(self, response):

        return await response.text()


class _HTTPJsonMixin:
    """Json模式混入类
    """

    async def _handle_response(self, response):

        return await response.json(encoding=r'utf-8', content_type=None)


class _HTTPTouchMixin:
    """Touch模式混入类，不接收body数据
    """

    async def _handle_response(self, response):

        return dict(response.headers)


class HTTPClient(_HTTPClient):
    """HTTP客户端，普通模式
    """

    async def get(self, url, params=None, *, cookies=None, headers=None):

        _, result = await self.send_request(aiohttp.hdrs.METH_GET, url, None, params, cookies=cookies, headers=headers)

        return result

    async def options(self, url, params=None, *, cookies=None, headers=None):

        _, result = await self.send_request(aiohttp.hdrs.METH_OPTIONS, url, None, params, cookies=cookies, headers=headers)

        return result

    async def head(self, url, params=None, *, cookies=None, headers=None):

        _, result = await self.send_request(aiohttp.hdrs.METH_HEAD, url, None, params, cookies=cookies, headers=headers)

        return result

    async def post(self, url, data=None, params=None, *, cookies=None, headers=None):

        _, result = await self.send_request(aiohttp.hdrs.METH_POST, url, data, params, cookies=cookies, headers=headers)

        return result

    async def put(self, url, data=None, params=None, *, cookies=None, headers=None):

        _, result = await self.send_request(aiohttp.hdrs.METH_PUT, url, data, params, cookies=cookies, headers=headers)

        return result

    async def patch(self, url, data=None, params=None, *, cookies=None, headers=None):

        _, result = await self.send_request(aiohttp.hdrs.METH_PATCH, url, data, params, cookies=cookies, headers=headers)

        return result

    async def delete(self, url, params=None, *, cookies=None, headers=None):

        _, result = await self.send_request(aiohttp.hdrs.METH_DELETE, url, None, params, cookies=cookies, headers=headers)

        return result


class HTTPTextClient(_HTTPTextMixin, HTTPClient):
    """HTTP客户端，Text模式
    """
    pass


class HTTPJsonClient(_HTTPJsonMixin, HTTPClient):
    """HTTP客户端，Json模式
    """
    pass


class HTTPTouchClient(_HTTPTouchMixin, HTTPClient):
    """HTTP客户端，Touch模式
    """
    pass


class HTTPClientPool(HTTPClient):
    """HTTP带连接池客户端，普通模式
    """

    def __init__(self,
                 retry_count=5, use_dns_cache=True, ttl_dns_cache=10,
                 limit=100, limit_per_host=0, timeout=_DEFAULT_TIMEOUT,
                 **kwargs
                 ):

        super().__init__(retry_count, timeout, **kwargs)

        self._tcp_connector = aiohttp.TCPConnector(
            use_dns_cache=use_dns_cache,
            ttl_dns_cache=ttl_dns_cache,
            ssl=self._ssl_context,
            limit=limit,
            limit_per_host=limit_per_host,
        )

        self._session_config[r'connector'] = self._tcp_connector
        self._session_config[r'connector_owner'] = False

    async def close(self):

        if not self._tcp_connector.closed:
            await self._tcp_connector.close()


class HTTPTextClientPool(_HTTPTextMixin, HTTPClientPool):
    """HTTP带连接池客户端，Text模式
    """
    pass


class HTTPJsonClientPool(_HTTPJsonMixin, HTTPClientPool):
    """HTTP带连接池客户端，Json模式
    """
    pass


class HTTPTouchClientPool(_HTTPTouchMixin, HTTPClientPool):
    """HTTP带连接池客户端，Touch模式
    """
    pass


class Downloader(_HTTPClient):
    """HTTP文件下载器
    """

    def __init__(self, file, retry_count=5, timeout=_DOWNLOAD_TIMEOUT, **kwargs):

        super().__init__(retry_count, timeout, **kwargs)

        self._file = file

        self._state = _STATE.PENDING

        self._response = None

    async def _sleep_for_retry(self, times):

        return await Utils.sleep(2 ** times)

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

        _, result = await self.send_request(aiohttp.hdrs.METH_GET, url, None, params, cookies=cookies, headers=headers)

        return result


class DownloadBuffer(Downloader):
    """HTTP文件下载器(临时文件版)
    """

    def __init__(self, timeout=_DOWNLOAD_TIMEOUT, **kwargs):

        super().__init__(FileBuffer(), 1, timeout, **kwargs)

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

        _, result = await self.send_request(aiohttp.hdrs.METH_GET, url, None, params, cookies=cookies, headers=headers)

        return result
