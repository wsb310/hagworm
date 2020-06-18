# -*- coding: utf-8 -*-

import os
import json
import functools
import aiohttp

from aiohttp.web_exceptions import HTTPBadGateway

from tornado.web import RequestHandler
from tornado.websocket import WebSocketHandler

from hagworm.extend.struct import Result
from hagworm.extend.asyncio.base import Utils
from hagworm.extend.asyncio.net import DownloadBuffer, HTTPClientPool

from wtforms_tornado import Form


PROXY_IGNORE_HEADERS = (r'CONTENT-ENCODING', r'TRANSFER-ENCODING',)


def json_wraps(func):
    """json装饰器
    """

    @functools.wraps(func)
    async def _wrapper(handler, *args, **kwargs):
        resp = await Utils.awaitable_wrapper(
            func(handler, *args, **kwargs)
        )

        if isinstance(resp, Result):
            return handler.write_json(resp)

    return _wrapper


class HttpBasicAuth:
    """Http基础认证装饰器
    """

    def __init__(self, realm, username, password):

        self._realm = realm
        self._username = username
        self._password = password

    def __call__(self, func):

        @functools.wraps(func)
        def _wrapper(handler, *args, **kwargs):

            auth_header = handler.get_header(r'Authorization')

            try:

                if auth_header:

                    auth_info = Utils.b64_decode(auth_header.split(r' ', 2)[1])

                    if auth_info == f'{self._username}:{self._password}':
                        return func(handler, *args, **kwargs)

            except Exception as err:

                Utils.log.error(err)

            handler.set_header(
                r'WWW-Authenticate',
                f'Basic realm="{self._realm}"'
            )
            handler.set_status(401)

            return handler.finish()

        return _wrapper


class FormInjection:
    """表单注入器
    """

    def __init__(self, form_cls=None, err_code: int = -1):

        if not issubclass(form_cls, Form):
            raise TypeError(r'Dot Implemented Form Interface')

        self._form_cls = form_cls
        self._err_code = err_code

    def __call__(self, func):

        @functools.wraps(func)
        async def _wrapper(handler: RequestBaseHandler, *args, **kwargs):

            form = self._form_cls(handler.request.arguments)

            setattr(handler, r'form', form)
            setattr(handler, r'data', form.data)

            if form.validate():

                await json_wraps(func)(handler, *args, **kwargs)

            else:

                return handler.write_json(
                    Result(self._err_code, extra=form.errors),
                    400
                )

        return _wrapper


class DebugHeader:
    """调试信息注入器
    """

    def __call__(self, func):

        @functools.wraps(func)
        async def _wrapper(handler: RequestBaseHandler, *args, **kwargs):

            if self._is_debug() is True:
                self._receive_header(
                    handler.get_header(r'Debug-Data')
                )

            await func(handler, *args, **kwargs)

            if self._is_debug() is True:
                handler.set_header(
                    r'Debug-Data',
                    self._get_debug_header()
                )

        return _wrapper

    def _is_debug(self) -> bool:

        raise NotImplementedError()

    def _receive_header(self, data: str):

        raise NotImplementedError()

    def _get_debug_header(self) -> str:

        raise NotImplementedError()


class LogRequestMixin:

    def log_request(self):
        Utils.log.info(
            '\n---------- request arguments ----------\n' +
            json.dumps(
                {
                    key: [val.decode(r'utf-8') for val in items]
                    for key, items in self.request.arguments.items()
                },
                ensure_ascii=False, indent=4
            )
        )


class _BaseHandlerMixin(Utils):
    """Handler基础工具混入类
    """

    @property
    def request_module(self):
        return f'{self.module}.{self.method}'

    @property
    def module(self):
        _class = self.__class__

        return f'{_class.__module__}.{_class.__name__}'

    @property
    def method(self):
        return self.request.method.lower()

    @property
    def version(self):
        return self.request.version.lower()

    @property
    def protocol(self):
        return self.request.protocol

    @property
    def host(self):
        return self.request.host

    @property
    def path(self):
        return self.request.path

    @property
    def query(self):
        return self.request.query

    @property
    def body(self):
        return self.request.body

    @property
    def files(self):
        return self.request.files

    @property
    def closed(self):
        return self.request.connection.stream.closed()

    @property
    def referer(self):
        return self.get_header(r'Referer', r'')

    @property
    def client_ip(self):
        return self.get_header(r'X-Real-IP', self.request.remote_ip)

    @property
    def content_type(self):
        return self.get_header(r'Content-Type', r'')

    @property
    def content_length(self):
        result = self.get_header(r'Content-Length', r'')

        return int(result) if result.isdigit() else 0

    @property
    def headers(self):
        return self.request.headers

    def get_header(self, name, default=None):
        """
        获取header数据
        """
        return self.request.headers.get(name, default)


class SocketBaseHandler(WebSocketHandler, _BaseHandlerMixin):
    """WebSocket请求处理类
    """

    def initialize(self, **kwargs):
        setattr(self, r'_payload', kwargs)

    @property
    def payload(self):
        return getattr(self, r'_payload', None)

    def check_origin(self, origin):
        return True


class RequestBaseHandler(RequestHandler, _BaseHandlerMixin):
    """Http请求处理类
    """

    def initialize(self, **kwargs):

        setattr(self, r'_payload', kwargs)

    @property
    def payload(self):

        return getattr(self, r'_payload', None)

    def head(self, *_1, **_2):

        self.finish()

    def options(self, *_1, **_2):

        self.finish()

    async def prepare(self):

        self._parse_json_arguments()

    def set_default_headers(self):

        self.set_header(r'Cache-Control', r'no-cache')

        self.set_header(r'X-Timestamp', self.timestamp())

        payload = self.get_header(r'X-Payload')

        if payload:
            self.set_header(r'X-Payload', payload)

        origin = self.get_header(r'Origin')

        if origin:

            self.set_header(r'Access-Control-Allow-Origin', r'*')

            method = self.get_header(r'Access-Control-Request-Method')
            if method:
                self.set_header(r'Access-Control-Allow-Methods', method)

            headers = self.get_header(r'Access-Control-Request-Headers')
            if headers:
                self.set_header(r'Access-Control-Allow-Headers', headers)

            self.set_header(r'Access-Control-Max-Age', r'86400')
            self.set_header(r'Access-Control-Allow-Credentials', r'true')

    def set_cookie(self, name, value, domain=None, expires=None, path="/", expires_days=None, **kwargs):

        if type(value) not in (str, bytes):
            value = str(value)

        return super().set_cookie(name, value, domain, expires, path, expires_days, **kwargs)

    def get_secure_cookie(self, name, value=None, max_age_days=31, min_version=None):

        result = super().get_secure_cookie(name, value, max_age_days, min_version)

        return self.basestring(result)

    def set_secure_cookie(self, name, value, expires_days=30, version=None, **kwargs):

        if type(value) not in (str, bytes):
            value = str(value)

        return super().set_secure_cookie(name, value, expires_days, version, **kwargs)

    def get_current_user(self):

        session = self.get_cookie(r'session')

        if not session:
            session = self.uuid1()
            self.set_cookie(r'session', session)

        self.current_user = session

        return session

    def compute_etag(self):

        return None

    def _parse_json_arguments(self):

        self.request.json_arguments = {}

        content_type = self.content_type

        if content_type and content_type.find(r'application/json') >= 0 and self.body:

            try:

                json_args = self.json_decode(self.body)

                if isinstance(json_args, dict):

                    self.request.json_arguments.update(json_args)

                    for key, val in self.request.json_arguments.items():

                        if not isinstance(val, str):
                            val = str(val)

                        self.request.arguments.setdefault(key, []).append(val)

            except Exception as _:

                self.log.debug(f'Invalid application/json body: {self.body}')

    def get_files(self, name):
        """
        获取files数据
        """
        result = []

        file_data = self.files.get(name, None)

        if file_data is not None:
            self.list_extend(result, file_data)

        for index in range(len(self.files)):

            file_data = self.files.get(f'{name}[{index}]', None)

            if file_data is not None:
                self.list_extend(result, file_data)

        return result

    def get_arg_str(self, name, default=r'', length=0):
        """
        获取str型输入
        """
        result = self.get_argument(name, None, True)

        if result is None:
            return default

        if not isinstance(result, str):
            result = str(result)

        if (length > 0) and (len(result) > length):
            result = result[0:length]

        return result

    def get_arg_bool(self, name, default=False):
        """
        获取bool型输入
        """
        result = self.get_argument(name, None, True)

        if result is None:
            return default

        result = self.convert_bool(result)

        return result

    def get_arg_int(self, name, default=0, min_val=None, max_val=None):
        """
        获取int型输入
        """
        result = self.get_argument(name, None, True)

        if result is None:
            return default

        result = self.convert_int(result, default)
        result = self.interval_limit(result, min_val, max_val)

        return result

    def get_arg_float(self, name, default=0.0, min_val=None, max_val=None):
        """
        获取float型输入
        """
        result = self.get_argument(name, None, True)

        if result is None:
            return default

        result = self.convert_float(result, default)
        result = self.interval_limit(result, min_val, max_val)

        return result

    def get_arg_json(self, name, default=None):
        """
        获取json型输入
        """

        result = default

        value = self.get_argument(name, None, True)

        if value is not None:
            try:
                result = self.json_decode(value)
            except Exception as _:
                self.log.debug(f'Invalid application/json argument({name}): {value}')

        return result

    def get_json_argument(self, name, default=None):

        return self.request.json_arguments.get(name, default)

    def get_json_arguments(self):

        return self.deepcopy(self.request.json_arguments)

    def get_all_arguments(self):

        result = {}

        for key in self.request.arguments.keys():
            result[key] = self.get_argument(key)

        return result

    def write_json(self, chunk, status_code=200):
        """
        输出JSON类型
        """
        self.set_header(r'Content-Type', r'application/json')

        if status_code != 200:
            self.set_status(status_code)

        result = None

        try:
            result = self.json_encode(chunk)
        except Exception as _:
            self.log.error(f'json encode error: {chunk}')

        return self.finish(result)

    def write_png(self, chunk):
        """
        输出PNG类型
        """
        self.set_header(r'Content-Type', r'image/png')

        return self.finish(chunk)


class DownloadAgent(RequestBaseHandler, DownloadBuffer):
    """文件下载代理类
    """

    def __init__(self, *args, **kwargs):

        RequestBaseHandler.__init__(self, *args, **kwargs)
        DownloadBuffer.__init__(self)

    def on_finish(self):

        self.close()

    async def _handle_response(self, response):

        for key, val in response.headers.items():
            if key.upper() not in PROXY_IGNORE_HEADERS:
                self.set_header(key, val)

        return await DownloadBuffer._handle_response(self, response)

    async def _flush_data(self):

        while True:

            if self.closed and self.response:
                self.response.close()
                break

            chunk = self._file.read(65536)

            if chunk:
                self.write(chunk)
                await self.flush()
            elif self.finished:
                break
            else:
                await Utils.wait_frame()

    def _get_file_name(self, url):

        result = None

        try:
            result = os.path.split(self.urlparse.urlparse(url)[2])[1]
        except Exception as _:
            self.log.error(f'urlparse error: {url}')

        return result

    async def transmit(self, url, file_name=None, *, params=None, cookies=None, headers=None):

        try:

            _range = self.request.headers.get(r'Range', None)

            if _range is not None:

                if headers is None:
                    headers = {r'Range': _range}
                else:
                    headers[r'Range'] = _range

            if not file_name:
                file_name = self._get_file_name(url)

            if file_name:
                self.set_header(
                    r'Content-Disposition',
                    f'attachment;filename={file_name}'
                )

            Utils.create_task(self.fetch(url, params=params, cookies=cookies, headers=headers))

            await self._flush_data()

        except Exception as err:

            self.log.exception(err)

        finally:

            self.finish()


class HTTPProxy(HTTPClientPool):
    """简易的HTTP代理类，带连接池功能
    """

    def __init__(self,
                 use_dns_cache=True, ttl_dns_cache=10,
                 limit=100, limit_per_host=0, timeout=None,
                 **kwargs
                 ):

        super().__init__(0, use_dns_cache, ttl_dns_cache, limit, limit_per_host, timeout, **kwargs)

    async def send_request(self, method: str, url: str, handler: RequestBaseHandler, **settings) -> int:

        global PROXY_IGNORE_HEADERS

        result = 0

        settings[r'data'] = handler.body
        settings[r'params'] = handler.query

        headers = dict(handler.headers)
        headers[r'Host'] = Utils.urlparse.urlparse(url).netloc
        settings[r'headers'] = headers

        settings.setdefault(r'ssl', self._ssl_context)

        try:

            async with aiohttp.ClientSession(**self._session_config) as _session:

                async with _session.request(method, url, **settings) as _response:

                    for key, val in _response.headers.items():
                        if key.upper() not in PROXY_IGNORE_HEADERS:
                            handler.set_header(key, val)

                    while True:

                        chunk = await _response.content.read(65536)

                        if chunk:
                            handler.write(chunk)
                            await handler.flush()
                        else:
                            break

                    handler.finish()

                    result = _response.status

            Utils.log.info(f'{method} {url} => status:{_response.status}')

        except aiohttp.ClientResponseError as err:

            Utils.log.error(err)

            handler.send_error(err.status, reason=err.message)

            result = err.status

        except Exception as err:

            Utils.log.error(err)

            handler.send_error(HTTPBadGateway.status_code, reason=r'Proxy Internal Error')

            result = HTTPBadGateway.status_code

        return result

    async def get(self, url: str, handler: RequestBaseHandler) -> int:

        return await self.send_request(aiohttp.hdrs.METH_GET, url, handler)

    async def options(self, url: str, handler: RequestBaseHandler) -> int:

        return await self.send_request(aiohttp.hdrs.METH_OPTIONS, url, handler)

    async def head(self, url: str, handler: RequestBaseHandler) -> int:

        return await self.send_request(aiohttp.hdrs.METH_HEAD, url, handler)

    async def post(self, url: str, handler: RequestBaseHandler) -> int:

        return await self.send_request(aiohttp.hdrs.METH_POST, url, handler)

    async def put(self, url: str, handler: RequestBaseHandler) -> int:

        return await self.send_request(aiohttp.hdrs.METH_PUT, url, handler)

    async def patch(self, url: str, handler: RequestBaseHandler) -> int:

        return await self.send_request(aiohttp.hdrs.METH_PATCH, url, handler)

    async def delete(self, url: str, handler: RequestBaseHandler) -> int:

        return await self.send_request(aiohttp.hdrs.METH_DELETE, url, handler)
