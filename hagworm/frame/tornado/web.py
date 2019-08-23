# -*- coding: utf-8 -*-

import os
import functools

from tornado.web import RequestHandler
from tornado.websocket import WebSocketHandler

from hagworm.extend.struct import ErrorData
from hagworm.extend.asyncio.base import Utils
from hagworm.extend.asyncio.net import DownloadBuffer


class HttpBasicAuth:

    def __init__(self, realm, username, password):

        self._realm = realm
        self._username = username
        self._password = password

    def __call__(self, func):

        @functools.wraps(func)
        def _wrapper(request, *args, **kwargs):

            auth_header = request.get_header(r'Authorization')

            try:

                if auth_header:

                    auth_info = Utils.b64_decode(auth_header.split(r' ', 2)[1])

                    if auth_info == r'{0}:{1}'.format(self._username, self._password):
                        return func(request, *args, **kwargs)

            except Exception as err:

                Utils.log.error(err)

            request.set_header(
                r'WWW-Authenticate',
                r'Basic realm="{0}"'.format(self._realm)
            )
            request.set_status(401)

            return request.finish()

        return _wrapper


class _BaseHandlerMixin(Utils):

    @property
    def request_module(self):

        return r'{0}.{1}'.format(self.module, self.method)

    @property
    def module(self):

        return r'{0}.{1}'.format(self.__class__.__module__, self.__class__.__name__)

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

    def get_header(self, name, default=None):
        """
        获取header数据
        """
        return self.request.headers.get(name, default)


class SocketBaseHandler(WebSocketHandler, _BaseHandlerMixin):

    def __init__(self, application, request, **kwargs):

        WebSocketHandler.__init__(self, application, request)

        self._payload = kwargs

    def check_origin(self, origin):

        return True


class RequestBaseHandler(RequestHandler, _BaseHandlerMixin):

    def __init__(self, application, request, **kwargs):

        RequestHandler.__init__(self, application, request)

        self._payload = kwargs

    def head(self, *_, **__):

        self.finish()

    def options(self, *_, **__):

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

            except BaseException:

                self.log.debug(
                    r'Invalid application/json body: {0}'.format(self.body))

    def get_files(self, name):
        """
        获取files数据
        """
        result = []

        file_data = self.files.get(name, None)

        if file_data is not None:
            self.list_extend(result, file_data)

        for index in range(len(self.files)):

            file_data = self.files.get(
                r'{0:s}[{1:d}]'.format(name, index), None)

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

    def get_arg_json(self, name, default=None, throw_error=False):
        """
        获取json型输入
        """
        result = self.get_argument(name, None, True)

        if result is None:
            result = default
        else:
            try:
                result = self.json_decode(result)
            except BaseException:
                result = ErrorData(result) if throw_error else default

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

        try:
            result = self.json_encode(chunk)
        except BaseException:
            result = None

        return self.finish(result)

    def write_png(self, chunk):
        """
        输出PNG类型
        """
        self.set_header(r'Content-Type', r'image/png')

        return self.finish(chunk)


class DownloadAgent(RequestBaseHandler, DownloadBuffer):

    def __init__(self, *args, **kwargs):

        RequestBaseHandler.__init__(self, *args, **kwargs)
        DownloadBuffer.__init__(self)

    async def _handle_response(self, response):

        for key, val in response.headers.items():
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
        except BaseException:
            pass

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
                    r'attachment;filename={0:s}'.format(file_name)
                )

            Utils.ensure_future(self.fetch(url, params=params, cookies=cookies, headers=headers))

            await self._flush_data()

        except Exception as err:

            self.log.exception(err)

        finally:

            self.finish()
