# -*- coding: utf-8 -*-


# 基础异常
class BaseError(Exception):

    def __init__(self, data=None):

        super().__init__()

        self._data = data

    @property
    def data(self):

        return self._data

    def __repr__(self):

        return repr(self._data)


# 数据库只读限制异常
class MySQLReadOnlyError(BaseError):
    pass


# 常量设置异常
class ConstError(BaseError):
    pass


# NTP校准异常
class NTPCalibrateError(BaseError):
    pass
