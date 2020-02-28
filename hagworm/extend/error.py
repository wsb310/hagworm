# -*- coding: utf-8 -*-


# 基础异常
class BaseError(Exception):
    pass


# 数据库只读限制异常
class MySQLReadOnlyError(BaseError):
    pass


# 流量限制异常
class RateLimitError(BaseError):
    pass


# 常量设置异常
class ConstError(BaseError):
    pass
