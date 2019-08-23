# -*- coding: utf-8 -*-

import threading
import traceback


class SingletonMetaclass(type):

    def __init__(cls, name, bases, attrs):

        cls._instance = None

        cls._lock = threading.Lock()

    def __call__(cls, *args, **kwargs):

        result = None

        cls._lock.acquire()

        try:

            if cls._instance is not None:
                result = cls._instance
            else:
                result = cls._instance = super().__call__(*args, **kwargs)

        except BaseException:

            traceback.print_exc()

        cls._lock.release()

        return result


class Singleton(metaclass=SingletonMetaclass):
    pass


class SubclassMetaclass(type):

    _baseclasses = []

    def __new__(mcs, name, bases, attrs):

        subclass = r'.'.join([attrs[r'__module__'], attrs[r'__qualname__']])

        for base in bases:

            if base in mcs._baseclasses:
                getattr(base, r'_subclasses').append(subclass)
            else:
                mcs._baseclasses.append(base)
                setattr(base, r'_subclasses', [subclass])

        return type.__new__(mcs, name, bases, attrs)
