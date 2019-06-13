# -*- coding: utf-8 -*-

from .base import Utils


class EventObserver:

    def __init__(self, *args):

        self._callbacks = set(args)

    def __call__(self, *args, **kwargs):

        for func in self._callbacks:
            func(*args, **kwargs)

    @property
    def is_valid(self):

        return len(self._callbacks) > 0

    def add(self, func):

        if func in self._callbacks:
            return False
        else:
            self._callbacks.add(func)
            return True

    def remove(self, func):

        if func in self._callbacks:
            self._callbacks.remove(func)
            return True
        else:
            return False


class EventDispatcher:

    def __init__(self):

        self._observers = {}

    def dispatch(self, _type, *args, **kwargs):

        Utils.log.debug(
            r'dispatch event {0} {1} {2}'.format(_type, args, kwargs))

        if _type in self._observers:
            self._observers[_type](*args, **kwargs)

    def add_listener(self, _type, _callable):

        Utils.log.debug(r'add event listener => type({0}) function({1})'.format(
            _type, id(_callable)))

        result = False

        if _type in self._observers:
            result = self._observers[_type].add(_callable)
        else:
            self._observers[_type] = EventObserver(_callable)
            result = True

        return result

    def remove_listener(self, _type, _callable):

        Utils.log.debug(r'remove event listener => type({0}) function({1})'.format(
            _type, id(_callable)))

        result = False

        if _type in self._observers:

            observer = self._observers[_type]

            result = observer.remove(_callable)

            if not observer.is_valid:
                del self._observers[_type]

        return result
