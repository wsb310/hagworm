# -*- coding: utf-8 -*-

from .base import Utils, FuncWrapper


class EventDispatcher:

    def __init__(self):

        self._observers = {}

    def _gen_observer(self):

        return FuncWrapper()

    def dispatch(self, _type, *args, **kwargs):

        Utils.log.debug(
            r'dispatch event {0} {1} {2}'.format(_type, args, kwargs))

        if _type in self._observers:
            self._observers[_type](*args, **kwargs)

    def add_listener(self, _type, _callable):

        Utils.log.debug(r'add event listener => type({0}) function({1})'.format(_type, id(_callable)))

        result = False

        if _type in self._observers:
            result = self._observers[_type].add(_callable)
        else:
            observer = self._observers[_type] = self._gen_observer()
            result = observer.add(_callable)

        return result

    def remove_listener(self, _type, _callable):

        Utils.log.debug(r'remove event listener => type({0}) function({1})'.format(_type, id(_callable)))

        result = False

        if _type in self._observers:

            observer = self._observers[_type]

            result = observer.remove(_callable)

            if not observer.is_valid:
                del self._observers[_type]

        return result
