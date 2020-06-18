# -*- coding: utf-8 -*-

from .base import Utils, ContextManager


class TransactionInterface:
    """事务接口
    """

    def __init__(self, *, commit_callback=None, rollback_callback=None):

        self._commit_callbacks = []
        self._rollback_callbacks = []

        if commit_callback is not None:
            self._commit_callbacks.append(commit_callback)

        if rollback_callback is not None:
            self._rollback_callbacks.append(rollback_callback)

    def _clear_callbacks(self):

        self._commit_callbacks.clear()
        self._rollback_callbacks.clear()

        self._commit_callbacks = self._rollback_callbacks = None

    def add_commit_callback(self, _callable, *args, **kwargs):

        if self._commit_callbacks is not None:
            if args or kwargs:
                self._commit_callbacks.append(
                    Utils.func_partial(_callable, *args, **kwargs)
                )
            else:
                self._commit_callbacks.append(_callable)

    def add_rollback_callback(self, _callable, *args, **kwargs):

        if self._rollback_callbacks is not None:
            if args or kwargs:
                self._rollback_callbacks.append(
                    Utils.func_partial(_callable, *args, **kwargs)
                )
            else:
                self._rollback_callbacks.append(_callable)

    def commit(self):

        raise NotImplementedError()

    def rollback(self):

        raise NotImplementedError()

    def bind(self, trx):

        self.add_commit_callback(trx.commit)
        self.add_rollback_callback(trx.rollback)


class Transaction(TransactionInterface, ContextManager):
    """事务对象

    使用上下文实现的一个事务对象，可以设置commit和rollback回调
    未显示commit的情况下，会自动rollback

    """

    def _context_release(self):

        self.rollback()

    def commit(self):

        if self._commit_callbacks is None:
            return

        callbacks = self._commit_callbacks.copy()

        self._clear_callbacks()

        for _callable in callbacks:
            try:
                _callable()
            except Exception as err:
                Utils.log.critical(f'transaction commit error:\n{err}')

    def rollback(self):

        if self._rollback_callbacks is None:
            return

        callbacks = self._rollback_callbacks.copy()

        self._clear_callbacks()

        for _callable in callbacks:
            try:
                _callable()
            except Exception as err:
                Utils.log.critical(f'transaction rollback error:\n{err}')
