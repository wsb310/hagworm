# -*- coding: utf-8 -*-

from .base import Utils, AsyncContextManager

from hagworm.extend.transaction import TransactionInterface


class Transaction(TransactionInterface, AsyncContextManager):
    """事务对象

    使用异步上下文实现的一个事务对象，可以设置commit和rollback回调
    未显示commit的情况下，会自动rollback

    """

    async def _context_release(self):

        await self.rollback()

    async def commit(self):

        if self._commit_callbacks is None:
            return

        callbacks = self._commit_callbacks.copy()

        self._clear_callbacks()

        for _callable in callbacks:
            try:
                await Utils.awaitable_wrapper(
                    _callable()
                )
            except Exception as err:
                Utils.log.critical(f'transaction commit error:\n{err}')

    async def rollback(self):

        if self._rollback_callbacks is None:
            return

        callbacks = self._rollback_callbacks.copy()

        self._clear_callbacks()

        for _callable in callbacks:
            try:
                await Utils.awaitable_wrapper(
                    _callable()
                )
            except Exception as err:
                Utils.log.critical(f'transaction rollback error:\n{err}')
