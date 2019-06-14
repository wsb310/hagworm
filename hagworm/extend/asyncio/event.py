# -*- coding: utf-8 -*-

from aioredis.pubsub import Receiver

from hagworm.extend.event import EventDispatcher
from hagworm.extend.asyncio.base import Utils, Transaction, FutureWithTimeout


class DistributedEvent(EventDispatcher):

    def __init__(self, redis_pool, channel_name, channel_count):

        super().__init__()

        self._redis_pool = redis_pool

        self._channels = [
            r'event_bus_{0}_{1}'.format(
                Utils.md5_u32(channel_name),
                channel
            )
            for channel in range(channel_count)
        ]

        for channel in self._channels:
            Utils.ensure_future(self._event_listener(channel))

    async def _event_listener(self, channel):

        while True:

            try:

                cache = await self._redis_pool.get_client()

                receiver = Receiver()

                await cache.subscribe(
                    receiver.channel(channel)
                )

                async for channel, message in receiver.iter():
                    self._event_assigner(channel, message)

            except Exception as err:

                Utils.log.error(err)

                await Utils.wait_frame()

    def _event_assigner(self, channel, message):

        message = Utils.pickle_loads(message)

        Utils.log.debug(r'event handling => channel({0}) message({1})'.format(channel.name.decode(), message))

        _type = message.get(r'type', r'')
        args = message.get(r'args', [])
        kwargs = message.get(r'kwargs', {})

        if _type in self._observers:
            self._observers[_type](*args, **kwargs)

    async def dispatch(self, _type, *args, **kwargs):

        channel = self._channels[Utils.md5_u32(_type) % len(self._channels)]

        message = {
            r'type': _type,
            r'args': args,
            r'kwargs': kwargs,
        }

        Utils.log.debug(r'event dispatch => channel({0}) message({1})'.format(channel, message))

        cache = await self._redis_pool.get_client()

        result = await cache.publish(channel, Utils.pickle_dumps(message))

        return result

    def gen_event_waiter(self, delay, buffer_time=0):

        return EventWaiter(self, delay, buffer_time)


class EventWaiter(FutureWithTimeout):

    def __init__(self, dispatcher, delay, buffer_time):

        super().__init__(delay)

        self._dispatcher = dispatcher
        self._transaction = Transaction()

        self._buffer_time = buffer_time
        self._buffer_data = []

    def listen(self, *args):

        for _type in args:

            handler = Utils.func_partial(self._event_handler, _type)

            if self._dispatcher.add_listener(_type, handler):
                self._transaction.add(
                    self._dispatcher.remove_listener,
                    _type,
                    handler
                )

        return self

    def _set_done(self):

        if self.done():
            return

        if self._transaction is not None:
            self._transaction.rollback()

        self._clear_timeout()

        self.set_result(self._buffer_data)

    def _event_handler(self, _type, *args, **kwargs):

        if self.done():
            return

        self._buffer_data.append(
            {
                r'type': _type,
                r'args': args,
                r'kwargs': kwargs,
            }
        )

        if self._buffer_time > 0:

            if len(self._buffer_data) == 1:
                self._clear_timeout()
                Utils.call_later(self._buffer_time, self._set_done)

        else:

            self._set_done()
