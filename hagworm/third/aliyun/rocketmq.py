# -*- coding: utf-8 -*-

from mq_http_sdk.mq_client import MQClient as _MQClient
from mq_http_sdk.mq_consumer import MQConsumer as _MQConsumer
from mq_http_sdk.mq_producer import MQProducer as _MQProducer, MQTransProducer as _MQTransProducer
from mq_http_sdk.mq_exception import MQServerException

from hagworm.extend.interface import TaskInterface
from hagworm.extend.asyncio.base import Utils
from hagworm.extend.asyncio.task import LoopTask
from hagworm.extend.asyncio.future import ThreadPool


class MQConsumer(_MQConsumer):

    async def consume_message(self, batch_size=1, wait_seconds=-1):

        return await self.mq_client.thread_pool.run(super().consume_message, batch_size, wait_seconds)

    async def ack_message(self, receipt_handle_list):

        return await self.mq_client.thread_pool.run(super().ack_message, receipt_handle_list)


class MQProducer(_MQProducer):

    async def publish_message(self, message):

        return await self.mq_client.thread_pool.run(super().publish_message, message)


class MQTransProducer(_MQTransProducer):

    async def publish_message(self, message):

        return await self.mq_client.thread_pool.run(super().publish_message, message)

    async def consume_half_message(self, batch_size=1, wait_seconds=-1):

        return await self.mq_client.thread_pool.run(super().consume_half_message, batch_size, wait_seconds)

    async def commit(self, receipt_handle):

        return await self.mq_client.thread_pool.run(super().commit, receipt_handle)

    async def rollback(self, receipt_handle):

        return await self.mq_client.thread_pool.run(super().rollback, receipt_handle)


class MQClient(_MQClient):

    def __init__(self, host, access_id, access_key, security_token=r'', debug=False, logger=None):

        super().__init__(host, access_id, access_key, security_token, debug, logger)

        # 原版SDK中的MQClient线程不安全，利用线程池仅有一个线程这种特例，保证线程安全
        self._thread_pool = ThreadPool(1)

    @property
    def thread_pool(self):

        return self._thread_pool

    def get_consumer(self, instance_id, topic_name, consumer, message_tag=r''):

        return MQConsumer(instance_id, topic_name, consumer, message_tag, self, self.debug)

    def get_producer(self, instance_id, topic_name):

        return MQProducer(instance_id, topic_name, self, self.debug)

    def get_trans_producer(self, instance_id, topic_name, group_id):

        return MQTransProducer(instance_id, topic_name, group_id, self, self.debug)


class MQCycleConsumer(MQConsumer, LoopTask):

    def __init__(self, message_handler, instance_id, topic_name, consumer, message_tag, mq_client, debug=False):

        MQConsumer.__init__(self, instance_id, topic_name, consumer, message_tag, mq_client, debug)
        LoopTask.__init__(self, self._do_task, 1)

        self._message_handler = message_handler

    async def _do_task(self):

        msg_list = None

        try:
            msg_list = await self.consume_message(16, 30)
        except MQServerException as err:
            if err.type == r'MessageNotExist':
                Utils.log.debug(err)
            else:
                Utils.log.error(err)

        if msg_list:
            await Utils.awaitable_wrapper(
                self._message_handler(self, msg_list)
            )


class MQMultiCycleConsumers(TaskInterface):

    def __init__(self, client_num, host, access_id, access_key, security_token=r'', debug=False, logger=None):

        self._clients = [
            MQClient(host, access_id, access_key, security_token, debug, logger)
            for _ in range(client_num)
        ]

        self._consumers = None

        self._running = False

    def init_consumers(self,
                       message_handler, instance_id, topic_name, consumer, message_tag, debug=False,
                       *, consumer_cls=MQCycleConsumer
                       ):

        if self._consumers:
            self.stop()

        self._consumers = [
            consumer_cls(message_handler, instance_id, topic_name, consumer, message_tag, client, debug)
            for client in self._clients
        ]

    def start(self):

        if self._running or self._consumers is None:
            return False

        self._running = True

        for consumer in self._consumers:
            consumer.start()

        return True

    def stop(self):

        if not self._running or self._consumers is None:
            return False

        self._running = False

        for consumer in self._consumers:
            consumer.stop()

        return True

    def is_running(self):

        return self._running


class MQCycleTransProducer(MQTransProducer, LoopTask):

    def __init__(self, message_handler, instance_id, topic_name, group_id, mq_client, debug=False):

        MQTransProducer.__init__(self, instance_id, topic_name, group_id, mq_client, debug)
        LoopTask.__init__(self, self._do_task, 1)

        self._message_handler = message_handler

    async def _do_task(self):

        msg_list = None

        try:
            msg_list = await self.consume_half_message(16, 30)
        except MQServerException as err:
            if err.type == r'MessageNotExist':
                Utils.log.debug(err)
            else:
                Utils.log.error(err)

        if msg_list:
            await Utils.awaitable_wrapper(
                self._message_handler(self, msg_list)
            )


class MQMultiCycleTransProducers(TaskInterface):

    def __init__(self, client_num, host, access_id, access_key, security_token=r'', debug=False, logger=None):

        self._clients = [
            MQClient(host, access_id, access_key, security_token, debug, logger)
            for _ in range(client_num)
        ]

        self._producers = None

        self._running = False

    def init_producers(self,
                       message_handler, instance_id, topic_name, group_id, debug=False,
                       *, producer_cls=MQCycleTransProducer
                       ):

        if self._producers:
            self.stop()

        self._producers = [
            producer_cls(message_handler, instance_id, topic_name, group_id, client, debug)
            for client in self._clients
        ]

    def start(self):

        if self._running or self._producers is None:
            return False

        self._running = True

        for consumer in self._producers:
            consumer.start()

        return True

    def stop(self):

        if not self._running or self._producers is None:
            return False

        self._running = False

        for producer in self._producers:
            producer.stop()

        return True

    def is_running(self):

        return self._running
