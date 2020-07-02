# -*- coding: utf-8 -*-

from mq_http_sdk.mq_client import MQClient as _MQClient
from mq_http_sdk.mq_consumer import MQConsumer as _MQConsumer
from mq_http_sdk.mq_producer import MQProducer as _MQProducer, MQTransProducer as _MQTransProducer

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

    async def consume_half_message(self, batch_size=1, wait_seconds=-1):

        return await self.mq_client.thread_pool.run(super().consume_half_message, batch_size, wait_seconds)

    async def commit(self, receipt_handle):

        return await self.mq_client.thread_pool.run(super().commit, receipt_handle)

    async def rollback(self, receipt_handle):

        return await self.mq_client.thread_pool.run(super().rollback, receipt_handle)


class MQClient(_MQClient):

    def __init__(self, host, access_id, access_key, security_token=r'', debug=False, logger=None):

        super().__init__(host, access_id, access_key, security_token, debug, logger)

        # 原版SDK中的MQClient线程不安全，并利用线程池仅有一个线程这种特例，保证线程安全
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
