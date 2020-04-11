# -*- coding: utf-8 -*-


class RunnableInterface:
    """Runnable接口定义
    """

    def run(self):
        raise NotImplementedError()


class TaskInterface:
    """Task接口定义
    """

    def start(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    def is_running(self):
        raise NotImplementedError()


class ObjectFactoryInterface:
    """对象工厂类接口定义
    """

    def create(self):
        raise NotImplementedError()
