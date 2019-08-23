# -*- coding: utf-8 -*-


class TaskInterface:

    def start(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    def is_running(self):
        raise NotImplementedError()
