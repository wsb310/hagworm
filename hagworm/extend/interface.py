# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod


class TaskInterface(ABC):

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def is_running(self):
        pass
