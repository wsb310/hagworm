# -*- coding: utf-8 -*-

import os
import sys

os.chdir(os.path.dirname(__file__))
sys.path.insert(0, os.path.abspath(r'../'))

from hagworm.extend.asyncio.base import Launcher
from hagworm.frame.stress_tests import Runner, TaskInterface


class Task(TaskInterface):

    async def run(self):

        for index in range(2):

            await self.sleep(0.1)

            resp_time = self.randint(12345, 98765) / 10000

            if self.randhit([True, False], [32, 8]):
                self._event.success(f'Test{index}', resp_time)
            else:
                self._event.failure(f'Test{index}', resp_time)


if __name__ == r'__main__':

    Launcher(process_num=2).run(Runner(Task).run(8, 32))
