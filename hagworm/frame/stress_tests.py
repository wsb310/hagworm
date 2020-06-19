# -*- coding: utf-8 -*-

import os

from terminal_table import Table

from hagworm.extend.interface import RunnableInterface
from hagworm.extend.asyncio.base import Launcher as _Launcher
from hagworm.extend.asyncio.base import Utils, MultiTasks, AsyncCirculatorForSecond
from hagworm.extend.asyncio.zmq import Subscriber, PublisherWithBuffer


SIGNAL_PROTOCOL = r'tcp'
SIGNAL_PORT = 0x310
HIGH_WATER_MARK = 0xffffff


class Guardian(RunnableInterface):

    async def _do_polling(self, pids, hwm):

        with Reporter() as reporter:

            reporter.set_hwm(hwm)

            async for _ in AsyncCirculatorForSecond():

                for pid in pids.copy():
                    if os.waitpid(pid, os.WNOHANG)[0] == pid:
                        pids.remove(pid)

                if len(pids) == 0:
                    break

            Utils.log.info(f'\n{reporter.get_report_table()}')

    def run(self, pids):

        global HIGH_WATER_MARK

        Utils.run_until_complete(self._do_polling(pids, HIGH_WATER_MARK))


class Launcher(_Launcher):

    def __init__(self,
                 log_file_path=None, log_level=r'INFO', log_file_num_backups=7,
                 process_number=1, process_guardian=None,
                 debug=False
                 ):

        if process_guardian is None:
            process_guardian = Guardian().run

        super().__init__(
            log_file_path, log_level, log_file_num_backups,
            process_number, process_guardian,
            debug
        )

    def run(self, func, *args, **kwargs):

        if self._process_number > 1:

            super().run(func, *args, **kwargs)

        else:

            async def _func():

                nonlocal func, args, kwargs

                with Reporter() as reporter:
                    await func(*args, **kwargs)
                    Utils.log.info(f'\n{reporter.get_report_table()}')

            super().run(_func)


class Reporter(Subscriber):

    class _Report:

        def __init__(self):
            self.success = []
            self.failure = []

    def __init__(self):

        global SIGNAL_PROTOCOL, SIGNAL_PORT, HIGH_WATER_MARK

        super().__init__(f'{SIGNAL_PROTOCOL}://*:{SIGNAL_PORT}', True)

        self.set_hwm(HIGH_WATER_MARK)

        self._reports = {}

    async def _message_handler(self, data):

        for name, result, resp_time in data:
            if name and result in (r'success', r'failure'):
                getattr(self._get_report(name), result).append(resp_time)

    def _get_report(self, name: str) -> _Report:

        if name not in self._reports:
            self._reports[name] = self._Report()

        return self._reports[name]

    def get_report_table(self) -> str:

        reports = []

        for key, val in self._reports.items():
            reports.append(
                (
                    key,
                    len(val.success),
                    len(val.failure),
                    r'{:.2%}'.format(len(val.success) / (len(val.success) + len(val.failure))),
                    r'{:.3f}s'.format(sum(val.success) / len(val.success) if len(val.success) > 0 else 0),
                    r'{:.3f}s'.format(min(val.success) if len(val.success) > 0 else 0),
                    r'{:.3f}s'.format(max(val.success) if len(val.success) > 0 else 0),
                )
            )

        return Table.create(
            reports,
            (
                r'EventName',
                r'SuccessTotal',
                r'FailureTotal',
                r'SuccessRatio',
                r'SuccessAveTime',
                r'SuccessMinTime',
                r'SuccessMaxTime',
            ),
            use_ansi=False
        )


class TaskInterface(Utils, RunnableInterface):

    def __init__(self, publisher: PublisherWithBuffer):

        self._publisher = publisher

    def success(self, name: str, resp_time: int):

        self._publisher.append(
            (name, r'success', resp_time,)
        )

    def failure(self, name: str, resp_time: int):

        self._publisher.append(
            (name, r'failure', resp_time,)
        )

    async def run(self):
        raise NotImplementedError()


class Runner(Utils, RunnableInterface):

    def __init__(self, task_cls: TaskInterface):

        global SIGNAL_PROTOCOL, SIGNAL_PORT

        self._task_cls = task_cls
        self._publisher = PublisherWithBuffer(f'{SIGNAL_PROTOCOL}://localhost:{SIGNAL_PORT}', False)

    async def run(self, times, task_num):

        self._publisher.open()

        for _ in range(times):

            tasks = MultiTasks()

            for _ in range(task_num):
                tasks.append(self._task_cls(self._publisher).run())

            await tasks

        await self._publisher.safe_close()
