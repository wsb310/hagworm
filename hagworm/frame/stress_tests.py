# -*- coding: utf-8 -*-

from terminal_table import Table

from hagworm.extend.interface import RunnableInterface
from hagworm.extend.asyncio.base import Utils, MultiTasks


class _Report:

    def __init__(self):
        self._success = []
        self._failure = []

    @property
    def success(self) -> list:
        return self._success

    @property
    def failure(self) -> list:
        return self._failure


class Event:

    def __init__(self):
        self._reports = {}

    @property
    def reports(self):
        return self._reports

    def _get_report(self, name: str) -> _Report:

        if name not in self._reports:
            self._reports[name] = _Report()

        return self._reports[name]

    def success(self, name: str, resp_time: float):
        self._get_report(name).success.append(resp_time)

    def failure(self, name: str, resp_time: float):
        self._get_report(name).failure.append(resp_time)


class TaskInterface(Utils, RunnableInterface):

    def __init__(self, pname: str, index: int, event: Event):

        self._pname = pname
        self._index = index
        self._event = event

    @property
    def name(self) -> str:
        return f'{self._pname}_{self._index}'

    async def run(self):
        raise NotImplementedError()


class Runner(Utils, RunnableInterface):

    def __init__(self, task_cls: TaskInterface):

        self._name = self.uuid1()[:8]
        self._task_cls = task_cls

    async def run(self, times, task_num):

        event = Event()

        for _time in range(times):

            tasks = MultiTasks()

            for _index in range(task_num):
                tasks.append(self._task_cls(self._name, _index, event).run())

            await tasks

        self.log.info(f'Runner: {self._name}\n{self._get_event_table(event)}')

    def _get_event_table(self, event: Event) -> str:

        reports = []

        for key, val in event.reports.items():
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
