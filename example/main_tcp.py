# -*- coding: utf-8 -*-

import os
import sys

os.chdir(os.path.dirname(__file__))
sys.path.insert(0, os.path.abspath(r'../'))

from hagworm.frame.tornado.socket import Protocol, Launcher
from hagworm.extend.base import Utils

from setting import ConfigStatic, ConfigDynamic
from service.base import DataSource


class EchoProtocol(Protocol):

    async def connection_made(self):

        Utils.log.info(f'connection made: {self.client_address}')

    async def connection_lost(self):

        Utils.log.info(f'connection lost: {self.client_address}')

    async def data_received(self, chunk):

        await self.data_write(chunk)


def main():

    cluster = os.getenv(r'CLUSTER', None)

    if cluster is None:
        ConfigStatic.read(r'./static.conf')
        ConfigDynamic.read(r'./dynamic.conf')
    else:
        ConfigStatic.read(f'./static.{cluster.lower()}.conf')
        ConfigDynamic.read(f'./dynamic.{cluster.lower()}.conf')

    Launcher(
        EchoProtocol,
        ConfigDynamic.Port,
        async_initialize=DataSource.initialize,
        debug=ConfigDynamic.Debug,
        log_level=ConfigDynamic.LogLevel,
        log_file_path=ConfigDynamic.LogFilePath,
        log_file_num_backups=ConfigDynamic.LogFileBackups,
    ).start()


if __name__ == r'__main__':
    main()
