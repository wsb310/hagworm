# -*- coding: utf-8 -*-

import os
import sys

os.chdir(os.path.dirname(__file__))
sys.path.insert(0, os.path.abspath(r'../'))

from hagworm.extend.logging import LogFileRotator
from hagworm.frame.tornado.base import Launcher

from routing import router
from setting import ConfigStatic, ConfigDynamic
from service.base import GlobalDict, DataSource


def main():

    cluster = os.getenv(r'CLUSTER', None)

    if cluster is None:
        ConfigStatic.read(r'./static.conf')
        ConfigDynamic.read(r'./dynamic.conf')
    else:
        ConfigStatic.read(f'./static.{cluster.lower()}.conf')
        ConfigDynamic.read(f'./dynamic.{cluster.lower()}.conf')

    # 初始化全局变量字典（进程间同步）
    GlobalDict()

    Launcher(
        router,
        ConfigDynamic.Port,
        process_num=ConfigDynamic.ProcessNum,
        async_initialize=DataSource.initialize,
        debug=ConfigDynamic.Debug,
        gzip=ConfigDynamic.GZip,
        template_path=r'view',
        cookie_secret=ConfigDynamic.Secret,
        log_level=ConfigDynamic.LogLevel,
        log_file_path=ConfigDynamic.LogFilePath,
        log_file_rotation=LogFileRotator.make(ConfigDynamic.LogFileSplitSize, ConfigDynamic.LogFileSplitTime),
        log_file_retention=ConfigDynamic.LogFileBackups,
    ).start()


if __name__ == r'__main__':

    main()
