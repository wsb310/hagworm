# -*- coding: utf-8 -*-

import os
import sys

os.chdir(os.path.dirname(__file__))
sys.path.insert(0, os.path.abspath(r'../'))

from hagworm.frame.tornado.base import Launcher

from routing import router
from setting import config
from model.base import DataSource


def main():

    cluster = os.getenv(r'CLUSTER', None)

    if cluster is None:
        config.read(r'./config.conf')
    else:
        config.read(r'./config.{0}.conf'.format(cluster.lower()))

    Launcher(
        router,
        config.Port,
        async_initialize=DataSource().initialize,
        debug=config.Debug,
        gzip=config.GZip,
        cookie_secret=config.Secret,
        log_level=config.LogLevel,
        log_file_path=config.LogFilePath,
        log_file_num_backups=config.LogFileBackups,
    ).start()


if __name__ == r'__main__':

    main()
