# -*- coding: utf-8 -*-

from hagworm.extend.struct import Configure


class _Static(Configure):

    def _init_options(self):

        ##################################################
        # MySql数据库

        self.MySqlMasterServer = self._parser.get_split_host(r'MySql', r'MySqlMasterServer')

        self.MySqlSlaveServer = self._parser.get_split_host(r'MySql', r'MySqlSlaveServer')

        self.MySqlName = self._parser.get(r'MySql', r'MySqlName')

        self.MySqlUser = self._parser.get(r'MySql', r'MySqlUser')

        self.MySqlPasswd = self._parser.get(r'MySql', r'MySqlPasswd')

        self.MySqlMasterMinConn = self._parser.getint(r'MySql', r'MySqlMasterMinConn')

        self.MySqlMasterMaxConn = self._parser.getint(r'MySql', r'MySqlMasterMaxConn')

        self.MySqlSlaveMinConn = self._parser.getint(r'MySql', r'MySqlSlaveMinConn')

        self.MySqlSlaveMaxConn = self._parser.getint(r'MySql', r'MySqlSlaveMaxConn')

        ##################################################
        # Mongo数据库

        self.MongoHost = self._parser.get_split_str(r'Mongo', r'MongoHost')

        self.MongoName = self._parser.get(r'Mongo', r'MongoName')

        self.MongoUser = self._parser.get(r'Mongo', r'MongoUser')

        self.MongoPasswd = self._parser.get(r'Mongo', r'MongoPasswd')

        self.MongoMinConn = self._parser.getint(r'Mongo', r'MongoMinConn')

        self.MongoMaxConn = self._parser.getint(r'Mongo', r'MongoMaxConn')

        ##################################################
        # 缓存

        self.RedisHost = self._parser.get_split_host(r'Redis', r'RedisHost')

        self.RedisBase = self._parser.getint(r'Redis', r'RedisBase')

        self.RedisPasswd = self._parser.getstr(r'Redis', r'RedisPasswd')

        self.RedisMinConn = self._parser.getint(r'Redis', r'RedisMinConn')

        self.RedisMaxConn = self._parser.getint(r'Redis', r'RedisMaxConn')

        self.RedisExpire = self._parser.getint(r'Redis', r'RedisExpire')

        self.RedisKeyPrefix = self._parser.get(r'Redis', r'RedisKeyPrefix')

        ##################################################


class _Dynamic(Configure):

    def _init_options(self):

        ##################################################
        # 基本

        self.Port = self._parser.getint(r'Base', r'Port')

        self.Debug = self._parser.getboolean(r'Base', r'Debug')

        self.GZip = self._parser.getboolean(r'Base', r'GZip')

        self.Secret = self._parser.get(r'Base', r'Secret')

        self.ProcessNum = self._parser.getint(r'Base', r'ProcessNum')

        ##################################################
        # 日志

        self.LogLevel = self._parser.get(r'Log', r'LogLevel')

        self.LogFilePath = self._parser.get(r'Log', r'LogFilePath')

        self.LogFileBackups = self._parser.getint(r'Log', r'LogFileBackups')

        ##################################################
        # 线程/进程

        self.ThreadPoolMaxWorkers = self._parser.getint(r'Pool', r'ThreadPoolMaxWorkers')

        self.ProcessPoolMaxWorkers = self._parser.getint(r'Pool', r'ProcessPoolMaxWorkers')

        ##################################################


ConfigStatic = _Static()
ConfigDynamic = _Dynamic()
