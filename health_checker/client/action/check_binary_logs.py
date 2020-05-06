# -*- coding:UTF-8 -*-

import logging

from health_checker.client.env import Env
from health_checker.client.util import get_disk_capacity


LOG = logging.getLogger(__name__)


class CheckBinaryLogs(object):

    def __init__(self, params):
        self.params = params

    """
    利用__call__方法实现可调用对象
    __call__方法将对象变成一个可调用的对象。
    只要在类中实现了__call__方法，就可以像普通函数一样调用一个类对象。
    """
    def __call__(self):
        res = {}
        res['log_bin'] = Env.database.get_variables_value("log_bin")
        if res['log_bin'] == "ON":
            variables = ['binlog_format', 'sync_binlog', 'expire_logs_days', 'datadir']
            res.update(Env.database.get_multi_variables_value(*variables))
            res['binlog_size'] = Env.database.get_binlog_size()

        res['sync_binlog'] = int(res['sync_binlog'])
        res['expire_logs_days'] = int(res['expire_logs_days'])

        res['disk_capacity'] = get_disk_capacity(res.pop('datadir'))

        return res
