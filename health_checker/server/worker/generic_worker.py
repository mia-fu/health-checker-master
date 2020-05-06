#!/usr/bin/python
#-*- coding: UTF-8 -*-
import logging
import time

LOG = logging.getLogger(__name__)

"""
每一个 Worker只需要继承 Generic Worker这个父类，
就实现了记录时间、打印日志和处理异常的功能
"""


class GenericWorker(object):
    def __init__(self, server, catalog, name):
        self.server = server
        self.catalog = catalog
        self.name = name

        self.rs = []

    def print_start_info(self):
        self.start_time = time.time()
        LOG.info('开始对该MySQL的"{0}"功能进行健康检查'.format(self.name))

    def print_end_info(self):
        elipse = time.time() - self.start_time
        LOG.info('{0}功能对该MySQL健康检查结束, 执行时间：{1}'.format(self.name, elipse))

    def map(self):
        try:
            self.print_start_info()
            self.execute()
        except Exception, e:
            LOG.info('健康检查失败: {0}, 失败原因是 : {1}'.format(self.name, e))
        else:
            self.print_end_info()

    def execute(self):
        raise NotImplementedError('===== 你还没有实现该功能 =====')

    def reduce(self, other):
        self.rs.extend(other.rs)