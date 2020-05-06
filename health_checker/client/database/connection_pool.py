# -*- coding:UTF-8 -*-
import logging
import Queue
import MySQLdb

"""
在实际的项目中，一般都会引入数据库连接池。
数据库连接池是一种池化技术，预先创建好数据库连接，
保存在内存中，当需要连接时从中取出即可，使用完后放回连接池。
"""
LOG = logging.getLogger(__name__)


#  首先连接数据库
class ConnectionPool(object):
    def __init__(self, **kwargs):
        self.size = kwargs.get('size', 10)  # 设置队列长队
        self.kwargs = kwargs
        self.conn_queue = Queue.Queue(maxsize=self.size)  # FIFO队列 先进先出

        for i in range(self.size):
            self.conn_queue.put(self._create_new_conn())

    def _create_new_conn(self):
        return MySQLdb.connect(host=self.kwargs.get('host', '127.0.0.1'),
                               user=self.kwargs.get('user'),
                               passwd=self.kwargs.get('password'),
                               port=self.kwargs.get('port', 3306),
                               connect_timeout=5)

    def _put_conn(self, conn):
        self.conn_queue.put(conn)

    def _get_conn(self):
        conn = self.conn_queue.get()
        if conn is None:
            self._create_new_conn()
        return conn

    def exec_sql(self, sql):  # 执行sql语句
        conn = self._get_conn()  # 在连接池中取出一个连接
        try:
            with conn as cur:
                cur.execute(sql)
                return cur.fetchall()
        except MySQLdb.ProgrammingError as e:
            LOG.error("execute sql ({0}) error {1}".format(sql, e))
            raise e
        except MySQLdb.OperationalError as e:
            # create connection if connection has interrupted
            conn = self._create_new_conn()
            raise e
        finally:
            self._put_conn(conn)

    def __del__(self):
        try:
            while True:
                """
                put_nowait与get_nowait方法是两个非阻塞方法：
                put_nowait没有值的话不等，
                get_nowait取不到值也不等了，
                程序不会夯住，但是一定要做异常处理！
                """
                conn = self.conn_queue.get_nowait()
                if conn:
                    conn.close()
        except Queue.Empty:
            pass