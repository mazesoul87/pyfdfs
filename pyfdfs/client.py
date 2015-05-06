# coding=utf-8
from __future__ import absolute_import

__author__ = 'mazesoul'

from pyfdfs.connection import ConnectionPool, Connection
from pyfdfs.tracker import Tracker


class FdfsClient(object):
    def __init__(self, host_tuple, pool_cls=ConnectionPool, conn_cls=Connection, timeout=60, max_conn=2 ** 31):
        self.tracker_pool = pool_cls(host_tuple=host_tuple, conn_cls=conn_cls, timeout=timeout, max_conn=max_conn)
        self.tracker = Tracker(self.tracker_pool)
        self.pool_cls = pool_cls
        self.conn_cls = conn_cls
        self.timeout = timeout
        self.max_conn = max_conn
        self.storage_servers = {}

    def __del__(self):
        try:
            self.tracker_pool.destory()
            self.tracker_pool = None
            for k, v in self.storage_servers.iteritems():
                v.pool.destory()
        except:
            pass

    def list_servers(self, group_name, storage_ip=None):
        return self.tracker.list_servers(group_name, storage_ip)