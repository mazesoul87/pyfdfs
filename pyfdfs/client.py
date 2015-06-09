# coding=utf-8
from __future__ import absolute_import

__author__ = 'mazesoul'

from pyfdfs.connection import ConnectionPool, Connection
from pyfdfs.tracker import Tracker
from pyfdfs.storage import Storage

class FdfsClient(object):
    def __init__(self, host_list, pool_cls=ConnectionPool, conn_cls=Connection, timeout=60, max_conn=2 ** 31):
        hosts = []
        for item in host_list:
            addr, port = item.split(":")
            hosts.append((str(addr), int(port),))
        self.tracker_pool = pool_cls(hosts=hosts, conn_cls=conn_cls, timeout=timeout, max_conn=max_conn)
        self.tracker = Tracker(self.tracker_pool)
        self.pool_cls = pool_cls
        self.conn_cls = conn_cls
        self.timeout = timeout
        self.max_conn = max_conn
        self.storage_servers = {}

    def __del__(self):
        try:
            self.tracker_pool.destroy()
            self.tracker_pool = None
            for k, v in self.storage_servers.iteritems():
                v.pool.destroy()
        except Exception, e:
            print("Error: %s" % e)
            pass

    def _get_storage(self, host, port):
        """
        :param host: which storage server host
        :param port: which storage server port
        :return: Storage Object
        """
        storage = self.storage_servers.get((host, port,))
        if storage is None:
            storage = Storage(host, port, pool_cls=self.pool_cls, conn_cls=self.conn_cls,
                              timeout=self.timeout, max_conn=self.max_conn)
            self.storage_servers[(host, port,)] = storage
        return storage

    def list_groups(self):
        """
        :return: List<GroupInfo>
        function: list all groups
        """
        return self.tracker.list_groups()

    def list_one_group(self, group_name):
        """
        :param: group_name: which group
        :return: GroupInfo
        function: get one group info
        """
        return self.tracker.list_one_group(group_name)

    def list_servers(self, group_name, storage_ip=None):
        """
        :param: group_name: which group
        :param: storage_ip: which storage servers
        :return: List<StorageInfo>
        function: list storage servers of a group
        """
        return self.tracker.list_servers(group_name, storage_ip)

    def query_store_without_group_one(self):
        """
        :return: BasicStorageInfo
        function: query storage server for upload, without group name
        """
        return self.tracker.query_store_without_group_one()

    def query_store_with_group_one(self, group_name):
        """
        :param: group_name: which group
        :return: BasicStorageInfo
        function: query storage server for upload, with group name
        """
        return self.tracker.query_store_with_group_one(group_name)

    def query_store_without_group_all(self):
        """
        :return: List<BasicStorageInfo>
        function: query which storage server to store file
        """
        return self.tracker.query_store_without_group_all()

    def query_store_with_group_all(self, group_name):
        """
        :param: group_name: which group
        :return: List<BasicStorageInfo>
        function: query storage server for upload, with group name
        """
        return self.tracker.query_store_with_group_all(group_name)

    def query_fetch_one(self, group_name, file_name):
        """
        :param group_name: which group
        :param file_name: which file
        :return: BasicStorageInfo
        function: query which storage server to download the file
        """
        return self.tracker.query_fetch_one(group_name, file_name)

    def query_fetch_all(self, group_name, file_name):
        """
        :param group_name: which group
        :param file_name: which file
        :return: List<BasicStorageInfo>
        """
        return self.tracker.query_fetch_all(group_name, file_name)
