# coding=utf-8
from __future__ import absolute_import

__author__ = 'mazesoul'

import os
import stat
from pyfdfs.connection import ConnectionPool, Connection
from pyfdfs.tracker import Tracker
from pyfdfs.storage import Storage
from pyfdfs.enums import STORAGE_SET_METADATA_FLAG_OVERWRITE, STORAGE_SET_METADATA_FLAG_MERGE

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

    @staticmethod
    def _check_file(file_name):
        if not os.path.isfile(file_name):
            return False, "%s is not a file." % file_name
        elif not stat.S_ISREG(os.stat(file_name).st_mode):
            return False, "%s is not a regular file." % file_name
        else:
            return True, ""

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
        function: query all storage servers to download the file
        """
        return self.tracker.query_fetch_all(group_name, file_name)

    def upload_file_by_filename(self, file_name, group_name=None, meta_data=None):
        """
        :param file_name: file name for upload
        :param group_name: which group, can be null
        :param meta_data: dictionary, store metadata in it ,can be null
        :return: StorageResponseInfo
        function: upload file to storage server
        """
        is_file, msg = self._check_file(file_name)
        if not is_file:
            raise Exception(msg)
        if group_name is not None:
            storage_info = self.tracker.query_store_with_group_one(group_name)
        else:
            storage_info = self.tracker.query_store_without_group_one()
        storage_server = self._get_storage(storage_info.ip_addr, storage_info.storage_port)
        return storage_server.upload_file_by_filename(file_name, storage_info.current_write_path, meta_data)

    def upload_file_by_buffer(self, file_buffer, ext, group_name=None, meta_data=None):
        """
        :param file_buffer: file name for upload
        :param ext: file ext name
        :param group_name: which group, can be null
        :param meta_data: dictionary, store metadata in it ,can be null
        :return: StorageResponseInfo
        function: upload file buffer to storage server
        """
        if group_name is not None:
            storage_info = self.tracker.query_store_with_group_one(group_name)
        else:
            storage_info = self.tracker.query_store_without_group_one()
        storage_server = self._get_storage(storage_info.ip_addr, storage_info.storage_port)
        return storage_server.upload_file_by_buffer(file_buffer, storage_info.current_write_path,
                                                    meta_data, ext)

    def set_meta(self, file_name, meta_data, group_name=None, overwrite=True):
        """
        :param file_name: which file
        :param meta_data: update info
        :param group_name: which group
        :param overwrite: default True, False for merge & update
        :return: none
        """
        if group_name is not None:
            storage_info = self.tracker.query_store_with_group_one(group_name)
        else:
            storage_info = self.tracker.query_store_without_group_one()
        storage_server = self._get_storage(storage_info.ip_addr, storage_info.storage_port)
        operation_flag = STORAGE_SET_METADATA_FLAG_OVERWRITE if overwrite else STORAGE_SET_METADATA_FLAG_MERGE
        return storage_server.set_meta(file_name, group_name, meta_data, operation_flag)

    def get_meta(self, group_name, file_name):
        """
        :param group_name: group name
        :param file_name: file name
        :return: meta data, dictionary, store metadata in it
        """
        if group_name is not None:
            storage_info = self.tracker.query_store_with_group_one(group_name)
        else:
            storage_info = self.tracker.query_store_without_group_one()
        storage_server = self._get_storage(storage_info.ip_addr, storage_info.storage_port)
        return storage_server.get_meta(group_name, file_name)
