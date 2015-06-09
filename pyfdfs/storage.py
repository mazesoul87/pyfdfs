# coding=utf-8
from __future__ import absolute_import

__author__ = 'mazesoul'

from pyfdfs.connection import ConnectionPool, Connection
from pyfdfs.command import CommandHeader, Command
from pyfdfs.structs import StorageResponseInfo
from pyfdfs.enums import TRACKER_PROTO_PKG_LEN_SIZE, FDFS_RECORD_SEPARATOR, FDFS_FIELD_SEPARATOR, \
    STORAGE_PROTO_CMD_UPLOAD_FILE, FDFS_GROUP_NAME_MAX_LEN


class Storage(object):
    def __init__(self, host, port, pool_cls=ConnectionPool, conn_cls=Connection, timeout=60, max_conn=2 ** 31):
        self.pool = pool_cls(hosts=(host, port,), conn_cls=conn_cls, timeout=timeout, max_conn=max_conn)

    @staticmethod
    def pack_meta(meta_data):
        meta_list = ['%s%c%s' % (k, FDFS_FIELD_SEPARATOR, v) for k, v in meta_data.items()]
        return FDFS_RECORD_SEPARATOR.join(meta_list)

    def upload_file_by_buffer(self, file_buffer, meta_data):
        """
        :param file_buffer: file buffer for send
        :param meta_data: dictionary, store metadata in it
        :return: StorageResponseInfo
        """
        meta_str = self.pack_meta(meta_data)
        pkg_len = TRACKER_PROTO_PKG_LEN_SIZE + TRACKER_PROTO_PKG_LEN_SIZE + len(meta_str) + len(file_buffer)
        header = CommandHeader(req_pkg_len=pkg_len, cmd=STORAGE_PROTO_CMD_UPLOAD_FILE)
        cmd = Command(pool=self.pool, header=header, fmt="!%ds %ds %ds %ds" % (TRACKER_PROTO_PKG_LEN_SIZE,
                                                                               TRACKER_PROTO_PKG_LEN_SIZE,
                                                                               len(meta_str), len(file_buffer)))
        cmd.pack(len(meta_str), len(file_buffer), meta_str, file_buffer)
        resp, resp_pkg_len = cmd.execute()
        sr = StorageResponseInfo()
        fmt = "!%ds %ds" % (FDFS_GROUP_NAME_MAX_LEN, resp_pkg_len - FDFS_GROUP_NAME_MAX_LEN)
        sr.group_name, sr.filename = cmd.unpack(fmt, resp)
        return sr

    def upload_file_by_filename(self, file_path, meta_data):
        pass