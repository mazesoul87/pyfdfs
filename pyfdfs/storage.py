# coding=utf-8
from __future__ import absolute_import

__author__ = 'mazesoul'

import os

from pyfdfs.connection import ConnectionPool, Connection
from pyfdfs.command import CommandHeader, Command
from pyfdfs.structs import StorageResponseInfo
from pyfdfs.enums import TRACKER_PROTO_PKG_LEN_SIZE, FDFS_RECORD_SEPARATOR, FDFS_FIELD_SEPARATOR, \
    STORAGE_PROTO_CMD_UPLOAD_FILE, FDFS_GROUP_NAME_MAX_LEN, FDFS_FILE_EXT_NAME_MAX_LEN


class Storage(object):
    def __init__(self, host, port, pool_cls=ConnectionPool, conn_cls=Connection, timeout=60, max_conn=2 ** 31):
        self.pool = pool_cls(hosts=(host, port,), conn_cls=conn_cls, timeout=timeout, max_conn=max_conn)

    @staticmethod
    def get_ext(file_name, double_ext=True):
        li = file_name.split(os.extsep)
        if len(li) <= 1:
            return ''
        else:
            if li[-1].find(os.sep) != -1:
                return ''
        if double_ext:
            if len(li) > 2:
                if li[-2].find(os.sep) == -1:
                    return '%s.%s' % (li[-2], li[-1])
        return li[-1]

    @staticmethod
    def pack_meta(meta_data):
        meta_list = ['%s%c%s' % (k, FDFS_FIELD_SEPARATOR, v) for k, v in meta_data.items()]
        return FDFS_RECORD_SEPARATOR.join(meta_list)

    def upload_file_by_buffer(self, file_buffer, meta_data, ext):
        """
        :param file_buffer: file buffer for send
        :param meta_data: dictionary, store metadata in it
        :param ext: file ext name
        :return: StorageResponseInfo
        """
        meta_str = self.pack_meta(meta_data)
        pkg_len = TRACKER_PROTO_PKG_LEN_SIZE + TRACKER_PROTO_PKG_LEN_SIZE + \
                  FDFS_FILE_EXT_NAME_MAX_LEN + len(meta_str) + len(file_buffer)
        header = CommandHeader(req_pkg_len=pkg_len, cmd=STORAGE_PROTO_CMD_UPLOAD_FILE)
        cmd = Command(pool=self.pool, header=header, fmt="!%ds %ds %ds %ds %ds" % (TRACKER_PROTO_PKG_LEN_SIZE,
                                                                                   TRACKER_PROTO_PKG_LEN_SIZE,
                                                                                   FDFS_FILE_EXT_NAME_MAX_LEN,
                                                                                   len(meta_str), len(file_buffer)))
        cmd.pack(len(meta_str), len(file_buffer), ext, meta_str, file_buffer)
        resp, resp_pkg_len = cmd.execute()
        sr = StorageResponseInfo()
        fmt = "!%ds %ds" % (FDFS_GROUP_NAME_MAX_LEN, resp_pkg_len - FDFS_GROUP_NAME_MAX_LEN)
        sr.group_name, sr.filename = cmd.unpack(fmt, resp)
        return sr

    def upload_file_by_filename(self, file_path, meta_data):
        """
        :param file_path: file path for send
        :param meta_data: dictionary, store metadata in it
        :return: StorageResponseInfo
        """
        file_size = os.stat(file_path).st_size
        meta_str = self.pack_meta(meta_data)
        ext = self.get_ext(file_path)
        pkg_len = TRACKER_PROTO_PKG_LEN_SIZE + TRACKER_PROTO_PKG_LEN_SIZE + \
                  FDFS_FILE_EXT_NAME_MAX_LEN + len(meta_str) + file_size
        header = CommandHeader(req_pkg_len=pkg_len, cmd=STORAGE_PROTO_CMD_UPLOAD_FILE)
        cmd = Command(pool=self.pool, header=header, fmt="!%ds %ds %ds %ds" % (TRACKER_PROTO_PKG_LEN_SIZE,
                                                                               TRACKER_PROTO_PKG_LEN_SIZE,
                                                                               FDFS_FILE_EXT_NAME_MAX_LEN,
                                                                               len(meta_str)))
        cmd.pack(len(meta_str), file_size, ext, meta_str)
        resp, resp_pkg_len = cmd.send_file(file_path)
        sr = StorageResponseInfo()
        fmt = "!%ds %ds" % (FDFS_GROUP_NAME_MAX_LEN, resp_pkg_len - FDFS_GROUP_NAME_MAX_LEN)
        sr.group_name, sr.filename = cmd.unpack(fmt, resp)
        return sr
