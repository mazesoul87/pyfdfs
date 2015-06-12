# coding=utf-8
from __future__ import absolute_import

__author__ = 'mazesoul'

import os

from pyfdfs.connection import ConnectionPool, Connection
from pyfdfs.command import CommandHeader, Command
from pyfdfs.structs import StorageResponseInfo
from pyfdfs.enums import TRACKER_PROTO_PKG_LEN_SIZE, FDFS_RECORD_SEPARATOR, FDFS_FIELD_SEPARATOR, \
    STORAGE_PROTO_CMD_UPLOAD_FILE, FDFS_GROUP_NAME_MAX_LEN, FDFS_FILE_EXT_NAME_MAX_LEN, \
    STORAGE_PROTO_CMD_DELETE_FILE, STORAGE_SET_METADATA_FLAG_OVERWRITE, \
    STORAGE_PROTO_CMD_SET_METADATA, STORAGE_PROTO_CMD_GET_METADATA


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
        if not meta_data:
            return ""
        meta_list = ['%s%c%s' % (k, FDFS_FIELD_SEPARATOR, v) for k, v in meta_data.items()]
        return FDFS_RECORD_SEPARATOR.join(meta_list)

    def upload_file_by_buffer(self, file_buffer, current_write_path, meta_data, ext):
        """
        :param file_buffer: file buffer for send
        :param current_write_path: store path index on the storage server
        :param meta_data: dictionary, store metadata in it
        :param ext: file ext name
        :return: StorageResponseInfo

         * STORAGE_PROTO_CMD_UPLOAD_FILE
            # function: upload file to storage server
            # request body:
              @ 1 byte: store path index on the storage server
              @ TRACKER_PROTO_PKG_LEN_SIZE bytes: meta data bytes
              @ TRACKER_PROTO_PKG_LEN_SIZE bytes: file size
              @ FDFS_FILE_EXT_NAME_MAX_LEN bytes: file ext name, do not include dot (.)
              @ meta data bytes: each meta data seperated by \x01,
                                 name and value seperated by \x02
              @ file size bytes: file content
            # response body: StorageResponseInfo
        """
        meta_str = self.pack_meta(meta_data)
        pkg_len = TRACKER_PROTO_PKG_LEN_SIZE + TRACKER_PROTO_PKG_LEN_SIZE + \
                  FDFS_FILE_EXT_NAME_MAX_LEN + len(meta_str) + len(file_buffer)
        header = CommandHeader(req_pkg_len=pkg_len, cmd=STORAGE_PROTO_CMD_UPLOAD_FILE)
        cmd = Command(pool=self.pool, header=header, fmt="!B %ds %ds %ds %ds %ds" % (TRACKER_PROTO_PKG_LEN_SIZE,
                                                                                     TRACKER_PROTO_PKG_LEN_SIZE,
                                                                                     FDFS_FILE_EXT_NAME_MAX_LEN,
                                                                                     len(meta_str), len(file_buffer)))
        cmd.pack(current_write_path, len(meta_str), len(file_buffer), ext, meta_str, file_buffer)
        resp, resp_pkg_len = cmd.execute()
        sr = StorageResponseInfo()
        fmt = "!%ds %ds" % (FDFS_GROUP_NAME_MAX_LEN, resp_pkg_len - FDFS_GROUP_NAME_MAX_LEN)
        sr.group_name, sr.filename = cmd.unpack(fmt, resp)
        return sr

    def upload_file_by_filename(self, file_path, current_write_path, meta_data):
        """
        :param file_path: file path for send
        :param current_write_path: store path index on the storage server
        :param meta_data: dictionary, store metadata in it
        :return: StorageResponseInfo

         * STORAGE_PROTO_CMD_UPLOAD_FILE
            # function: upload file to storage server
            # request body:
              @ 1 byte: store path index on the storage server
              @ TRACKER_PROTO_PKG_LEN_SIZE bytes: meta data bytes
              @ TRACKER_PROTO_PKG_LEN_SIZE bytes: file size
              @ FDFS_FILE_EXT_NAME_MAX_LEN bytes: file ext name, do not include dot (.)
              @ meta data bytes: each meta data seperated by \x01,
                                 name and value seperated by \x02
              @ file size bytes: file content
            # response body: StorageResponseInfo
        """
        file_size = os.stat(file_path).st_size
        meta_str = self.pack_meta(meta_data)
        ext = self.get_ext(file_path)
        pkg_len = TRACKER_PROTO_PKG_LEN_SIZE + TRACKER_PROTO_PKG_LEN_SIZE + \
                  FDFS_FILE_EXT_NAME_MAX_LEN + len(meta_str) + file_size
        header = CommandHeader(req_pkg_len=pkg_len, cmd=STORAGE_PROTO_CMD_UPLOAD_FILE)
        cmd = Command(pool=self.pool, header=header, fmt="!B %ds %ds %ds %ds" % (TRACKER_PROTO_PKG_LEN_SIZE,
                                                                                 TRACKER_PROTO_PKG_LEN_SIZE,
                                                                                 FDFS_FILE_EXT_NAME_MAX_LEN,
                                                                                 len(meta_str)))
        cmd.pack(current_write_path, len(meta_str), file_size, ext, meta_str)
        resp, resp_pkg_len = cmd.send_file(file_path)
        sr = StorageResponseInfo()
        fmt = "!%ds %ds" % (FDFS_GROUP_NAME_MAX_LEN, resp_pkg_len - FDFS_GROUP_NAME_MAX_LEN)
        sr.group_name, sr.filename = cmd.unpack(fmt, resp)
        return sr

    def delete_file(self, group_name, file_name):
        """
        :param group_name:
        :param file_name:
        :return: none

        * STORAGE_PROTO_CMD_DELETE_FILE
           # function: delete file from storage server
           # request body:
             @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
             @ filename bytes: filename
           # response body: none
        """
        file_name_len = len(file_name)
        header = CommandHeader(req_pkg_len=FDFS_GROUP_NAME_MAX_LEN + file_name_len,
                               cmd=STORAGE_PROTO_CMD_DELETE_FILE)
        cmd = Command(pool=self.pool, header=header, fmt="! %ds %ds" % (FDFS_GROUP_NAME_MAX_LEN, file_name_len))
        cmd.pack(group_name, file_name)
        cmd.execute()

    def set_meta(self, file_name, group_name, meta_data, operation_flag=STORAGE_SET_METADATA_FLAG_OVERWRITE):
        """
        :param file_name: file name
        :param group_name: group name
        :param meta_data: dictionary, store metadata in it
        :param operation_flag: 'O' for overwrite all old metadata
                                'M' for merge, insert when the meta item not exist, otherwise update it
        :return: none

        * STORAGE_PROTO_CMD_SET_METADATA
           # function: delete file from storage server
           # request body:
             @ TRACKER_PROTO_PKG_LEN_SIZE bytes: filename length
             @ TRACKER_PROTO_PKG_LEN_SIZE bytes: meta data size
             @ 1 bytes: operation flag,
                   'O' for overwrite all old metadata
                   'M' for merge, insert when the meta item not exist, otherwise update it
             @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
             @ filename bytes: filename
             @ meta data bytes: each meta data seperated by \x01,
                                 name and value seperated by \x02
           # response body: none
        """
        file_name_len = len(file_name)
        meta_str = self.pack_meta(meta_data)
        meta_len = len(meta_str)
        pkg_len = TRACKER_PROTO_PKG_LEN_SIZE + TRACKER_PROTO_PKG_LEN_SIZE + 1 + FDFS_GROUP_NAME_MAX_LEN + \
                  file_name_len + meta_len
        header = CommandHeader(req_pkg_len=pkg_len, cmd=STORAGE_PROTO_CMD_SET_METADATA)
        cmd_fmt = "!%ds %ds c %ds %ds %ds" %(TRACKER_PROTO_PKG_LEN_SIZE, TRACKER_PROTO_PKG_LEN_SIZE,
                                             FDFS_GROUP_NAME_MAX_LEN, file_name_len, meta_len)
        cmd = Command(pool=self.pool, header=header, fmt=cmd_fmt)
        cmd.pack(file_name_len, meta_len, operation_flag, group_name, meta_str)
        cmd.execute()

    def get_meta(self, group_name, file_name):
        """
        :param group_name: group name
        :param file_name: file name
        :return: meta data, dictionary, store metadata in it

        * STORAGE_PROTO_CMD_GET_METADATA
            # function: get metat data from storage server
            # request body:
              @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
              @ filename bytes: filename
            # response body
              @ meta data buff, each meta data seperated by \x01, name and value seperated by \x02
        """
        header = CommandHeader(req_pkg_len=FDFS_GROUP_NAME_MAX_LEN + len(file_name), cmd=STORAGE_PROTO_CMD_GET_METADATA)
        cmd = Command(pool=self.pool, header=header, fmt="!%ds %ds" % (FDFS_GROUP_NAME_MAX_LEN, len(file_name)))
        cmd.pack(group_name, file_name)
        resp, resp_size = cmd.execute()
        meta_data = {}
        for item in resp.split(FDFS_RECORD_SEPARATOR):
            k, v = item.split(FDFS_FIELD_SEPARATOR)
            meta_data[k] = v
        return meta_data

