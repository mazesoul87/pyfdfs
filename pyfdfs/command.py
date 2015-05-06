# coding=utf-8
from __future__ import absolute_import

__author__ = 'mazesoul'

import os
import struct


class CommandHeader(object):
    st = struct.Struct("!QBB")

    def __init__(self, req_pkg_len=0, cmd=0, status=0):
        self.req_pkg_len = req_pkg_len
        self.cmd = cmd
        self.status = status
        self.resp_pkg_len = 0

    def pack_req(self):
        return self.st.pack(self.req_pkg_len, self.cmd, self.status)

    def unpack_resp(self, byte_stream):
        self.resp_pkg_len, self.cmd, self.status = self.st.unpack(byte_stream)

    def resp_header_len(self):
        return self.st.size


class Command(object):
    buffer_size = 4096

    def __init__(self, pool=None, header=None, fmt=None):
        self.pool = pool
        self._conn = None
        self.header = header
        self.buf = self.header.pack()
        self.fmt = fmt

    def get_conn(self):
        if self._conn is None:
            self._conn = self.pool.get_connection()
        return self._conn

    def set_conn(self, value):
        self._conn = value

    def del_conn(self):
        if self._conn:
            self.pool.release(self.conn)
        self._conn = None

    conn = property(get_conn, set_conn, del_conn)

    def pack(self, *values):
        struct.pack_into(self.fmt, self.buf, self.header.resp_header_len(), *values)

    def execute(self):
        """
        :return: response_body, total_response_size
        """
        try:
            self.conn.send(self.buf)
            resp_header = self.conn.recv(self.header.header_len())
            self.header.unpack_resp(resp_header)
            if self.header.status != 0:
                raise Exception('Error: %d, %s' % (self.header.status, os.strerror(self.header.status)))
            recv_buff = []
            total_size = 0
            bytes_size = self.header.resp_pkg_len
            while bytes_size > 0:
                resp = self.conn.recv(self.buffer_size)
                resp_len = len(resp)
                recv_buff.append(resp)
                total_size += resp_len
                bytes_size -= resp_len
            resp_body = ''.join(recv_buff)
            return resp_body, total_size
        except Exception as e:
            if self.conn:
                self.conn.disconnect()
            raise e
        finally:
            del self.conn



