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
        self.buf = self.header.pack_req()
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
        st = struct.Struct(self.fmt)
        self.buf = "%s%s" % (self.buf, st.pack(*values))

    def execute(self):
        """
        :return: response_body, total_response_size
        """
        try:
            self.conn.send(self.buf)
            resp_header = self.conn.recv(self.header.resp_header_len())
            self.header.unpack_resp(resp_header)
            if self.header.status != 0:
                raise Exception('Error: %d, %s' % (self.header.status, os.strerror(self.header.status)))
            resp_body = self.conn.recv(self.header.resp_pkg_len)
            return resp_body, self.header.resp_pkg_len
        except Exception as e:
            if self.conn:
                self.conn.disconnect()
            raise e
        finally:
            del self.conn

    def fetch_by_fmt(self, fmt):
        resp, resp_size = self.execute()
        return struct.unpack(fmt, resp)

    def fetch_list(self, item_cls):
        resp, resp_size = self.execute()
        ret_list = []
        idx = 0
        item = item_cls()
        fmt_size = item.get_fmt_size()
        while resp_size > 0:
            item.set_info(resp[idx * fmt_size:(idx + 1) * fmt_size])
            ret_list.append(item)
            item = item_cls()
            resp_size -= fmt_size
            idx += 1
        return ret_list

    def fetch_one(self, item_cls):
        resp, resp_size = self.execute()
        ret = item_cls()
        ret.set_info(resp)
        return ret