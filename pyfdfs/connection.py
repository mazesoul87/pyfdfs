# coding=utf-8
from __future__ import absolute_import, with_statement

__author__ = 'mazesoul'

import os
import sys
import random
import socket
import threading
from itertools import chain


class Connection(object):
    """
    Manages TCP communication to and from Fast DFS server
    """
    description_format = "Connection<host=%(remote_addr)s,port=%(remote_port)s>"

    def __init__(self, **conn_kwargs):
        self.pid = os.getpid()
        self.hosts = conn_kwargs["hosts"]
        self.remote_addr = None
        self.remote_port = None
        self.sock = None
        self.timeout = conn_kwargs['timeout']

    def __repr__(self):
        return self.description_format % {
            "remote_addr": self.remote_addr,
            "remote_port": self.remote_port
        }

    def __del__(self):
        try:
            self.disconnect()
        except:
            pass

    def connect(self):
        if self.sock:
            return
        self.remote_addr, self.remote_port = random.choice(self.hosts)
        try:
            sock = socket.create_connection((self.remote_addr, self.remote_port,), self.timeout)
        except socket.error:
            e = sys.exc_info()[1]
            raise Exception(self._error_message(e))
        self.sock = sock

    def _error_message(self, exception):
        """
        args for socket.error can either be (errno, "message") or just 'message'
        """
        if len(exception.args) == 1:
            return "Error connecting to %s:%s. %s." % \
                   (self.remote_addr, self.remote_port, exception.args[0])
        else:
            return "Error %s connecting to %s:%s. %s." % \
                   (exception.args[0], self.remote_addr, self.remote_port, exception.args[1])

    def disconnect(self):
        """
        disconnects from the fast dfs server
        """
        if self.sock is None:
            return
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
        except socket.error:
            pass
        self.sock = None

    def recv(self, byte_size, buffer_size=4096):
        if self.sock is None:
            self.connect()
        recv_buff = []
        try:
            while byte_size > 0:
                resp = self.sock.recv(buffer_size if buffer_size <= byte_size else byte_size)
                recv_buff.append(resp)
                byte_size -= len(resp)
        except (socket.error, socket.timeout), e:
            raise Exception('Error: while reading from socket: (%s)' % e.args)
        return ''.join(recv_buff)

    def send(self, byte_stream):
        if self.sock is None:
            self.connect()
        try:
            self.sock.sendall(byte_stream)
        except (socket.error, socket.timeout), e:
            raise Exception('Error: while writing to socket: (%s)' % e.args)


class ConnectionPool(object):
    """
    Generic connection pool
    """
    pid = os.getpid()
    max_conn = 2 ** 31
    _created_connections = 0
    _available_connections = []
    _in_use_connections = set()
    check_lock = threading.Lock()

    def __init__(self, conn_cls=Connection, max_conn=None, **connection_kwargs):
        if max_conn is not None:
            if not isinstance(max_conn, (int, long)) or max_conn < 0:
                raise ValueError('"max_conn" must be a positive integer')
            self.max_conn = max_conn
        self.conn_cls = conn_cls
        self.connection_kwargs = connection_kwargs
        self.reset()

    def reset(self):
        self.pid = os.getpid()
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()

    def __repr__(self):
        return "%s<%s:%s>" % (
            type(self).__name__,
            self.conn_cls.__name__,
            self.connection_kwargs["hosts"],
        )

    def _check_pid(self):
        if self.pid != os.getpid():
            with self.check_lock:
                if self.pid == os.getpid():
                    return
                self.destroy()

    def get_connection(self):
        """
        get a connection from the pool
        """
        self._check_pid()
        try:
            connection = self._available_connections.pop()
        except IndexError:
            connection = self.make_connection()
        self._in_use_connections.add(connection)
        return connection

    def make_connection(self):
        """
        create a new connection
        """
        if self._created_connections >= self.max_conn:
            raise Exception("Too many connections")
        conn_instance = None
        num_try = 10
        while 1:
            try:
                if num_try <= 0:
                    sys.exit()
                conn_instance = self.conn_cls(**self.connection_kwargs)
                conn_instance.connect()
                self._created_connections += 1
                break
            except Exception as e:
                print("%s %s retry:[%s]" % (self, e, num_try))
                num_try -= 1
        return conn_instance

    def release(self, connection):
        """
        release the connection back to the pool
        """
        self._check_pid()
        if connection.pid != self.pid:
            return
        self._in_use_connections.remove(connection)
        if connection.sock:
            self._available_connections.append(connection)

    def destroy(self):
        """
        disconnects all connections in the pool
        """
        all_conns = chain(self._available_connections, self._in_use_connections)
        for conn in all_conns:
            conn.disconnect()
        self.reset()
