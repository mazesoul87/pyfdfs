# coding=utf-8
from __future__ import absolute_import

__author__ = 'mazesoul'

from pyfdfs.command import CommandHeader,Command


class Storage(object):
    def __init__(self, pool):
        self.pool = pool

    def upload_file(self):
        pass