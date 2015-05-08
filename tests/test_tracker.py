# coding=utf-8
from __future__ import absolute_import

__author__ = 'mazesoul'

import unittest
from testconfig import config
from pyfdfs.client import FdfsClient


class TestTracker(unittest.TestCase):
    def setUp(self):
        host_list = config["tracker"]["host"].split(",")
        self.client = FdfsClient(host_list)

    def tearDown(self):
        del self.client

    def test_list_servers(self):
        for item in self.client.list_groups():
            print item
            for storage_server in self.client.list_servers(item.group_name):
                print storage_server