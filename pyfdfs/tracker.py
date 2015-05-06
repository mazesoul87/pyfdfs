# coding=utf-8
from __future__ import absolute_import

__author__ = 'mazesoul'

from pyfdfs.command import CommandHeader, Command
from pyfdfs.resp_fmt import StorageInfo
from pyfdfs.enums import FDFS_GROUP_NAME_MAX_LEN, TRACKER_PROTO_CMD_SERVER_LIST_STORAGE, IP_ADDRESS_SIZE


class Tracker(object):
    def __init__(self, pool):
        self.pool = pool

    def list_servers(self, group_name, storage_ip=None):
        """
        :param group_name: which group
        :param storage_ip: which storage servers
        :return: list of StorageInfo

        * TRACKER_PROTO_CMD_SERVER_LIST_STORAGE
          # function: list storage servers of a group
          # request body:
             @ FDFS_GROUP_NAME_MAX_LEN bytes: the group name to query
             @ IP_ADDRESS_SIZE bytes: this storage server ip address
          # response body: n storage entries, n >= 0, the format of each entry: see StorageInfo struct
        """
        ip_len = IP_ADDRESS_SIZE if storage_ip else 0
        header = CommandHeader(req_pkg_len=FDFS_GROUP_NAME_MAX_LEN + ip_len, cmd=TRACKER_PROTO_CMD_SERVER_LIST_STORAGE)
        cmd = Command(pool=self.pool, header=header, fmt="!%ds %ds" % (FDFS_GROUP_NAME_MAX_LEN, ip_len))
        cmd.pack(group_name, storage_ip or "")
        resp, resp_size = cmd.execute()
        si_list = []
        idx = 0
        si = StorageInfo()
        fmt_size = si.get_fmt_size()
        while resp_size > 0:
            si_list.append(si.set_info(resp[(idx * fmt_size):((idx + 1)* fmt_size)]))
            si = StorageInfo()
            resp_size -= fmt_size
            idx += 1
        return si_list

    def list_group(self):
        pass