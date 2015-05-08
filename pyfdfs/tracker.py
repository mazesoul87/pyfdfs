# coding=utf-8
from __future__ import absolute_import

__author__ = 'mazesoul'

from pyfdfs.command import CommandHeader, Command
from pyfdfs.resp_fmt import StorageInfo, GroupInfo
from pyfdfs.enums import FDFS_GROUP_NAME_MAX_LEN, IP_ADDRESS_SIZE, \
    TRACKER_PROTO_CMD_SERVER_LIST_STORAGE, TRACKER_PROTO_CMD_SERVER_LIST_ALL_GROUPS


class Tracker(object):
    def __init__(self, pool):
        self.pool = pool

    def list_groups(self):
        """
        :return: List<GroupInfo>
         * TRACKER_PROTO_CMD_SERVER_LIST_GROUP
          # function: list all groups
          # request body: none
          # response body: n group entries, n >= 0, the format of each entry: see GroupInfo struct
        """
        header = CommandHeader(cmd=TRACKER_PROTO_CMD_SERVER_LIST_ALL_GROUPS)
        cmd = Command(pool=self.pool, header=header)
        return cmd.fetch_list(GroupInfo)

    def list_servers(self, group_name, storage_ip=None):
        """
        :param group_name: which group
        :param storage_ip: which storage servers
        :return: List<StorageInfo>

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
        return cmd.fetch_list(StorageInfo)