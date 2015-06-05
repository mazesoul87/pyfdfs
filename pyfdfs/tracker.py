# coding=utf-8
from __future__ import absolute_import

__author__ = 'mazesoul'

from pyfdfs.command import CommandHeader, Command
from pyfdfs.structs import StorageInfo, GroupInfo
from pyfdfs.enums import FDFS_GROUP_NAME_MAX_LEN, IP_ADDRESS_SIZE, \
    TRACKER_PROTO_CMD_SERVER_LIST_STORAGE, TRACKER_PROTO_CMD_SERVER_LIST_ALL_GROUPS, \
    TRACKER_PROTO_CMD_SERVER_LIST_ONE_GROUP, TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE, \
    TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE, TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ALL, \
    TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ALL


class Tracker(object):
    def __init__(self, pool):
        self.pool = pool

    def list_groups(self):
        """
        :return: List<GroupInfo>

        * TRACKER_PROTO_CMD_SERVER_LIST_GROUP
           # function: list all groups
           # request body: none
           # response body: List<GroupInfo>
        """
        header = CommandHeader(cmd=TRACKER_PROTO_CMD_SERVER_LIST_ALL_GROUPS)
        cmd = Command(pool=self.pool, header=header)
        return cmd.fetch_list(GroupInfo)

    def list_one_group(self, group_name):
        """
        :param: group_name: which group
        :return: GroupInfo

        * TRACKER_PROTO_CMD_SERVER_LIST_ONE_GROUP
           # function: get one group info
           # request body:
              @ FDFS_GROUP_NAME_MAX_LEN bytes: the group name to query
           # response body: GroupInfo
        """
        header = CommandHeader(req_pkg_len=FDFS_GROUP_NAME_MAX_LEN, cmd=TRACKER_PROTO_CMD_SERVER_LIST_ONE_GROUP)
        cmd = Command(pool=self.pool, header=header, fmt='!%ds' % FDFS_GROUP_NAME_MAX_LEN)
        cmd.pack(group_name)
        return cmd.fetch_one(GroupInfo)

    def list_servers(self, group_name, storage_ip=None):
        """
        :param: group_name: which group
        :param: storage_ip: which storage servers
        :return: List<StorageInfo>

        * TRACKER_PROTO_CMD_SERVER_LIST_STORAGE
           # function: list storage servers of a group
           # request body:
              @ FDFS_GROUP_NAME_MAX_LEN bytes: the group name to
              @ IP_ADDRESS_SIZE bytes: this storage server ip address
           # response body: List<StorageInfo>
        """
        ip_len = IP_ADDRESS_SIZE if storage_ip else 0
        header = CommandHeader(req_pkg_len=FDFS_GROUP_NAME_MAX_LEN + ip_len, cmd=TRACKER_PROTO_CMD_SERVER_LIST_STORAGE)
        cmd = Command(pool=self.pool, header=header, fmt="!%ds %ds" % (FDFS_GROUP_NAME_MAX_LEN, ip_len))
        cmd.pack(group_name, storage_ip or "")
        return cmd.fetch_list(StorageInfo)

    def query_store_without_group_one(self):
        """
        :return: StorageInfo
        * TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE
           # function: query storage server for upload, without group name
           # request body: none (no body part)
           # response body: StorageInfo
        """
        header = CommandHeader(cmd=TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE)
        cmd = Command(pool=self.pool, header=header)
        recv_fmt = '!%ds %ds Q B' % (FDFS_GROUP_NAME_MAX_LEN, IP_ADDRESS_SIZE - 1)
        si = StorageInfo()
        si.group_name, si.ip_addr, si.port, si.current_write_path = cmd.fetch_by_fmt(recv_fmt)
        return si

    def query_store_with_group_one(self, group_name):
        """
        :param: group_name: which group
        :return: StorageInfo
        * TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE
           # function: query storage server for upload, with group name
           # request body:
              @ FDFS_GROUP_NAME_MAX_LEN bytes: the group name to
           # response body: StorageInfo
        """
        header = CommandHeader(req_pkg_len=FDFS_GROUP_NAME_MAX_LEN,
                               cmd=TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE)
        cmd = Command(pool=self.pool, header=header, fmt='!%ds' % FDFS_GROUP_NAME_MAX_LEN)
        recv_fmt = '!%ds %ds Q B' % (FDFS_GROUP_NAME_MAX_LEN, IP_ADDRESS_SIZE - 1)
        cmd.pack(group_name)
        si = StorageInfo()
        si.group_name, si.ip_addr, si.port, si.current_write_path = cmd.fetch_by_fmt(recv_fmt)
        return si

    def query_store_without_group_all(self):
        """
        :return: list of StorageInfo
        * TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ALL
           # function: query which storage server to store file
           # request body: none (no body part)
           # response body: list of StorageInfo
        """
        header = CommandHeader(cmd=TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ALL)
        cmd = Command(pool=self.pool, header=header)
        resp, resp_size = cmd.execute()
        server_count = (resp_size - FDFS_GROUP_NAME_MAX_LEN - 1) / (IP_ADDRESS_SIZE - 1 + 8)
        recv_fmt = '!%ds %ds %dQ B' % (FDFS_GROUP_NAME_MAX_LEN, server_count * (IP_ADDRESS_SIZE - 1), server_count)
        result = cmd.unpack(recv_fmt, resp)

        group_name = result[0]
        current_write_path = result[-1]
        si_list = []
        for idx in xrange(server_count):
            si = StorageInfo()
            si.group_name = group_name
            si.current_write_path = current_write_path
            si.ip_addr = result[idx + 1]
            si.port = result[idx + 1 + server_count]
            si_list.append(si)
        return si_list

    def query_store_with_group_all(self, group_name):
        """
        :param group_name: which group
        :return: list of StorageInfo
        * TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ALL
           # function: query which storage server to store file, with group name
           # request body:
              @ FDFS_GROUP_NAME_MAX_LEN bytes: the group name to
           # response body: list of StorageInfo
        """
        header = CommandHeader(cmd=TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ALL)
        cmd = Command(pool=self.pool, header=header, fmt='!%ds' % FDFS_GROUP_NAME_MAX_LEN)
        cmd.pack(group_name)
        resp, resp_size = cmd.execute()
        server_count = (resp_size - FDFS_GROUP_NAME_MAX_LEN - 1) / (IP_ADDRESS_SIZE - 1 + 8)
        recv_fmt = '!%ds %ds %dQ B' % (FDFS_GROUP_NAME_MAX_LEN, server_count * (IP_ADDRESS_SIZE - 1), server_count)
        result = cmd.unpack(recv_fmt, resp)

        group_name = result[0]
        current_write_path = result[-1]
        si_list = []
        for idx in xrange(server_count):
            si = StorageInfo()
            si.group_name = group_name
            si.current_write_path = current_write_path
            si.ip_addr = result[idx + 1]
            si.port = result[idx + 1 + server_count]
            si_list.append(si)
        return si_list
