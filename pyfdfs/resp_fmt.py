# coding=utf-8
from __future__ import absolute_import

__author__ = 'mazesoul'

import struct
from datetime import datetime

from pyfdfs.enums import IP_ADDRESS_SIZE, FDFS_STORAGE_ID_MAX_SIZE, FDFS_DOMAIN_NAME_MAX_SIZE, \
    FDFS_VERSION_SIZE, FDFS_SPACE_SIZE_BASE_INDEX, FDFS_GROUP_NAME_MAX_LEN


class BaseAttr(object):
    val = None

    def get_val(self):
        return self.val

    def set_val(self, val):
        self.val = val

    def del_val(self):
        self.val = None


class IntAttr(BaseAttr):
    val = 0


class StrAttr(BaseAttr):
    val = ""

    def set_val(self, val):
        self.val = str(val).strip("\x00")


class DatetimeAttr(BaseAttr):
    val = datetime.fromtimestamp(0).isoformat()

    def set_val(self, val):
        self.val = datetime.fromtimestamp(val).isoformat()


class SpaceAttr(StrAttr):
    def __init__(self, index):
        self.suffix = ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB',)
        self.index = index or 0

    def set_val(self, val):
        multiples = 1024
        if val < multiples:
            return '{0:d}{1}'.format(val, self.suffix[self.index])
        for suffix in self.suffix[self.index:]:
            if val < multiples:
                return '{0:.2f}{1}'.format(val, suffix)
            val //= multiples
        self.val = val


class BaseMeta(type):
    def __new__(cls, name, bases, attrs):
        new_attrs = {}
        str_attrs = attrs.pop("str_attrs", [])
        dt_attrs = attrs.pop("dt_attrs", [])
        space_attrs = attrs.pop("space_attrs", [])
        attr_list = attrs.pop("attributes", [])
        new_attrs["attributes"] = attr_list
        for item in attr_list:
            attr_obj = attrs.get(item)
            if attr_obj is None:
                if item in str_attrs:
                    attr_obj = StrAttr()
                elif item in dt_attrs:
                    attr_obj = DatetimeAttr()
                elif item in space_attrs:
                    attr_obj = SpaceAttr(FDFS_SPACE_SIZE_BASE_INDEX)
                else:
                    attr_obj = IntAttr()
            new_attrs[item] = property(attr_obj.get_val, attr_obj.set_val, attr_obj.del_val)

        for attr_name, attr in attrs.items():
            new_attrs[attr_name] = attr
        new_attrs["get_fmt_size"] = lambda self: struct.calcsize(getattr(self, "fmt", ""))

        def _str_(self):
            str_list = ["%s:" % self.desc]
            for attr_item in self.attributes:
                str_list.append("\t%s = %s\n" % (attr_item.replace("_", " "), getattr(self, attr_item)))

        new_attrs["__str__"] = _str_

        def set_info(self, byte_stream):
            for idx, info in enumerate(struct.unpack(self.fmt, byte_stream)):
                setattr(self, self.attributes[idx], info)

        new_attrs["set_info"] = set_info
        return super(BaseMeta, cls).__new__(cls, name, bases, new_attrs)


class StorageInfo(object):
    __metaclass__ = BaseMeta
    desc = "Storage information"
    fmt = '!B %ds %ds %ds %ds %ds 52QB' % (FDFS_STORAGE_ID_MAX_SIZE, IP_ADDRESS_SIZE, FDFS_DOMAIN_NAME_MAX_SIZE,
                                           IP_ADDRESS_SIZE, FDFS_VERSION_SIZE)

    attributes = ("status", "id", "ip_addr", "domain_name", "src_ip_addr", "version", "totalMB", "freeMB",
                  "upload_prio", "join_time", "up_time", "store_path_count", "subdir_count_per_path",
                  "storage_port", "storage_http_port", "curr_write_path", "total_upload_count",
                  "success_upload_count", "total_append_count", "success_append_count", "total_modify_count",
                  "success_modify_count", "total_truncate_count", "success_truncate_count", "total_setmeta_count",
                  "success_setmeta_count", "total_del_count", "success_del_count", "total_download_count",
                  "success_download_count", "total_getmeta_count", "success_getmeta_count", "total_create_link_count",
                  "success_create_link_count", "total_del_link_count", "success_del_link_count", "total_upload_bytes",
                  "success_upload_bytes", "total_append_bytes", "success_append_bytes", "total_modify_bytes",
                  "success_modify_bytes", "total_download_bytes", "success_download_bytes", "total_sync_in_bytes",
                  "success_sync_in_bytes", "total_sync_out_bytes", "success_sync_out_bytes", "total_file_open_count",
                  "success_file_open_count", "total_file_read_count", "success_file_read_count",
                  "total_file_write_count", "success_file_write_count", "last_source_sync",
                  "last_sync_update", "last_synced_time", "last_heartbeat_time", "if_trunk_server",)

    str_attrs = ("id", "ip_addr", "domain_name", "src_ip_addr", "version",)
    dt_attrs = ("join_time", "up_time", "last_source_sync", "last_sync_update",
                "last_synced_time", "last_heartbeat_time",)
    space_attrs = ("totalMB", "freeMB",)


class GroupInfo(object):
    __metaclass__ = BaseMeta
    desc = "Group information"
    fmt = '!%ds 11Q' % (FDFS_GROUP_NAME_MAX_LEN + 1)

    attributes = ("group_name", "totalMB", "freeMB", "trunk_freeMB", "count", "storage_port", "store_http_port",
                  "active_count", "curr_write_server", "store_path_count", "subdir_count_per_path",
                  "curr_trunk_file_id",)
    str_attrs = ("group_name",)
    space_attrs = ("totalMB", "freeMB","trunk_freeMB",)