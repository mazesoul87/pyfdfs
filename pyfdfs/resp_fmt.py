# coding=utf-8
from __future__ import absolute_import

__author__ = 'mazesoul'

import struct
from datetime import datetime

from pyfdfs.enums import IP_ADDRESS_SIZE, FDFS_STORAGE_ID_MAX_SIZE, FDFS_DOMAIN_NAME_MAX_SIZE, \
    FDFS_VERSION_SIZE, FDFS_SPACE_SIZE_BASE_INDEX, FDFS_GROUP_NAME_MAX_LEN


class BaseAttr(object):
    val = None

    def __get__(self, obj, val):
        return self.val

    def __set__(self, obj, val):
        self.val = val

    def __delete__(self):
        self.val = None


class IntAttr(BaseAttr):
    val = 0


class StrAttr(BaseAttr):
    val = ""

    def __set__(self, obj, val):
        self.val = str(val).strip("\x00")


class DatetimeAttr(BaseAttr):
    val = datetime.fromtimestamp(0).isoformat()

    def __set__(self, obj, val):
        self.val = datetime.fromtimestamp(val).isoformat()


class SpaceAttr(StrAttr):
    def __init__(self, index=None):
        self.suffix = ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB',)
        self.index = index or 0

    def __set__(self, obj, val):
        multiples = 1024.0
        if val < multiples:
            self.val = '{0:d}{1}'.format(val, self.suffix[self.index])
            return
        for suffix in self.suffix[self.index:]:
            if val < multiples:
                self.val = '{0:.2f}{1}'.format(val, suffix)
                return
            val /= multiples
        self.val = val


class BaseInfo(object):
    def __str__(self):
        str_list = ["%s:\n" % self.desc]
        for attr_item in self.attributes:
            str_list.append("\t%s = %s\n" % (attr_item.replace("_", " "), getattr(self, attr_item)))
        return "".join(str_list)

    def get_fmt_size(self):
        return struct.calcsize(getattr(self, "fmt", ""))

    def set_info(self, byte_stream):
        for idx, info in enumerate(struct.unpack(self.fmt, byte_stream)):
            setattr(self, self.attributes[idx], info)


class BaseMeta(type):
    def __new__(cls, name, bases, attrs):
        new_attrs = {}
        str_attrs = attrs.pop("str_attrs", [])
        date_attrs = attrs.pop("date_attrs", [])
        space_attrs = attrs.pop("space_attrs", [])
        attr_list = attrs.pop("attributes", [])
        new_attrs["attributes"] = attr_list
        for item in attr_list:
            attr_obj = attrs.get(item)
            if attr_obj is None:
                if item in str_attrs:
                    attr_obj = StrAttr()
                elif item in date_attrs:
                    attr_obj = DatetimeAttr()
                elif item in space_attrs:
                    attr_obj = SpaceAttr()
                else:
                    attr_obj = IntAttr()
            new_attrs[item] = attr_obj
        for attr_name, attr in attrs.items():
            new_attrs[attr_name] = attr
        return super(BaseMeta, cls).__new__(cls, name, (BaseInfo,) + bases, new_attrs)


class StorageInfo(object):
    __metaclass__ = BaseMeta
    desc = "Storage information"
    fmt = '!B %ds %ds %ds %ds %ds 10Q 3L 42Q B' % (FDFS_STORAGE_ID_MAX_SIZE, IP_ADDRESS_SIZE,
                                                   FDFS_DOMAIN_NAME_MAX_SIZE, FDFS_STORAGE_ID_MAX_SIZE,
                                                   FDFS_VERSION_SIZE)

    attributes = ("status", "id", "ip_addr", "domain_name", "src_ip", "version", "join_time", "up_time",
                  "total_mb", "free_mb", "upload_priority", "store_path_count", "subdir_count_per_path",
                  "current_write_path", "storage_port", "storage_http_port",
                  "alloc_count", "current_count", "max_count",
                  "total_upload_count", "success_upload_count", "total_append_count", "success_append_count",
                  "total_modify_count", "success_modify_count", "total_truncate_count", "success_truncate_count",
                  "total_set_meta_count", "success_set_meta_count", "total_delete_count", "success_delete_count",
                  "total_download_count", "success_download_count", "total_get_meta_count", "success_get_meta_count",
                  "total_create_link_count", "success_create_link_count", "total_delete_link_count",
                  "success_delete_link_count", "total_upload_bytes", "success_upload_bytes", "total_append_bytes",
                  "success_append_bytes", "total_modify_bytes", "success_modify_bytes", "total_download_bytes",
                  "success_download_bytes", "total_sync_in_bytes", "success_sync_in_bytes", "total_sync_out_bytes",
                  "success_sync_out_bytes", "total_file_open_count", "success_file_open_count", "total_file_read_count",
                  "success_file_read_count", "total_file_write_count", "success_file_write_count", "last_source_update",
                  "last_sync_update", "last_synced_timestamp", "last_heart_beat_time", "if_trunk_server",)

    str_attrs = ("id", "ip_addr", "domain_name", "src_ip", "version",)
    date_attrs = ("join_time", "up_time", "last_source_update", "last_sync_update",
                  "last_synced_timestamp", "last_heart_beat_time",)
    space_attrs = ("total_upload_bytes", "success_upload_bytes", "total_append_bytes",
                   "success_append_bytes", "total_modify_bytes", "success_modify_bytes", "total_download_bytes",
                   "success_download_bytes", "total_sync_in_bytes", "success_sync_in_bytes", "total_sync_out_bytes",
                   "success_sync_out_bytes",)
    total_mb = SpaceAttr(FDFS_SPACE_SIZE_BASE_INDEX)
    free_mb = SpaceAttr(FDFS_SPACE_SIZE_BASE_INDEX)


class GroupInfo(object):
    __metaclass__ = BaseMeta
    desc = "Group information"
    fmt = '!%ds 11Q' % (FDFS_GROUP_NAME_MAX_LEN + 1)

    attributes = ("group_name", "total_mb", "free_mb", "trunk_free_mb", "count", "storage_port", "storage_http_port",
                  "active_count", "current_write_server", "store_path_count", "subdir_count_per_path",
                  "current_trunk_file_id",)
    str_attrs = ("group_name",)
    total_mb = SpaceAttr(FDFS_SPACE_SIZE_BASE_INDEX)
    free_mb = SpaceAttr(FDFS_SPACE_SIZE_BASE_INDEX)
    trunk_free_mb = SpaceAttr(FDFS_SPACE_SIZE_BASE_INDEX)