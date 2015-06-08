# coding=utf-8
from __future__ import absolute_import

__author__ = 'mazesoul'

import struct
from datetime import datetime

from pyfdfs.enums import IP_ADDRESS_SIZE, FDFS_STORAGE_ID_MAX_SIZE, FDFS_DOMAIN_NAME_MAX_SIZE, \
    FDFS_VERSION_SIZE, FDFS_SPACE_SIZE_BASE_INDEX, FDFS_GROUP_NAME_MAX_LEN


class BaseAttr(object):
    def __init__(self, name, val=None):
        if name is None:
            raise Exception("name is required! cannot be None")
        self.name = name
        self.val = val

    def __get__(self, obj, owner):
        if obj is None:
            return self
        return obj._data.get(self.name, self.val)

    def __set__(self, obj, val):
        obj._data[self.name] = val

    def __delete__(self, obj):
        del obj._data[self.name]


class IntAttr(BaseAttr):
    def __init__(self, name, val=None):
        val = val or 0
        super(IntAttr, self).__init__(name, val)


class StrAttr(BaseAttr):
    def __init__(self, name, val=None):
        val = val or ""
        super(StrAttr, self).__init__(name, val)

    def __set__(self, obj, val):
        obj._data[self.name] = str(val).strip("\x00")


class DatetimeAttr(BaseAttr):
    def __init__(self, name, val=None):
        val = datetime.fromtimestamp(val or 0).isoformat()
        super(DatetimeAttr, self).__init__(name, val)

    def __set__(self, obj, val):
        obj._data[self.name] = datetime.fromtimestamp(val).isoformat()


class SpaceAttr(StrAttr):
    def __init__(self, name, index=None):
        self.suffix = ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB',)
        self.index = index or 0
        val = "0B"
        super(SpaceAttr, self).__init__(name, val)

    def __set__(self, obj, val):
        multiples = 1024.0
        if val < multiples:
            obj._data[self.name] = '{0:d}{1}'.format(val, self.suffix[self.index])
            return
        for suffix in self.suffix[self.index:]:
            if val < multiples:
                obj._data[self.name] = '{0:.2f}{1}'.format(val, suffix)
                return
            val /= multiples
        obj._data[self.name] = val


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
                    attr_obj = StrAttr(item)
                elif item in date_attrs:
                    attr_obj = DatetimeAttr(item)
                elif item in space_attrs:
                    attr_obj = SpaceAttr(item)
                else:
                    attr_obj = IntAttr(item)
            new_attrs[item] = attr_obj
        for attr_name, attr in attrs.items():
            new_attrs[attr_name] = attr
        return super(BaseMeta, cls).__new__(cls, name, (BaseInfo,) + bases, new_attrs)

    def __call__(cls, *args, **kwargs):
        obj = type.__call__(cls, *args, **kwargs)
        obj._data = {}
        return obj


class StorageInfo(object):
    """
    @ 1 byte: status
    @ FDFS_STORAGE_ID_MAX_SIZE bytes: id
    @ IP_ADDRESS_SIZE bytes: ip_addr
    @ FDFS_DOMAIN_NAME_MAX_SIZE bytes: domain_name
    @ FDFS_STORAGE_ID_MAX_SIZE bytes: src_ip
    @ FDFS_VERSION_SIZE bytes: version
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: join_time
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: up_time
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_mb
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: free_mb
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: upload_priority
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: store_path_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: subdir_count_per_path
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: current_write_path
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: storage_port
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: storage_http_port
    @ 4 bytes: alloc_count
    @ 4 bytes: current_count
    @ 4 bytes: max_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_upload_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: success_upload_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_append_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: success_append_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_modify_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: success_modify_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_truncate_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: success_truncate_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_set_meta_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: success_set_meta_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_delete_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: success_delete_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_download_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: success_download_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_get_meta_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: success_get_meta_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_create_link_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: success_create_link_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_delete_link_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: success_delete_link_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_upload_bytes
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: success_upload_bytes
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_append_bytes
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: success_append_bytes
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_modify_bytes
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: success_modify_bytes
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_download_bytes
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: success_download_bytes
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_sync_in_bytes
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: success_sync_in_bytes
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_sync_out_bytes
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: success_sync_out_bytes
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_file_open_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: success_file_open_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_file_read_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: success_file_read_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_file_write_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: success_file_write_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: last_source_update
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: last_sync_update
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: last_synced_timestamp
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: last_heart_beat_time
    @ 1 byte: if_trunk_server
    """
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
    total_mb = SpaceAttr("total_mb", FDFS_SPACE_SIZE_BASE_INDEX)
    free_mb = SpaceAttr("free_mb", FDFS_SPACE_SIZE_BASE_INDEX)


class BasicStorageInfo(object):
    """
    @ FDFS_GROUP_NAME_MAX_LEN bytes: group_name
    @ IP_ADDRESS_SIZE bytes: ip_addr
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: current_write_path
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: storage_port
    """
    __metaclass__ = BaseMeta
    desc = "BasicStorageInfo information"
    fmt = '!%ds %ds Q B' % (FDFS_GROUP_NAME_MAX_LEN, IP_ADDRESS_SIZE)

    attributes = ("group_name", "ip_addr", "current_write_path", "storage_port",)
    str_attrs = ("group_name", "ip_addr",)


class GroupInfo(object):
    """
    @ FDFS_GROUP_NAME_MAX_LEN + 1 bytes: group_name
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: total_mb
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: free_mb
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: trunk_free_mb
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: storage_port
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: storage_http_port
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: active_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: current_write_server
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: store_path_count
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: subdir_count_per_path
    @ TRACKER_PROTO_PKG_LEN_SIZE bytes: current_trunk_file_id
    """
    __metaclass__ = BaseMeta
    desc = "Group information"
    fmt = '!%ds 11Q' % (FDFS_GROUP_NAME_MAX_LEN + 1)

    attributes = ("group_name", "total_mb", "free_mb", "trunk_free_mb", "count", "storage_port", "storage_http_port",
                  "active_count", "current_write_server", "store_path_count", "subdir_count_per_path",
                  "current_trunk_file_id",)
    str_attrs = ("group_name",)
    total_mb = SpaceAttr("total_mb", FDFS_SPACE_SIZE_BASE_INDEX)
    free_mb = SpaceAttr("free_mb", FDFS_SPACE_SIZE_BASE_INDEX)
    trunk_free_mb = SpaceAttr("trunk_free_mb", FDFS_SPACE_SIZE_BASE_INDEX)
