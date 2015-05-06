# coding=utf-8
from __future__ import absolute_import, with_statement

__author__ = 'mazesoul'

IP_ADDRESS_SIZE = 16

FDFS_DOMAIN_NAME_MAX_SIZE = 128

FDFS_ONE_MB = (1024 * 1024)

FDFS_GROUP_NAME_MAX_LEN = 16
FDFS_MAX_SERVERS_EACH_GROUP = 32
FDFS_MAX_GROUPS = 512
FDFS_MAX_TRACKERS = 16

FDFS_MAX_META_NAME_LEN = 64
FDFS_MAX_META_VALUE_LEN = 256

FDFS_FILE_PREFIX_MAX_LEN = 16
FDFS_LOGIC_FILE_PATH_LEN = 10
FDFS_TRUE_FILE_PATH_LEN = 6
FDFS_FILENAME_BASE64_LENGTH = 27
FDFS_TRUNK_FILE_INFO_LEN = 16
FDFS_FILE_EXT_NAME_MAX_LEN = 6
FDFS_SPACE_SIZE_BASE_INDEX = 2  # storage space size based (MB)

FDFS_MAX_SERVER_ID = ((1 << 24) - 1)

FDFS_ID_TYPE_SERVER_ID = 1
FDFS_ID_TYPE_IP_ADDRESS = 2

FDFS_NORMAL_LOGIC_FILENAME_LENGTH = (FDFS_LOGIC_FILE_PATH_LEN + FDFS_FILENAME_BASE64_LENGTH +
                                     FDFS_FILE_EXT_NAME_MAX_LEN + 1)

FDFS_TRUNK_FILENAME_LENGTH = (FDFS_TRUE_FILE_PATH_LEN + FDFS_FILENAME_BASE64_LENGTH +
                              FDFS_TRUNK_FILE_INFO_LEN + 1 + FDFS_FILE_EXT_NAME_MAX_LEN)
FDFS_TRUNK_LOGIC_FILENAME_LENGTH = (FDFS_TRUNK_FILENAME_LENGTH + (FDFS_LOGIC_FILE_PATH_LEN - FDFS_TRUE_FILE_PATH_LEN))

FDFS_VERSION_SIZE = 6

# status order is important!
FDFS_STORAGE_STATUS_INIT = 0
FDFS_STORAGE_STATUS_WAIT_SYNC = 1
FDFS_STORAGE_STATUS_SYNCING = 2
FDFS_STORAGE_STATUS_IP_CHANGED = 3
FDFS_STORAGE_STATUS_DELETED = 4
FDFS_STORAGE_STATUS_OFFLINE = 5
FDFS_STORAGE_STATUS_ONLINE = 6
FDFS_STORAGE_STATUS_ACTIVE = 7
FDFS_STORAGE_STATUS_RECOVERY = 9
FDFS_STORAGE_STATUS_NONE = 99

STORAGE_STATUS_MAP = {
    FDFS_STORAGE_STATUS_INIT: 'INIT',
    FDFS_STORAGE_STATUS_WAIT_SYNC: 'WAIT_SYNC',
    FDFS_STORAGE_STATUS_SYNCING: 'SYNCING',
    FDFS_STORAGE_STATUS_IP_CHANGED: 'IP_CHANGED',
    FDFS_STORAGE_STATUS_DELETED: 'DELETED',
    FDFS_STORAGE_STATUS_OFFLINE: 'OFFLINE',
    FDFS_STORAGE_STATUS_ONLINE: 'ONLINE',
    FDFS_STORAGE_STATUS_ACTIVE: 'ACTIVE',
    FDFS_STORAGE_STATUS_RECOVERY: 'RECOVERY'
}


# which group to upload file
FDFS_STORE_LOOKUP_ROUND_ROBIN = 0  # round robin
FDFS_STORE_LOOKUP_SPEC_GROUP = 1  # specify group
FDFS_STORE_LOOKUP_LOAD_BALANCE = 2  # load balance

# which server to upload file
FDFS_STORE_SERVER_ROUND_ROBIN = 0  # round robin
FDFS_STORE_SERVER_FIRST_BY_IP = 1  # the first server order by ip
FDFS_STORE_SERVER_FIRST_BY_PRI = 2  # the first server order by priority

# which server to download file
FDFS_DOWNLOAD_SERVER_ROUND_ROBIN = 0  # round robin
FDFS_DOWNLOAD_SERVER_SOURCE_FIRST = 1  # the source server

# which path to upload file
FDFS_STORE_PATH_ROUND_ROBIN = 0  # round robin
FDFS_STORE_PATH_LOAD_BALANCE = 2  # load balance

# the mode of the files distributed to the data path
FDFS_FILE_DIST_PATH_ROUND_ROBIN = 0  # round robin
FDFS_FILE_DIST_PATH_RANDOM = 1  # random

# http check alive type
FDFS_HTTP_CHECK_ALIVE_TYPE_TCP = 0  # tcp
FDFS_HTTP_CHECK_ALIVE_TYPE_HTTP = 1  # http

FDFS_DOWNLOAD_TYPE_TCP = 0  # tcp
FDFS_DOWNLOAD_TYPE_HTTP = 1  # http

FDFS_FILE_DIST_DEFAULT_ROTATE_COUNT = 100

FDFS_DOMAIN_NAME_MAX_SIZE = 128

FDFS_STORAGE_STORE_PATH_PREFIX_CHAR = 'M'
FDFS_STORAGE_DATA_DIR_FORMAT = "%02X"
FDFS_STORAGE_META_FILE_EXT = "-m"

FDFS_CHANGE_FLAG_TRACKER_LEADER = 1  # tracker leader changed
FDFS_CHANGE_FLAG_TRUNK_SERVER = 2  # trunk server changed
FDFS_CHANGE_FLAG_GROUP_SERVER = 4  # group server changed

FDFS_STORAGE_ID_MAX_SIZE = 16

TRACKER_STORAGE_RESERVED_SPACE_FLAG_MB = 0
TRACKER_STORAGE_RESERVED_SPACE_FLAG_RATIO = 1

TRACKER_PROTO_CMD_STORAGE_JOIN = 81
FDFS_PROTO_CMD_QUIT = 82
TRACKER_PROTO_CMD_STORAGE_BEAT = 83  # storage heart beat
TRACKER_PROTO_CMD_STORAGE_REPORT_DISK_USAGE = 84  # report disk usage
TRACKER_PROTO_CMD_STORAGE_REPLICA_CHG = 85  # repl new storage servers
TRACKER_PROTO_CMD_STORAGE_SYNC_SRC_REQ = 86  # src storage require sync
TRACKER_PROTO_CMD_STORAGE_SYNC_DEST_REQ = 87  # dest storage require sync
TRACKER_PROTO_CMD_STORAGE_SYNC_NOTIFY = 88  # sync done notify
TRACKER_PROTO_CMD_STORAGE_SYNC_REPORT = 89  # report src last synced time as dest server
TRACKER_PROTO_CMD_STORAGE_SYNC_DEST_QUERY = 79  # dest storage query sync src storage server
TRACKER_PROTO_CMD_STORAGE_REPORT_IP_CHANGED = 78  # storage server report it's ip changed
TRACKER_PROTO_CMD_STORAGE_CHANGELOG_REQ = 77  # storage server request storage server's changelog
TRACKER_PROTO_CMD_STORAGE_REPORT_STATUS = 76  # report specified storage server status
TRACKER_PROTO_CMD_STORAGE_PARAMETER_REQ = 75  # storage server request parameters
TRACKER_PROTO_CMD_STORAGE_REPORT_TRUNK_FREE = 74  # storage report trunk free space
TRACKER_PROTO_CMD_STORAGE_REPORT_TRUNK_FID = 73  # storage report current trunk file id
TRACKER_PROTO_CMD_STORAGE_FETCH_TRUNK_FID = 72  # storage get current trunk file id
TRACKER_PROTO_CMD_STORAGE_GET_STATUS = 71  # get storage status from tracker
TRACKER_PROTO_CMD_STORAGE_GET_SERVER_ID = 70  # get storage server id from tracker
TRACKER_PROTO_CMD_STORAGE_FETCH_STORAGE_IDS = 69  # get all storage ids from tracker
TRACKER_PROTO_CMD_STORAGE_GET_GROUP_NAME = 109  # get storage group name from tracker

TRACKER_PROTO_CMD_TRACKER_GET_SYS_FILES_START = 61  # start of tracker get system data files
TRACKER_PROTO_CMD_TRACKER_GET_SYS_FILES_END = 62  # end of tracker get system data files
TRACKER_PROTO_CMD_TRACKER_GET_ONE_SYS_FILE = 63  # tracker get a system data file
TRACKER_PROTO_CMD_TRACKER_GET_STATUS = 64  # tracker get status of other tracker
TRACKER_PROTO_CMD_TRACKER_PING_LEADER = 65  # tracker ping leader
TRACKER_PROTO_CMD_TRACKER_NOTIFY_NEXT_LEADER = 66  # notify next leader to other trackers
TRACKER_PROTO_CMD_TRACKER_COMMIT_NEXT_LEADER = 67  # commit next leader to other trackers
TRACKER_PROTO_CMD_TRACKER_NOTIFY_RESELECT_LEADER = 68  # storage notify reselect leader when split-brain

TRACKER_PROTO_CMD_SERVER_LIST_ONE_GROUP = 90
TRACKER_PROTO_CMD_SERVER_LIST_ALL_GROUPS = 91
TRACKER_PROTO_CMD_SERVER_LIST_STORAGE = 92
TRACKER_PROTO_CMD_SERVER_DELETE_STORAGE = 93
TRACKER_PROTO_CMD_SERVER_SET_TRUNK_SERVER = 94
TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE = 101
TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE = 102
TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE = 103
TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE = 104
TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ALL = 105
TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ALL = 106
TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ALL = 107
TRACKER_PROTO_CMD_SERVER_DELETE_GROUP = 108

TRACKER_PROTO_CMD_RESP = 100
FDFS_PROTO_CMD_ACTIVE_TEST = 111  # active test, tracker and storage both support since V1.28

STORAGE_PROTO_CMD_REPORT_SERVER_ID = 9
STORAGE_PROTO_CMD_UPLOAD_FILE = 11
STORAGE_PROTO_CMD_DELETE_FILE = 12
STORAGE_PROTO_CMD_SET_METADATA = 13
STORAGE_PROTO_CMD_DOWNLOAD_FILE = 14
STORAGE_PROTO_CMD_GET_METADATA = 15
STORAGE_PROTO_CMD_SYNC_CREATE_FILE = 16
STORAGE_PROTO_CMD_SYNC_DELETE_FILE = 17
STORAGE_PROTO_CMD_SYNC_UPDATE_FILE = 18
STORAGE_PROTO_CMD_SYNC_CREATE_LINK = 19
STORAGE_PROTO_CMD_CREATE_LINK = 20
STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE = 21
STORAGE_PROTO_CMD_QUERY_FILE_INFO = 22
STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE = 23  # create appender file
STORAGE_PROTO_CMD_APPEND_FILE = 24  # append file
STORAGE_PROTO_CMD_SYNC_APPEND_FILE = 25
STORAGE_PROTO_CMD_FETCH_ONE_PATH_BINLOG = 26  # fetch binlog of one store path
STORAGE_PROTO_CMD_RESP = TRACKER_PROTO_CMD_RESP
STORAGE_PROTO_CMD_UPLOAD_MASTER_FILE = STORAGE_PROTO_CMD_UPLOAD_FILE

STORAGE_PROTO_CMD_TRUNK_ALLOC_SPACE = 27  # since V3.00, storage to trunk server
STORAGE_PROTO_CMD_TRUNK_ALLOC_CONFIRM = 28  # since V3.00, storage to trunk server
STORAGE_PROTO_CMD_TRUNK_FREE_SPACE = 29  # since V3.00, storage to trunk server
STORAGE_PROTO_CMD_TRUNK_SYNC_BINLOG = 30  # since V3.00, trunk storage to storage
STORAGE_PROTO_CMD_TRUNK_GET_BINLOG_SIZE = 31  # since V3.07, tracker to storage
STORAGE_PROTO_CMD_TRUNK_DELETE_BINLOG_MARKS = 32  # since V3.07, tracker to storage
STORAGE_PROTO_CMD_TRUNK_TRUNCATE_BINLOG_FILE = 33  # since V3.07, trunk storage to storage

STORAGE_PROTO_CMD_MODIFY_FILE = 34  # since V3.08
STORAGE_PROTO_CMD_SYNC_MODIFY_FILE = 35  # since V3.08
STORAGE_PROTO_CMD_TRUNCATE_FILE = 36  # since V3.08
STORAGE_PROTO_CMD_SYNC_TRUNCATE_FILE = 37  # since V3.08

# for overwrite all old metadata
STORAGE_SET_METADATA_FLAG_OVERWRITE = 'O'
STORAGE_SET_METADATA_FLAG_OVERWRITE_STR = "O"
# for replace, insert when the meta item not exist, otherwise update it
STORAGE_SET_METADATA_FLAG_MERGE = 'M'
STORAGE_SET_METADATA_FLAG_MERGE_STR = "M"

FDFS_PROTO_PKG_LEN_SIZE = 8
FDFS_PROTO_CMD_SIZE = 1
FDFS_PROTO_IP_PORT_SIZE = (IP_ADDRESS_SIZE + 6)

TRACKER_QUERY_STORAGE_FETCH_BODY_LEN = (FDFS_GROUP_NAME_MAX_LEN + IP_ADDRESS_SIZE - 1 + FDFS_PROTO_PKG_LEN_SIZE)
TRACKER_QUERY_STORAGE_STORE_BODY_LEN = (FDFS_GROUP_NAME_MAX_LEN + IP_ADDRESS_SIZE - 1 + FDFS_PROTO_PKG_LEN_SIZE + 1)
