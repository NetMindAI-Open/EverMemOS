from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Sequence, TYPE_CHECKING
from memory_layer.memory_manager import MemorizeRequest

if TYPE_CHECKING:
    from api_specs.dtos.memory_query import FetchMemRequest
    from api_specs.dtos.memory_query import RetrieveMemRequest

"""
本文件中的各类RawData结构是需要与Memorize方法的输入结构一致
"""


class Mode(str, Enum):
    WORK = "work"
    COMPANION = "companion"


class RetrieveMethod(str, Enum):
    """检索方法枚举"""

    KEYWORD = "keyword"
    VECTOR = "vector"
    HYBRID = "hybrid"


class RequestEntrypointType(str, Enum):
    REST = "rest"
    MQ = "mq"


class RequestType(str, Enum):
    """
    后续可扩展其他策略
    """

    MEMORIZE = "memorize"  # 调用 memorize 进行记忆提取/存储
    FETCH_MEM = "fetch_mem"
    RETRIEVE_MEM = "retrieve_mem"  # 使用KV进行动态记忆检索
    RETRIEVE_MEM_VECTOR = "retrieve_mem_vector"  # 使用向量进行记忆检索
    FETCH_AND_RETRIEVE = "fetch_and_retrieve"  # 同时进行 fetch 和 retrieve 操作
    RETRIEVE_DYNAMIC_MEM_KEYWORD = (
        "retrieve_dynamic_mem_keyword"  # 使用 BM25 进行动态记忆检索
    )
    RETRIEVE_DYNAMIC_MEM_VECTOR = (
        "retrieve_dynamic_mem_vector"  # 使用向量进行动态记忆检索
    )
    RETRIEVE_STATIC_MEM_KEYWORD = (
        "retrieve_static_mem_keyword"  # 使用 BM25 进行静态记忆检索
    )
    RETRIEVE_STATIC_MEM_VECTOR = (
        "retrieve_static_mem_vector"  # 使用向量进行静态记忆检索
    )
    RETRIEVE_DYNAMIC_MEM_MIX = (
        "retrieve_dynamic_mem_mix"  # 使用混合检索策略进行动态记忆检索
    )
    RETRIEVE_STATIC_MEM_MIX = (
        "retrieve_static_mem_mix"  # 使用混合检索策略进行静态记忆检索
    )

    # 添加一种动静态混合？


class AppType(str, Enum):
    SMART_REPLY = "smart_reply"
    SMART_VOTE = "smart_vote"
    FOLLOW_UP = "follow_up"
    OUTPUTS = "outputs"
    OUTLOOK = "outlook"
    UNKNOWN = "unknown"


# --------- Topic 后缀常量 ---------
class TopicSuffix:
    """Kafka Topic 后缀常量"""

    EMAIL = "_email"
    MEMO = "_memo"
    NOTION = "_notion"
    GOOGLE_DRIVE = "_google_drive"
    DROPBOX = "_dropbox"


class MessageType:
    """Kafka 消息 类型的类型"""

    CONVERSATION = "conversation"

    EMAIL = "email"
    MEMO = "memo"
    NOTION = "notion"
    GOOGLE_DRIVE = "google_drive"
    DROPBOX = "dropbox"

    UNKNOWN = "unknown"


# --------- 数据字段常量 ---------
class DataFields:
    """数据字段常量"""

    # 参与者相关字段
    PARTICIPANTS = "participants"
    USER_ID_LIST = "user_id_list"
    CREATE_BY = "createBy"
    SHAREIDS = "shareids"

    # 邮件相关字段
    SENDER_ADDRESS = "sender_address"
    RECEIVER = "receiver"
    CC = "cc"
    BCC = "bcc"

    # 邮箱地址字段
    EMAIL = "email"
    ADDRESS = "address"
    MAIL_ADDRESS = "mail_address"

    # 消息相关字段
    MSG_TYPE = "msgType"
    MESSAGES = "messages"
    RAW_DATA_TYPE = "raw_data_type"

    # 群组相关字段
    GROUP_ID = "group_id"
    ROOM_ID = "roomId"
    THREAD_ID = "thread_id"

    # 接收者相关字段
    RECEIVER_ID = "receiverId"
    USER_ID_LIST = "userIdList"


# --------- 参与者提取配置 ---------
class ParticipantConfig:
    """参与者提取配置"""

    # 邮件参与者字段
    EMAIL_FIELDS = ["sender_address", "receiver", "cc", "bcc"]

    # 文档参与者字段
    DOCUMENT_FIELDS = ["participants", "user_id_list", "createBy"]

    # Memo 参与者字段
    MEMO_FIELDS = ["shareids", "createBy"]

    # 消息参与者字段
    MESSAGE_FIELDS = ["createBy", "receiverId", "userIdList"]


# --------- 消息类型配置 ---------
class MsgTypeConfig:
    """消息类型配置"""

    # 支持的最大数字消息类型
    MAX_SUPPORTED_MSG_TYPE = 6


@dataclass
class MemoryCell:
    key: int
    value: Any
    metadata: Dict[str, Any] = field(default_factory=dict)


# --------- Raw data structures ---------


@dataclass
class ChatRawData:
    room_id: str
    message_id: str
    user_name: str
    content: str
    timestamp: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EmailRawData:
    message_id: str
    sender: str
    recipients: List[str]
    subject: str
    content: str
    timestamp: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MemoRawData:
    memo_id: str
    title: str
    content: str
    timestamp: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LincDocRawData:
    doc_id: str
    title: str
    content: str
    url: Optional[str]
    timestamp: float
    metadata: Dict[str, Any] = field(default_factory=dict)


# TODO: 占位，后续统一替换为fastapi的request
# Unified Request type distinguished by 'intent'
@dataclass
class Request:
    mode: Mode = Mode.WORK  # 目前就一个

    # Optional fine-grained request type. If None, fallback strategy applies
    request_entrypoint_type: Optional[RequestEntrypointType] = (
        RequestEntrypointType.REST
    )

    request_type: Optional[RequestType] = None
    # Fields for MEMORIZE REQTYPE
    memorize_request: Optional[MemorizeRequest] = None
    # Fields for FETCH_MEM REQTYPE
    fetch_mem_request: Optional['FetchMemRequest'] = None

    # Fields for RETRIEVE REQTYPE
    retrieve_mem_request: Optional['RetrieveMemRequest'] = None
    override_keys: Optional[Sequence[str]] = None  # 废弃

    source: AppType = (
        AppType.UNKNOWN
    )  # retrieve和fetch会用到，可能不同来源对应不同的场景

    def __post_init__(self) -> None:
        import logging

        logger = logging.getLogger(__name__)

        if self.request_type == RequestType.MEMORIZE:
            if self.memorize_request is None:
                raise ValueError(
                    "Request.memorize_request is required when request_type=MEMORIZE"
                )
        elif self.request_type == RequestType.FETCH_MEM:
            if self.fetch_mem_request is None:
                raise ValueError(
                    "Request.fetch_mem_request is required when request_type=FETCH_MEM"
                )
        elif self.request_type == RequestType.FETCH_AND_RETRIEVE:
            if self.fetch_mem_request is None:
                raise ValueError(
                    "Request.fetch_mem_request is required when request_type=FETCH_AND_RETRIEVE"
                )
            if self.retrieve_mem_request is None:
                raise ValueError(
                    "Request.retrieve_mem_request is required when request_type=FETCH_AND_RETRIEVE"
                )
