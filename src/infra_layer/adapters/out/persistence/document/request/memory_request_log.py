# -*- coding: utf-8 -*-
"""
MemoryRequestLog MongoDB Document Model

存储来自 memories 请求的关键信息，用于替代 conversation_data 的功能。
主要保存 memorize 请求中的消息内容，后续可用于替换 Redis 中的 RawData 存储。
"""

from datetime import datetime
from typing import Optional, Dict, Any, List
from core.oxm.mongo.document_base import DocumentBase
from core.oxm.mongo.audit_base import AuditBase
from pydantic import Field, ConfigDict
from pymongo import IndexModel, ASCENDING, DESCENDING


class MemoryRequestLog(DocumentBase, AuditBase):
    """
    Memory 请求日志文档模型

    存储来自 memories 接口请求的关键信息：
    - group_id: 会话组 ID
    - request_id: 请求 ID
    - user_id: 用户 ID
    - raw_input: 原始输入数据
    - 消息核心字段: message_id, create_time, sender, sender_name, content 等
    """

    # 核心字段
    group_id: str = Field(..., description="会话组 ID")
    request_id: str = Field(..., description="请求 ID")
    user_id: Optional[str] = Field(default=None, description="用户 ID")

    # ========== 消息核心字段（用于替代 RawData）==========
    # 参考 group_chat_converter.py 中的字段定义
    message_id: Optional[str] = Field(default=None, description="消息 ID")
    message_create_time: Optional[str] = Field(
        default=None, description="消息创建时间（ISO 8601 格式）"
    )
    sender: Optional[str] = Field(default=None, description="发送者 ID")
    sender_name: Optional[str] = Field(default=None, description="发送者名称")
    content: Optional[str] = Field(default=None, description="消息内容")
    group_name: Optional[str] = Field(default=None, description="群组名称")
    refer_list: Optional[List[str]] = Field(
        default=None, description="引用消息 ID 列表"
    )

    # 原始输入（保留用于调试和完整性）
    raw_input: Optional[Dict[str, Any]] = Field(
        default=None, description="原始输入数据（解析后的 JSON body）"
    )
    raw_input_str: Optional[str] = Field(default=None, description="原始输入字符串")

    # 请求元信息
    version: Optional[str] = Field(default=None, description="代码版本")
    endpoint_name: Optional[str] = Field(default=None, description="端点名称")
    method: Optional[str] = Field(default=None, description="HTTP 方法")
    url: Optional[str] = Field(default=None, description="请求 URL")

    # 租户信息
    organization_id: Optional[str] = Field(default=None, description="组织 ID")
    space_id: Optional[str] = Field(default=None, description="空间 ID")

    # 原始事件 ID（用于关联 RequestHistory）
    event_id: Optional[str] = Field(default=None, description="原始事件 ID")

    # 同步状态字段（数值型）
    # -1: 只是 log 记录（刚通过 listener 保存的原始请求）
    #  0: 窗口累积中（通过 save_conversation_data 确认进入累积窗口）
    #  1: 已全部使用过（通过 delete_conversation_data 标记，边界检测后）
    sync_status: int = Field(
        default=-1, description="同步状态: -1=log记录, 0=窗口累积, 1=已使用"
    )

    model_config = ConfigDict(
        collection="memory_request_logs",
        validate_assignment=True,
        json_encoders={datetime: lambda dt: dt.isoformat()},
        json_schema_extra={
            "example": {
                "group_id": "group_123",
                "request_id": "req_456",
                "user_id": "user_789",
                "message_id": "msg_001",
                "message_create_time": "2024-01-01T12:00:00+08:00",
                "sender": "user_789",
                "sender_name": "张三",
                "content": "这是一条测试消息",
                "group_name": "测试群组",
                "refer_list": [],
                "raw_input": {"message_id": "msg_001", "content": "这是一条测试消息"},
                "version": "1.0.0",
                "endpoint_name": "memorize",
            }
        },
    )

    class Settings:
        """Beanie settings"""

        name = "memory_request_logs"
        indexes = [
            IndexModel([("group_id", ASCENDING), ("created_at", DESCENDING)]),
            IndexModel([("request_id", ASCENDING)]),
            IndexModel([("user_id", ASCENDING)]),
            IndexModel([("created_at", DESCENDING)]),
            IndexModel([("event_id", ASCENDING)]),
            IndexModel([("message_id", ASCENDING)]),
            IndexModel([("group_id", ASCENDING), ("message_create_time", DESCENDING)]),
            # 复合索引：用于批量更新和按状态查询
            # 支持 update_many({"group_id": "xxx", "sync_status": -1}, ...) 等操作
            IndexModel([("group_id", ASCENDING), ("sync_status", ASCENDING)]),
        ]
