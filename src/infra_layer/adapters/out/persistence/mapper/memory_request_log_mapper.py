# -*- coding: utf-8 -*-
"""
MemoryRequestLog <-> RawData 转换器

负责 MemoryRequestLog 和 RawData 之间的相互转换。
"""

import json
from typing import Optional, List, Dict, Any

from core.observation.logger import get_logger
from common_utils.datetime_utils import from_iso_format
from zoneinfo import ZoneInfo
from api_specs.dtos.memory_command import RawData
from api_specs.request_converter import (
    build_raw_data_from_simple_message,
    normalize_refer_list,
)
from infra_layer.adapters.out.persistence.document.request.memory_request_log import (
    MemoryRequestLog,
)

logger = get_logger(__name__)


class MemoryRequestLogMapper:
    """
    MemoryRequestLog <-> RawData 转换器

    提供 MemoryRequestLog 和 RawData 之间的双向转换功能。
    """

    @staticmethod
    def to_raw_data(log: MemoryRequestLog) -> Optional[RawData]:
        """
        将 MemoryRequestLog 转换为 RawData

        转换策略（按优先级）：
        1. 优先从 raw_input_str 解析简单消息格式
        2. 其次使用 raw_input 字典解析简单消息格式
        3. 最后从独立字段构建 RawData

        Args:
            log: MemoryRequestLog 对象

        Returns:
            RawData 对象或 None（转换失败时）
        """
        if log is None:
            return None

        # 策略 1: 优先从 raw_input_str 解析简单消息格式
        if log.raw_input_str:
            try:
                data = json.loads(log.raw_input_str)
                raw_data = MemoryRequestLogMapper._convert_simple_message_to_raw_data(
                    data, log.request_id
                )
                if raw_data:
                    return raw_data
            except (json.JSONDecodeError, ValueError, TypeError) as e:
                logger.debug("从 raw_input_str 解析失败，尝试其他方式: %s", e)

        # 策略 2: 使用 raw_input 字典解析简单消息格式
        if log.raw_input:
            raw_data = MemoryRequestLogMapper._convert_simple_message_to_raw_data(
                log.raw_input, log.request_id
            )
            if raw_data:
                return raw_data

        # 策略 3: 从独立字段构建
        return MemoryRequestLogMapper._build_from_fields(log)

    @staticmethod
    def _convert_simple_message_to_raw_data(
        message_data: Dict[str, Any], request_id: Optional[str] = None
    ) -> Optional[RawData]:
        """
        将简单消息格式转换为 RawData

        简单消息格式: {"message_id": "...", "sender": "...", "content": "...", ...}

        Args:
            message_data: 简单消息数据字典
            request_id: 请求 ID（可选，用于 metadata）

        Returns:
            RawData 对象或 None
        """
        if not isinstance(message_data, dict):
            return None

        message_id = message_data.get("message_id")
        sender = message_data.get("sender")
        content = message_data.get("content", "")
        create_time_str = message_data.get("create_time")

        if not message_id or not sender:
            return None

        # 解析时间戳
        timestamp = None
        if create_time_str:
            try:
                if isinstance(create_time_str, str):
                    timestamp = from_iso_format(create_time_str, ZoneInfo("UTC"))
                else:
                    timestamp = create_time_str
            except (ValueError, TypeError) as e:
                logger.warning(
                    "解析 create_time 失败: %s, error: %s", create_time_str, e
                )

        # 标准化 refer_list
        refer_list = normalize_refer_list(message_data.get("refer_list", []))

        # 构建 extra_metadata
        extra_metadata = {"request_id": request_id} if request_id else None

        return build_raw_data_from_simple_message(
            message_id=message_id,
            sender=sender,
            content=content,
            timestamp=timestamp,
            sender_name=message_data.get("sender_name"),
            group_id=message_data.get("group_id"),
            group_name=message_data.get("group_name"),
            refer_list=refer_list,
            extra_metadata=extra_metadata,
        )

    @staticmethod
    def _build_from_fields(log: MemoryRequestLog) -> RawData:
        """
        从 MemoryRequestLog 的独立字段构建 RawData

        使用统一的 build_raw_data_from_simple_message 函数确保字段一致性。

        Args:
            log: MemoryRequestLog 对象

        Returns:
            RawData 对象
        """
        # 处理时间戳
        timestamp = None
        if log.message_create_time:
            try:
                # 如果是字符串，解析为 datetime
                if isinstance(log.message_create_time, str):
                    timestamp = from_iso_format(
                        log.message_create_time, ZoneInfo("UTC")
                    )
                else:
                    timestamp = log.message_create_time
            except (ValueError, TypeError) as e:
                logger.warning(
                    "解析 message_create_time 失败: %s, error: %s",
                    log.message_create_time,
                    e,
                )
                timestamp = None

        # 使用统一的构建函数
        return build_raw_data_from_simple_message(
            message_id=log.message_id or str(log.id),
            sender=log.sender or "",
            content=log.content or "",
            timestamp=timestamp,
            sender_name=log.sender_name,
            group_id=log.group_id,
            group_name=log.group_name,
            refer_list=log.refer_list or [],
            extra_metadata={"request_id": log.request_id},
        )

    @staticmethod
    def to_raw_data_list(logs: List[MemoryRequestLog]) -> List[RawData]:
        """
        批量将 MemoryRequestLog 列表转换为 RawData 列表

        Args:
            logs: MemoryRequestLog 对象列表

        Returns:
            RawData 对象列表（跳过转换失败的记录）
        """
        raw_data_list: List[RawData] = []

        for log in logs:
            try:
                raw_data = MemoryRequestLogMapper.to_raw_data(log)
                if raw_data:
                    raw_data_list.append(raw_data)
            except (ValueError, TypeError) as e:
                logger.error(
                    "❌ 转换 MemoryRequestLog 到 RawData 失败: log_id=%s, error=%s",
                    log.id,
                    e,
                )
                continue

        return raw_data_list
