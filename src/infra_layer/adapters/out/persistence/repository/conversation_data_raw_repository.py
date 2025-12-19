# -*- coding: utf-8 -*-
"""
ConversationDataRepository interface and implementation

基于 MemoryRequestLog 实现的会话数据存储，替代原有的 Redis 实现。
"""

from abc import ABC, abstractmethod
from typing import List, Optional

from core.observation.logger import get_logger
from core.di.decorators import repository
from core.di import get_bean
from memory_layer.memcell_extractor.base_memcell_extractor import RawData
from biz_layer.mem_db_operations import _normalize_datetime_for_storage
from infra_layer.adapters.out.persistence.repository.memory_request_log_repository import (
    MemoryRequestLogRepository,
)
from infra_layer.adapters.out.persistence.mapper.memory_request_log_mapper import (
    MemoryRequestLogMapper,
)

logger = get_logger(__name__)


# ==================== Interface Definition ====================


class ConversationDataRepository(ABC):
    """Conversation data access interface"""

    @abstractmethod
    async def save_conversation_data(
        self, raw_data_list: List[RawData], group_id: str
    ) -> bool:
        """Save conversation data"""
        pass

    @abstractmethod
    async def get_conversation_data(
        self,
        group_id: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 100,
    ) -> List[RawData]:
        """Get conversation data"""
        pass

    @abstractmethod
    async def delete_conversation_data(self, group_id: str) -> bool:
        """
        Delete all conversation data for the specified group

        Args:
            group_id: Group ID

        Returns:
            bool: Return True if deletion succeeds, False otherwise
        """
        pass


# ==================== Implementation ====================


@repository("conversation_data_repo", primary=True)
class ConversationDataRepositoryImpl(ConversationDataRepository):
    """
    基于 MemoryRequestLog 的 ConversationDataRepository 实现

    复用 MemoryRequestLog 存储会话数据，将 RawData 与 MemoryRequestLog 相互转换。
    数据通过 RequestHistoryEvent 监听器自动保存到 MemoryRequestLog。
    """

    def __init__(self):
        self._repo: Optional[MemoryRequestLogRepository] = None

    def _get_repo(self) -> MemoryRequestLogRepository:
        """懒加载获取 MemoryRequestLogRepository"""
        if self._repo is None:
            self._repo = get_bean("memory_request_log_repository")
        return self._repo

    # ==================== ConversationDataRepository 接口实现 ====================

    async def save_conversation_data(
        self, raw_data_list: List[RawData], group_id: str
    ) -> bool:
        """
        确认会话数据进入窗口累积

        将 sync_status = -1 (log 记录) 更新为 sync_status = 0 (窗口累积中)。
        数据本身已通过 RequestHistoryEvent 监听器自动保存到 MemoryRequestLog，
        此方法用于确认这些数据进入窗口累积状态。

        更新策略：
        - 如果 raw_data_list 中有 data_id（即 message_id），则精确更新这些记录
        - 否则回退到按 group_id 更新所有 sync_status=-1 的记录

        sync_status 状态流转:
        - -1: 只是 log 记录（刚通过 listener 保存的原始请求）
        -  0: 窗口累积中（通过此方法确认进入累积窗口）
        -  1: 已全部使用过（通过 delete_conversation_data 标记）

        Args:
            raw_data_list: RawData 列表，用于提取 message_id 进行精确更新
            group_id: 会话组 ID

        Returns:
            bool: 操作成功返回 True，否则返回 False
        """
        logger.info(
            "确认会话数据进入窗口累积: group_id=%s, data_count=%d",
            group_id,
            len(raw_data_list) if raw_data_list else 0,
        )

        try:
            repo = self._get_repo()

            # 提取 message_id 列表（过滤掉空值）
            message_ids = [r.data_id for r in (raw_data_list or []) if r.data_id]

            if message_ids:
                # 精确更新：只更新指定 message_id 的记录
                modified_count = await repo.confirm_accumulation_by_message_ids(
                    group_id, message_ids
                )
            else:
                # 兜底：按 group_id 更新所有 sync_status=-1 的记录
                logger.debug("raw_data_list 中没有 data_id，回退到按 group_id 更新")
                modified_count = await repo.confirm_accumulation_by_group_id(group_id)

            logger.info(
                "✅ 确认窗口累积完成: group_id=%s, message_ids=%d, modified=%d",
                group_id,
                len(message_ids),
                modified_count,
            )
            return True

        except Exception as e:
            logger.error("❌ 确认窗口累积失败: group_id=%s, error=%s", group_id, e)
            return False

    async def get_conversation_data(
        self,
        group_id: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 100,
    ) -> List[RawData]:
        """
        获取窗口累积中的会话数据

        查询 sync_status = 0 (窗口累积中) 的 MemoryRequestLog 并转换为 RawData。
        只返回已确认进入累积窗口但尚未被使用的数据。

        sync_status 状态说明:
        - -1: 只是 log 记录（不返回）
        -  0: 窗口累积中（返回这些数据）
        -  1: 已全部使用过（不返回）

        Args:
            group_id: 会话组 ID
            start_time: 开始时间（ISO 格式字符串）
            end_time: 结束时间（ISO 格式字符串）
            limit: 返回数量限制

        Returns:
            List[RawData]: 会话数据列表
        """
        logger.info(
            "开始获取会话数据: group_id=%s, start_time=%s, end_time=%s, limit=%d",
            group_id,
            start_time,
            end_time,
            limit,
        )

        try:
            repo = self._get_repo()

            # 转换时间格式
            start_dt = (
                _normalize_datetime_for_storage(start_time) if start_time else None
            )
            end_dt = _normalize_datetime_for_storage(end_time) if end_time else None

            # 查询 MemoryRequestLog
            logs = await repo.find_by_group_id(
                group_id=group_id, start_time=start_dt, end_time=end_dt, limit=limit
            )

            # 使用 mapper 转换为 RawData 列表
            raw_data_list = MemoryRequestLogMapper.to_raw_data_list(logs)

            logger.info(
                "✅ 获取会话数据完成: group_id=%s, count=%d",
                group_id,
                len(raw_data_list),
            )
            return raw_data_list

        except Exception as e:
            logger.error("❌ 获取会话数据失败: group_id=%s, error=%s", group_id, e)
            return []

    async def delete_conversation_data(self, group_id: str) -> bool:
        """
        标记指定会话组的累积数据为已使用

        将 sync_status = 0 (窗口累积中) 更新为 sync_status = 1 (已全部使用)。
        注意：此方法不会真正删除数据，而是更新 sync_status 状态。
        这样可以保留历史数据用于审计和重放，同时不影响后续查询。

        sync_status 状态流转:
        - -1: 只是 log 记录
        -  0: 窗口累积中（通过 save_conversation_data 确认）
        -  1: 已全部使用过（通过此方法标记，边界检测后调用）

        Args:
            group_id: 会话组 ID

        Returns:
            bool: 操作成功返回 True，否则返回 False
        """
        logger.info("开始标记会话数据为已使用: group_id=%s", group_id)

        try:
            repo = self._get_repo()
            # 将 sync_status: 0 -> 1
            modified_count = await repo.mark_as_used_by_group_id(group_id)

            logger.info(
                "✅ 标记会话数据为已使用完成: group_id=%s, modified=%d",
                group_id,
                modified_count,
            )
            return True

        except Exception as e:
            logger.error(
                "❌ 标记会话数据为已使用失败: group_id=%s, error=%s", group_id, e
            )
            return False
