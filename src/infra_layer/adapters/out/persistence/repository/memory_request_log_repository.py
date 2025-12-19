# -*- coding: utf-8 -*-
"""
MemoryRequestLog Repository

Memory 请求日志数据访问层，提供 memories 请求记录的 CRUD 操作。
用于替代 conversation_data 的功能。
"""

from datetime import datetime
from typing import List, Optional
from pymongo.asynchronous.client_session import AsyncClientSession
from core.observation.logger import get_logger
from core.di.decorators import repository
from core.oxm.mongo.base_repository import BaseRepository
from infra_layer.adapters.out.persistence.document.request.memory_request_log import (
    MemoryRequestLog,
)

logger = get_logger(__name__)


@repository("memory_request_log_repository", primary=True)
class MemoryRequestLogRepository(BaseRepository[MemoryRequestLog]):
    """
    Memory 请求日志 Repository

    提供 memories 接口请求记录的 CRUD 操作和查询功能。
    可作为 conversation_data 的替代实现。
    """

    def __init__(self):
        super().__init__(MemoryRequestLog)

    # ==================== 保存方法 ====================

    async def save(
        self,
        memory_request_log: MemoryRequestLog,
        session: Optional[AsyncClientSession] = None,
    ) -> Optional[MemoryRequestLog]:
        """
        保存 Memory 请求日志

        Args:
            memory_request_log: MemoryRequestLog 对象
            session: 可选的 MongoDB session

        Returns:
            保存后的 MemoryRequestLog 或 None
        """
        try:
            await memory_request_log.insert(session=session)
            logger.debug(
                "✅ 保存 Memory 请求日志成功: id=%s, group_id=%s, request_id=%s",
                memory_request_log.id,
                memory_request_log.group_id,
                memory_request_log.request_id,
            )
            return memory_request_log
        except Exception as e:
            logger.error("❌ 保存 Memory 请求日志失败: %s", e)
            return None

    # ==================== 查询方法 ====================

    async def get_by_request_id(
        self, request_id: str, session: Optional[AsyncClientSession] = None
    ) -> Optional[MemoryRequestLog]:
        """
        根据请求 ID 获取 Memory 请求日志

        Args:
            request_id: 请求 ID
            session: 可选的 MongoDB session

        Returns:
            MemoryRequestLog 或 None
        """
        try:
            result = await MemoryRequestLog.find_one(
                {"request_id": request_id}, session=session
            )
            return result
        except Exception as e:
            logger.error("❌ 根据请求 ID 获取 Memory 请求日志失败: %s", e)
            return None

    async def find_by_group_id(
        self,
        group_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100,
        sync_status: Optional[int] = 0,
        session: Optional[AsyncClientSession] = None,
    ) -> List[MemoryRequestLog]:
        """
        根据 group_id 查询 Memory 请求日志

        Args:
            group_id: 会话组 ID
            start_time: 开始时间
            end_time: 结束时间
            limit: 返回数量限制
            sync_status: 同步状态过滤（默认 0=窗口累积中，None=不过滤）
                - -1: 只是 log 记录
                -  0: 窗口累积中
                -  1: 已全部使用
                - None: 不过滤，返回所有状态
            session: 可选的 MongoDB session

        Returns:
            MemoryRequestLog 列表
        """
        try:
            query = {"group_id": group_id}

            # 按状态过滤
            if sync_status is not None:
                query["sync_status"] = sync_status

            if start_time:
                query["created_at"] = {"$gte": start_time}
            if end_time:
                if "created_at" in query:
                    query["created_at"]["$lte"] = end_time
                else:
                    query["created_at"] = {"$lte": end_time}

            results = (
                await MemoryRequestLog.find(query, session=session)
                .sort([("created_at", 1)])  # 按时间升序，早的在前
                .limit(limit)
                .to_list()
            )
            logger.debug(
                "✅ 根据 group_id 查询 Memory 请求日志: group_id=%s, sync_status=%s, count=%d",
                group_id,
                sync_status,
                len(results),
            )
            return results
        except Exception as e:
            logger.error("❌ 根据 group_id 查询 Memory 请求日志失败: %s", e)
            return []

    async def find_by_user_id(
        self,
        user_id: str,
        limit: int = 100,
        session: Optional[AsyncClientSession] = None,
    ) -> List[MemoryRequestLog]:
        """
        根据用户 ID 查询 Memory 请求日志

        Args:
            user_id: 用户 ID
            limit: 返回数量限制
            session: 可选的 MongoDB session

        Returns:
            MemoryRequestLog 列表
        """
        try:
            results = (
                await MemoryRequestLog.find({"user_id": user_id}, session=session)
                .sort([("created_at", -1)])
                .limit(limit)
                .to_list()
            )
            logger.debug(
                "✅ 根据用户 ID 查询 Memory 请求日志: user_id=%s, count=%d",
                user_id,
                len(results),
            )
            return results
        except Exception as e:
            logger.error("❌ 根据用户 ID 查询 Memory 请求日志失败: %s", e)
            return []

    async def delete_by_group_id(
        self, group_id: str, session: Optional[AsyncClientSession] = None
    ) -> int:
        """
        根据 group_id 删除 Memory 请求日志

        Args:
            group_id: 会话组 ID
            session: 可选的 MongoDB session

        Returns:
            删除的记录数
        """
        try:
            result = await MemoryRequestLog.find(
                {"group_id": group_id}, session=session
            ).delete()
            deleted_count = result.deleted_count if result else 0
            logger.info(
                "✅ 删除 Memory 请求日志: group_id=%s, deleted=%d",
                group_id,
                deleted_count,
            )
            return deleted_count
        except Exception as e:
            logger.error(
                "❌ 删除 Memory 请求日志失败: group_id=%s, error=%s", group_id, e
            )
            return 0

    # ==================== 同步状态管理 ====================
    # sync_status 状态流转:
    # -1 (log记录) -> 0 (窗口累积) -> 1 (已使用)
    #
    # - save_conversation_data: -1 -> 0 (确认进入窗口累积)
    # - delete_conversation_data: 0 -> 1 (标记已全部使用)

    async def confirm_accumulation_by_group_id(
        self, group_id: str, session: Optional[AsyncClientSession] = None
    ) -> int:
        """
        将指定 group_id 的 log 记录确认为窗口累积状态

        批量更新 sync_status: -1 -> 0，用于 save_conversation_data。
        使用 (group_id, sync_status) 复合索引，高效查询。

        注意：此方法会更新该 group 下所有 sync_status=-1 的记录，
        如需精确控制，请使用 confirm_accumulation_by_message_ids。

        Args:
            group_id: 会话组 ID
            session: 可选的 MongoDB session

        Returns:
            更新的记录数
        """
        try:
            collection = MemoryRequestLog.get_pymongo_collection()
            result = await collection.update_many(
                {"group_id": group_id, "sync_status": -1},
                {"$set": {"sync_status": 0}},
                session=session,
            )
            modified_count = result.modified_count if result else 0
            logger.info(
                "✅ 确认窗口累积: group_id=%s, modified=%d", group_id, modified_count
            )
            return modified_count
        except Exception as e:
            logger.error("❌ 确认窗口累积失败: group_id=%s, error=%s", group_id, e)
            return 0

    async def confirm_accumulation_by_message_ids(
        self,
        group_id: str,
        message_ids: List[str],
        session: Optional[AsyncClientSession] = None,
    ) -> int:
        """
        将指定 message_id 列表的 log 记录确认为窗口累积状态

        精确更新：只更新指定 message_id 的记录，避免误更新其他并发请求的数据。
        sync_status: -1 -> 0

        Args:
            group_id: 会话组 ID（用于额外校验）
            message_ids: 要更新的 message_id 列表
            session: 可选的 MongoDB session

        Returns:
            更新的记录数
        """
        if not message_ids:
            logger.debug("message_ids 为空，跳过更新")
            return 0

        try:
            collection = MemoryRequestLog.get_pymongo_collection()
            result = await collection.update_many(
                {
                    "group_id": group_id,
                    "message_id": {"$in": message_ids},
                    "sync_status": -1,
                },
                {"$set": {"sync_status": 0}},
                session=session,
            )
            modified_count = result.modified_count if result else 0
            logger.info(
                "✅ 确认窗口累积(精确): group_id=%s, message_ids=%d, modified=%d",
                group_id,
                len(message_ids),
                modified_count,
            )
            return modified_count
        except Exception as e:
            logger.error(
                "❌ 确认窗口累积(精确)失败: group_id=%s, error=%s", group_id, e
            )
            return 0

    async def mark_as_used_by_group_id(
        self, group_id: str, session: Optional[AsyncClientSession] = None
    ) -> int:
        """
        将指定 group_id 的未使用数据标记为已使用

        批量更新 sync_status: -1 或 0 -> 1，用于 delete_conversation_data（边界检测后）。
        - -1: 刚保存的 log 记录（当前请求的消息）
        -  0: 已确认的窗口累积数据（之前累积的消息）
        两者都标记为 1（已使用）。

        使用 (group_id, sync_status) 复合索引，高效查询。

        Args:
            group_id: 会话组 ID
            session: 可选的 MongoDB session

        Returns:
            更新的记录数
        """
        try:
            collection = MemoryRequestLog.get_pymongo_collection()
            result = await collection.update_many(
                {"group_id": group_id, "sync_status": {"$in": [-1, 0]}},
                {"$set": {"sync_status": 1}},
                session=session,
            )
            modified_count = result.modified_count if result else 0
            logger.info(
                "✅ 标记为已使用: group_id=%s, modified=%d", group_id, modified_count
            )
            return modified_count
        except Exception as e:
            logger.error("❌ 标记为已使用失败: group_id=%s, error=%s", group_id, e)
            return 0
