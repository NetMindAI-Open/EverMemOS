"""Simple Memory Manager - 简化的记忆管理器

封装复杂的初始化和转换逻辑，提供简单易用的 API。
"""

from typing import List, Dict, Any, Optional
from pathlib import Path

from agentic_layer.memory_manager import MemoryManager
from memory_layer.memory_manager import MemorizeRequest
from memory_layer.types import RawDataType
from memory_layer.memcell_extractor.base_memcell_extractor import RawData
from common_utils.datetime_utils import get_now_with_timezone
from demo.memory_config import MongoDBConfig
from demo.memory_utils import ensure_mongo_beanie_ready


class SimpleMemoryManager:
    """简化的记忆管理器
    
    提供简单的 API：
    - add_memory(): 添加对话记忆
    - search_memory(): 搜索记忆
    """
    
    def __init__(self):
        """初始化管理器"""
        self.manager: Optional[MemoryManager] = None
        self._initialized = False
        self._message_counter = 0
    
    async def initialize(self) -> None:
        """初始化 MongoDB 和 MemoryManager"""
        if self._initialized:
            return
        
        # 初始化 MongoDB
        mongo_config = MongoDBConfig()
        await ensure_mongo_beanie_ready(mongo_config)
        
        # 创建 MemoryManager
        self.manager = MemoryManager()
        self._initialized = True
    
    async def add_memory(
        self,
        messages: List[Dict[str, str]],
        group_id: str = "default",
        group_name: str = "Default Group",
        user_id: str = "default_user",
    ) -> Dict[str, Any]:
        """添加对话记忆
        
        Args:
            messages: 消息列表，格式：
                [
                    {"role": "user", "content": "消息内容"},
                    {"role": "assistant", "content": "回复内容"},
                ]
            group_id: 群组 ID（可选）
            group_name: 群组名称（可选）
            user_id: 用户 ID（可选）
        
        Returns:
            结果字典，包含 success 和 memories 字段
        """
        # 自动初始化
        if not self._initialized:
            await self.initialize()
        
        # 转换消息格式
        raw_data_list = []
        for msg in messages:
            # 构建消息内容
            content = {
                "speaker_id": user_id if msg["role"] == "user" else "assistant",
                "speaker_name": msg["role"],
                "content": msg["content"],
                "timestamp": get_now_with_timezone(),
            }
            
            # 创建 RawData
            raw_data = RawData(
                content=content,
                data_id=f"msg_{self._message_counter}",
                data_type=RawDataType.CONVERSATION,
            )
            raw_data_list.append(raw_data)
            self._message_counter += 1
        
        # 构建请求（第一条作为历史，其余作为新消息）
        history_data = raw_data_list[:1] if len(raw_data_list) > 1 else []
        new_data = raw_data_list[1:] if len(raw_data_list) > 1 else raw_data_list
        
        request = MemorizeRequest(
            history_raw_data_list=history_data,
            new_raw_data_list=new_data,
            raw_data_type=RawDataType.CONVERSATION,
            user_id_list=[user_id],
            group_id=group_id,
            group_name=group_name,
            enable_semantic_extraction=True,
            enable_event_log_extraction=True,
        )
        
        # 存储记忆
        try:
            result = await self.manager.memorize(request)
            
            # 返回结果和提示
            if result and len(result) > 0:
                print(f"💾 已提取 {len(result)} 条记忆")
            else:
                print("📝 消息已记录（需要更多上下文才能提取记忆）")
            
            return {
                "success": True,
                "memories": result if result else [],
                "count": len(result) if result else 0,
            }
        except Exception as e:
            print(f"❌ 存储失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "memories": [],
                "count": 0,
            }
    
    async def search_memory(
        self,
        query: str,
        group_id: str = "default",
        user_id: str = "default_user",
        top_k: int = 5,
        mode: str = "rrf",
    ) -> List[str]:
        """搜索记忆
        
        Args:
            query: 查询文本
            group_id: 群组 ID（可选）
            user_id: 用户 ID（可选）
            top_k: 返回结果数量（默认 5）
            mode: 检索模式（默认 "rrf"）
                - "rrf": RRF 混合检索（推荐）
                - "embedding": 纯向量检索
                - "bm25": 纯关键词检索
        
        Returns:
            记忆内容列表 ["记忆1", "记忆2", ...]
        """
        # 自动初始化
        if not self._initialized:
            await self.initialize()
        
        try:
            # 调用检索 API
            result = await self.manager.retrieve_lightweight(
                query=query,
                user_id=user_id,
                group_id=group_id,
                time_range_days=365,
                top_k=top_k,
                retrieval_mode=mode,
                data_source="memcell",
            )
            
            # 提取记忆内容
            memories = result.get("memories", [])
            return [m.get("content", "") for m in memories]
        
        except Exception as e:
            print(f"搜索失败: {e}")
            return []
    
    async def check_memory_count(self, group_id: str = "default") -> int:
        """检查指定群组的记忆数量（用于调试）
        
        Args:
            group_id: 群组 ID
            
        Returns:
            记忆数量
        """
        # 自动初始化
        if not self._initialized:
            await self.initialize()
        
        try:
            from infra_layer.adapters.out.persistence.document.memory.memcell import MemCell as DocMemCell
            
            # 查询 MongoDB 中的 MemCell 数量
            count = await DocMemCell.find({"group_id": group_id}).count()
            return count
        except Exception as e:
            print(f"检查失败: {e}")
            return 0


