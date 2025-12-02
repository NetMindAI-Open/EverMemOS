"""
ClusterState 原生 CRUD 仓库

基于 Beanie ODM 的聚类状态数据访问层，同时实现 ClusterStorage 接口。
"""

from typing import Optional, Dict, Any
from core.observation.logger import get_logger
from core.di.decorators import repository
from core.oxm.mongo.base_repository import BaseRepository

from memory_layer.cluster_manager.storage import ClusterStorage
from infra_layer.adapters.out.persistence.document.memory.cluster_state import ClusterState

logger = get_logger(__name__)


@repository("cluster_state_raw_repository", primary=True)
class ClusterStateRawRepository(BaseRepository[ClusterState], ClusterStorage):
    """
    ClusterState 原生 CRUD 仓库，同时实现 ClusterStorage 接口
    """
    
    def __init__(self):
        super().__init__(ClusterState)
    
    # ==================== ClusterStorage 接口实现 ====================
    
    async def save_cluster_state(self, group_id: str, state: Dict[str, Any]) -> bool:
        result = await self.upsert_by_group_id(group_id, state)
        return result is not None
    
    async def load_cluster_state(self, group_id: str) -> Optional[Dict[str, Any]]:
        cluster_state = await self.get_by_group_id(group_id)
        if cluster_state is None:
            return None
        return cluster_state.model_dump(exclude={"id", "revision_id"})
    
    async def clear(self, group_id: Optional[str] = None) -> bool:
        if group_id is None:
            await self.delete_all()
        else:
            await self.delete_by_group_id(group_id)
        return True
    
    # ==================== 原生 CRUD 方法 ====================
    
    async def get_by_group_id(self, group_id: str) -> Optional[ClusterState]:
        try:
            return await self.model.find_one(ClusterState.group_id == group_id)
        except Exception as e:
            logger.error(f"获取聚类状态失败: group_id={group_id}, error={e}")
            return None
    
    async def upsert_by_group_id(self, group_id: str, state: Dict[str, Any]) -> Optional[ClusterState]:
        try:
            existing = await self.model.find_one(ClusterState.group_id == group_id)
            
            if existing:
                for key, value in state.items():
                    if hasattr(existing, key):
                        setattr(existing, key, value)
                await existing.save()
                logger.debug(f"更新聚类状态: group_id={group_id}")
                return existing
            else:
                state["group_id"] = group_id
                cluster_state = ClusterState(**state)
                await cluster_state.insert()
                logger.info(f"创建聚类状态: group_id={group_id}")
                return cluster_state
        except Exception as e:
            logger.error(f"保存聚类状态失败: group_id={group_id}, error={e}")
            return None
    
    async def get_cluster_assignments(self, group_id: str) -> Dict[str, str]:
        try:
            cluster_state = await self.model.find_one(ClusterState.group_id == group_id)
            if cluster_state is None:
                return {}
            return cluster_state.eventid_to_cluster or {}
        except Exception as e:
            logger.error(f"获取聚类映射失败: group_id={group_id}, error={e}")
            return {}
    
    async def delete_by_group_id(self, group_id: str) -> bool:
        try:
            cluster_state = await self.model.find_one(ClusterState.group_id == group_id)
            if cluster_state:
                await cluster_state.delete()
                logger.info(f"删除聚类状态: group_id={group_id}")
            return True
        except Exception as e:
            logger.error(f"删除聚类状态失败: group_id={group_id}, error={e}")
            return False
    
    async def delete_all(self) -> int:
        try:
            result = await self.model.delete_all()
            count = result.deleted_count if result else 0
            logger.info(f"删除所有聚类状态: {count} 条")
            return count
        except Exception as e:
            logger.error(f"删除所有聚类状态失败: {e}")
            return 0
