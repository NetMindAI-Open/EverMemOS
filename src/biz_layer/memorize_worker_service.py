"""
记忆提取 Worker 服务

提供全局队列和常驻 Worker，用于异步处理 MemCell 的记忆提取任务。
"""

import asyncio
import traceback
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
from enum import Enum
from dataclasses import dataclass

from memory_layer.memory_manager import MemoryManager
from api_specs.dtos.memory_command import MemorizeRequest
from api_specs.memory_types import (
    MemoryType,
    MemCell,
    Memory,
    SemanticMemoryItem,
)
from memory_layer.memory_extractor.event_log_extractor import EventLog
from core.observation.logger import get_logger
from core.di import get_bean_by_type
from core.di.decorators import service
from infra_layer.adapters.out.persistence.repository.memcell_raw_repository import (
    MemCellRawRepository,
)
from infra_layer.adapters.out.persistence.repository.conversation_meta_raw_repository import (
    ConversationMetaRawRepository,
)
from biz_layer.mem_db_operations import (
    _convert_episode_memory_to_doc,
    _convert_semantic_memory_to_doc,
    _convert_event_log_to_docs,
)
from biz_layer.mem_memorize import (
    MemoryDocPayload,
    save_memory_docs,
    load_core_memories,
    update_status_after_memcell,
    if_memorize,
    _trigger_clustering,
)

logger = get_logger(__name__)


class RequestStatus(str, Enum):
    """请求状态"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class ExtractionState:
    """记忆提取状态，存储中间结果"""
    memcell: MemCell
    request: MemorizeRequest
    current_time: datetime
    scene: str
    is_assistant_scene: bool
    participants: List[str]
    group_episode: Optional[Memory] = None
    group_episode_memories: List[Memory] = None
    episode_memories: List[Memory] = None
    parent_docs_map: Dict[str, Any] = None
    
    def __post_init__(self):
        self.group_episode_memories = []
        self.episode_memories = []
        self.parent_docs_map = {}


@service(name="memorize_worker_service", primary=True)
class MemorizeWorkerService:
    """记忆提取 Worker 服务"""
    
    def __init__(self):
        self.memcell_queue: asyncio.Queue = asyncio.Queue()
        self.worker: Optional[asyncio.Task] = None
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._request_status: Dict[str, RequestStatus] = {}
        logger.info("[MemorizeWorkerService] 初始化")
    
    async def start(self):
        """启动 Worker"""
        if self._running:
            return
        self._running = True
        self._shutdown_event.clear()
        self.worker = asyncio.create_task(self._worker())
        logger.info("[MemorizeWorkerService] Worker 已启动")
    
    async def stop(self, timeout: float = 30.0):
        """停止 Worker"""
        if not self._running:
            return
        self._running = False
        await self.memcell_queue.put(None)
        self._shutdown_event.set()
        if self.worker:
            try:
                await asyncio.wait_for(self.worker, timeout=timeout)
            except asyncio.TimeoutError:
                if not self.worker.done():
                    self.worker.cancel()
        self.worker = None
        logger.info("[MemorizeWorkerService] Worker 已停止")
    
    async def submit_memcell(self, memcell: MemCell, request: MemorizeRequest, current_time: datetime) -> str:
        """提交 MemCell 到队列，返回 request_id"""
        if not self._running:
            await self.start()
        
        request_id = memcell.event_id
        self._request_status[request_id] = RequestStatus.PENDING
        await self.memcell_queue.put({
            'request_id': request_id,
            'memcell': memcell,
            'request': request,
            'current_time': current_time,
        })
        logger.info(f"[Worker] 任务已提交: {request_id}")
        return request_id
    
    def get_status(self, request_id: str) -> Optional[RequestStatus]:
        return self._request_status.get(request_id)
    
    def is_completed(self, request_id: str) -> bool:
        status = self._request_status.get(request_id)
        return status in (RequestStatus.COMPLETED, RequestStatus.FAILED)
    
    # ==================== Worker 主循环 ====================
    
    async def _worker(self):
        """Worker 主循环"""
        logger.info("[Worker] 启动")
        memory_manager = MemoryManager()
        
        while self._running or not self.memcell_queue.empty():
            try:
                try:
                    task_data = await asyncio.wait_for(self.memcell_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                if task_data is None:
                    self.memcell_queue.task_done()
                    break
                
                request_id = task_data['request_id']
                self._request_status[request_id] = RequestStatus.PROCESSING
                
                try:
                    await self._process_memcell(
                        task_data['memcell'],
                        task_data['request'],
                        memory_manager,
                        task_data['current_time'],
                    )
                    self._request_status[request_id] = RequestStatus.COMPLETED
                    logger.info(f"[Worker] ✅ 完成: {request_id}")
                except Exception as e:
                    self._request_status[request_id] = RequestStatus.FAILED
                    logger.error(f"[Worker] ❌ 失败: {request_id}, error={e}")
                    traceback.print_exc()
                
                self.memcell_queue.task_done()
            except Exception as e:
                logger.error(f"[Worker] 异常: {e}")
                traceback.print_exc()
        
        logger.info("[Worker] 退出")
    
    # ==================== 主处理流程 ====================
    
    async def _process_memcell(
        self,
        memcell: MemCell,
        request: MemorizeRequest,
        memory_manager: MemoryManager,
        current_time: datetime,
    ):
        """处理单个 MemCell（主流程调度）"""
        # 1. 初始化状态
        state = await self._init_state(memcell, request, current_time)
        
        # 2. 提取 Episodes
        await self._extract_episodes(state, memory_manager)
        
        # 3. 更新 MemCell 并触发聚类
        await self._update_memcell_and_cluster(state)
        
        # 4. 保存和提取后续记忆
        if if_memorize(memcell):
            await self._process_memories(state, memory_manager)
    
    # ==================== 步骤1：初始化状态 ====================
    
    async def _init_state(
        self,
        memcell: MemCell,
        request: MemorizeRequest,
        current_time: datetime
    ) -> ExtractionState:
        """初始化提取状态"""
        # 获取场景信息
        conversation_meta_repo = get_bean_by_type(ConversationMetaRawRepository)
        conversation_meta = await conversation_meta_repo.get_by_group_id(request.group_id)
        scene = conversation_meta.scene if conversation_meta and conversation_meta.scene else "assistant"
        is_assistant_scene = scene.lower() in ["assistant", "companion"]
        
        # 提取参与者
        participants = list(set(memcell.participants)) if memcell.participants else []
        
        return ExtractionState(
            memcell=memcell,
            request=request,
            current_time=current_time,
            scene=scene,
            is_assistant_scene=is_assistant_scene,
            participants=participants,
        )
    
    # ==================== 步骤2：提取 Episodes ====================
    
    async def _extract_episodes(self, state: ExtractionState, memory_manager: MemoryManager):
        """提取群组和个人 Episodes"""
        # 构建提取任务
        if state.is_assistant_scene:
            logger.info("[Worker] assistant 场景，仅提取群组 Episode")
            tasks = [self._create_episode_task(state, memory_manager, None)]
        else:
            logger.info(f"[Worker] 非 assistant 场景，提取群组 + {len(state.participants)} 个个人 Episode")
            tasks = [self._create_episode_task(state, memory_manager, None)]
            tasks.extend([
                self._create_episode_task(state, memory_manager, uid)
                for uid in state.participants
            ])
        
        # 并行执行
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理结果
        self._process_episode_results(state, results)
    
    def _create_episode_task(self, state: ExtractionState, memory_manager: MemoryManager, user_id: Optional[str]):
        """创建 Episode 提取任务"""
        return memory_manager.extract_memory(
            memcell=state.memcell,
            memory_type=MemoryType.EPISODIC_MEMORY,
            user_id=user_id,
            group_id=state.request.group_id,
            group_name=state.request.group_name,
        )
    
    def _process_episode_results(self, state: ExtractionState, results: List[Any]):
        """处理 Episode 提取结果"""
        # 处理群组 Episode
        group_episode = results[0] if results else None
        if isinstance(group_episode, Exception):
            logger.error(f"[Worker] ❌ 群组 Episode 异常: {group_episode}")
            group_episode = None
        elif group_episode:
            group_episode.ori_event_id_list = [state.memcell.event_id]
            group_episode.memcell_event_id_list = [state.memcell.event_id]
            state.group_episode_memories.append(group_episode)
            state.group_episode = group_episode
            state.memcell.episode = group_episode.episode
            state.memcell.subject = group_episode.subject
            logger.info("[Worker] ✅ 群组 Episode 提取成功")
        
        # 处理个人 Episodes
        if not state.is_assistant_scene:
            for user_id, result in zip(state.participants, results[1:]):
                if isinstance(result, Exception):
                    logger.error(f"[Worker] ❌ 个人 Episode 异常: user_id={user_id}")
                    continue
                if result:
                    result.ori_event_id_list = [state.memcell.event_id]
                    result.memcell_event_id_list = [state.memcell.event_id]
                    state.episode_memories.append(result)
                    logger.info(f"[Worker] ✅ 个人 Episode 成功: user_id={user_id}")
    
    # ==================== 步骤3：更新 MemCell 并触发聚类 ====================
    
    async def _update_memcell_and_cluster(self, state: ExtractionState):
        """更新 MemCell 的 episode 字段并触发聚类"""
        if not state.request.group_id or not state.group_episode:
            return
        
        # 更新 MemCell
        try:
            memcell_repo = get_bean_by_type(MemCellRawRepository)
            await memcell_repo.update_by_event_id(
                event_id=state.memcell.event_id,
                update_data={"episode": state.group_episode.episode, "subject": state.group_episode.subject}
            )
            logger.info(f"[Worker] ✅ 更新 MemCell episode: {state.memcell.event_id}")
        except Exception as e:
            logger.error(f"[Worker] ❌ 更新 MemCell 失败: {e}")
        
        # 异步触发聚类
        try:
            memcell_for_clustering = MemCell(
                event_id=state.memcell.event_id,
                user_id_list=state.memcell.user_id_list,
                original_data=state.memcell.original_data,
                timestamp=state.memcell.timestamp,
                summary=state.memcell.summary,
                group_id=state.memcell.group_id,
                group_name=state.memcell.group_name,
                participants=state.memcell.participants,
                type=state.memcell.type,
                episode=state.group_episode.episode,
            )
            asyncio.create_task(_trigger_clustering(state.request.group_id, memcell_for_clustering, state.scene))
            logger.info(f"[Worker] 异步触发聚类 (scene={state.scene})")
        except Exception as e:
            logger.error(f"[Worker] ❌ 触发聚类失败: {e}")
    
    # ==================== 步骤4：保存和提取后续记忆 ====================
    
    async def _process_memories(self, state: ExtractionState, memory_manager: MemoryManager):
        """保存 Episodes 并提取/保存 Semantic 和 EventLog"""
        await load_core_memories(state.request, state.participants, state.current_time)
        
        # 准备要保存的 Episodes
        episodic_source = state.group_episode_memories + state.episode_memories
        episodes_to_save = list(episodic_source)
        
        # assistant 场景：复制群组 Episode 给每个用户
        if state.is_assistant_scene and state.group_episode_memories:
            episodes_to_save.extend(self._clone_episodes_for_users(state))
        
        # 保存 Episodes
        if episodes_to_save:
            await self._save_episodes(state, episodes_to_save, episodic_source)
        
        # 提取并保存 Semantic 和 EventLog
        if episodic_source:
            semantic_memories, event_logs = await self._extract_semantic_and_eventlog(
                state, memory_manager, episodic_source
            )
            await self._save_semantic_and_eventlog(state, semantic_memories, event_logs)
        
        # 更新状态表
        await update_status_after_memcell(state.request, state.memcell, state.current_time, state.request.raw_data_type)
    
    def _clone_episodes_for_users(self, state: ExtractionState) -> List[Memory]:
        """为每个用户复制群组 Episode"""
        from dataclasses import replace
        cloned = []
        group_ep = state.group_episode_memories[0]
        for user_id in state.participants:
            if "robot" in user_id.lower() or "assistant" in user_id.lower():
                continue
            cloned.append(replace(group_ep, user_id=user_id, user_name=user_id))
        logger.info(f"[Worker] 复制群组 Episode 给 {len(cloned)} 个用户")
        return cloned
    
    async def _save_episodes(
        self,
        state: ExtractionState,
        episodes_to_save: List[Memory],
        episodic_source: List[Memory]
    ):
        """保存 Episodes 到数据库"""
        for ep in episodes_to_save:
            if getattr(ep, "group_name", None) is None:
                ep.group_name = state.request.group_name
            if getattr(ep, "user_name", None) is None:
                ep.user_name = ep.user_id
        
        docs = [_convert_episode_memory_to_doc(ep, state.current_time) for ep in episodes_to_save]
        payloads = [MemoryDocPayload(MemoryType.EPISODIC_MEMORY, doc) for doc in docs]
        saved_map = await save_memory_docs(payloads)
        saved_docs = saved_map.get(MemoryType.EPISODIC_MEMORY, [])
        
        # 设置 event_id（只给 episodic_source）
        for ep, saved_doc in zip(episodic_source, saved_docs):
            ep.event_id = str(saved_doc.event_id)
            state.parent_docs_map[str(saved_doc.event_id)] = saved_doc
    
    async def _extract_semantic_and_eventlog(
        self,
        state: ExtractionState,
        memory_manager: MemoryManager,
        episodic_source: List[Memory]
    ) -> Tuple[List[SemanticMemoryItem], List[EventLog]]:
        """提取 Semantic 和 EventLog"""
        logger.info(f"[Worker] 提取 Semantic/EventLog，共 {len(episodic_source)} 个 Episode")
        
        tasks = []
        metadata = []
        
        for ep in episodic_source:
            if not ep.event_id:
                continue
            # Semantic
            tasks.append(memory_manager.extract_memory(
                memcell=state.memcell, memory_type=MemoryType.SEMANTIC_MEMORY,
                user_id=ep.user_id, episode_memory=ep,
            ))
            metadata.append({'type': MemoryType.SEMANTIC_MEMORY, 'ep': ep})
            # EventLog
            tasks.append(memory_manager.extract_memory(
                memcell=state.memcell, memory_type=MemoryType.PERSONAL_EVENT_LOG,
                user_id=ep.user_id, episode_memory=ep,
            ))
            metadata.append({'type': MemoryType.PERSONAL_EVENT_LOG, 'ep': ep})
        
        if not tasks:
            return [], []
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        semantic_memories = []
        event_logs = []
        
        for meta, result in zip(metadata, results):
            if isinstance(result, Exception) or not result:
                continue
            
            ep = meta['ep']
            if meta['type'] == MemoryType.SEMANTIC_MEMORY:
                for mem in result:
                    # 动态设置上下文属性
                    mem.parent_event_id = ep.event_id
                    mem.user_id = ep.user_id
                    mem.group_id = ep.group_id
                    mem.group_name = ep.group_name
                    mem.user_name = ep.user_name
                    semantic_memories.append(mem)
            elif meta['type'] == MemoryType.PERSONAL_EVENT_LOG:
                # 动态设置上下文属性
                result.parent_event_id = ep.event_id
                result.user_id = ep.user_id
                result.group_id = ep.group_id
                result.group_name = ep.group_name
                result.user_name = ep.user_name
                event_logs.append(result)
        
        return semantic_memories, event_logs
    
    async def _save_semantic_and_eventlog(
        self,
        state: ExtractionState,
        semantic_memories: List[SemanticMemoryItem],
        event_logs: List[EventLog]
    ):
        """保存 Semantic 和 EventLog"""
        # 转换为 docs
        semantic_docs = []
        for mem in semantic_memories:
            parent_doc = state.parent_docs_map.get(str(mem.parent_event_id))
            if parent_doc:
                semantic_docs.append(_convert_semantic_memory_to_doc(mem, parent_doc, state.current_time))
        
        event_log_docs = []
        for el in event_logs:
            parent_doc = state.parent_docs_map.get(str(el.parent_event_id))
            if parent_doc:
                event_log_docs.extend(_convert_event_log_to_docs(el, parent_doc, state.current_time))
        
        # assistant 场景：复制给每个用户
        if state.is_assistant_scene:
            user_ids = [u for u in state.participants if "robot" not in u.lower() and "assistant" not in u.lower()]
            semantic_docs.extend([
                doc.model_copy(update={"user_id": uid, "user_name": uid})
                for doc in semantic_docs for uid in user_ids
            ])
            event_log_docs.extend([
                doc.model_copy(update={"user_id": uid, "user_name": uid})
                for doc in event_log_docs for uid in user_ids
            ])
            logger.info(f"[Worker] 复制 Semantic/EventLog 给 {len(user_ids)} 个用户")
        
        # 保存
        payloads = []
        payloads.extend(MemoryDocPayload(MemoryType.SEMANTIC_MEMORY, doc) for doc in semantic_docs)
        payloads.extend(MemoryDocPayload(MemoryType.PERSONAL_EVENT_LOG, doc) for doc in event_log_docs)
        if payloads:
            await save_memory_docs(payloads)
