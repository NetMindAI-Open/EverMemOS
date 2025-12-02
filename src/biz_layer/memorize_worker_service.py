"""
记忆提取 Worker 服务

只负责队列管理和任务调度，业务逻辑在 mem_memorize.py 中。
"""

import asyncio
import traceback
from typing import Optional, Dict
from datetime import datetime
from enum import Enum

from memory_layer.memory_manager import MemoryManager
from api_specs.dtos.memory_command import MemorizeRequest
from api_specs.memory_types import MemCell
from core.observation.logger import get_logger
from core.di.decorators import service
from biz_layer.mem_memorize import process_memory_extraction

logger = get_logger(__name__)


class RequestStatus(str, Enum):
    """请求状态"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


@service(name="memorize_worker_service", primary=True)
class MemorizeWorkerService:
    """记忆提取 Worker 服务（仅队列调度）"""
    
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
        self.worker = asyncio.create_task(self._worker_loop())
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
    
    async def _worker_loop(self):
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
                    # 记忆提取主流程
                    await process_memory_extraction(
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
