from typing import Dict, Any, Optional
from abc import ABC, abstractmethod
import uuid
from fastapi import Request

from core.di.decorators import component
from core.observation.logger import get_logger

logger = get_logger(__name__)


class AppLogicProvider(ABC):
    """
    应用逻辑提供者接口

    负责从请求中提取应用级别的上下文信息及处理应用逻辑。
    提供请求生命周期的钩子方法：
    - on_request_begin(): 请求开始时调用
    - on_request_complete(): 请求结束时调用（可选实现）
    """

    @abstractmethod
    async def on_request_begin(self, request: Request) -> Dict[str, Any]:
        """
        请求开始时的回调方法

        处理请求并提供应用级别的上下文数据，例如：
        - 提取 request_id
        - 设置租户上下文
        - 投递请求开始事件

        Args:
            request: FastAPI请求对象

        Returns:
            Dict[str, Any]: 包含所有上下文数据的字典（app_info）
        """
        raise NotImplementedError

    async def on_request_complete(
        self,
        request: Request,
        app_info: Dict[str, Any],
        http_code: int,
        error_message: Optional[str] = None,
    ) -> None:
        """
        请求完成时的回调方法（可选实现）

        子类可以重写此方法来处理请求完成后的逻辑，
        例如：记录请求日志、投递事件等。

        Args:
            request: FastAPI 请求对象
            app_info: 应用信息字典，包含 on_request_begin() 返回的数据
            http_code: HTTP 响应状态码
            error_message: 错误信息（可选）
        """
        # 默认实现为空，子类可选择性重写
        _ = (request, app_info, http_code, error_message)  # 避免未使用参数警告


@component(name="app_logic_provider")
class AppLogicProviderImpl(AppLogicProvider):
    """应用逻辑提供者实现，负责从请求中提取应用级别的上下文信息"""

    async def on_request_begin(self, request: Request) -> Dict[str, Any]:
        """
        请求开始时的回调方法

        Args:
            request: FastAPI请求对象

        Returns:
            Dict[str, Any]: 包含所有上下文数据的字典，包括request_id
        """
        # 创建新的app_info字典
        app_info = {}

        # 从请求头中获取request_id，优先X-Request-Id，兼容小写
        request_id = request.headers.get('X-Request-Id') or request.headers.get(
            'x-request-id'
        )
        if not request_id:
            request_id = str(uuid.uuid4())

        app_info['request_id'] = request_id

        return app_info
