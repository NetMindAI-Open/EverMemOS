"""
应用逻辑中间件
负责提取和设置应用级别的上下文信息，以及处理应用相关的逻辑（如上报等）
"""

from typing import Callable, Dict, Any, Optional
from contextvars import Token

from fastapi import Request
from starlette.responses import Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from core.observation.logger import get_logger
from core.context.context import clear_current_app_info, set_current_app_info
from core.di.utils import get_bean_by_type
from component.app_logic_provider import AppLogicProvider

logger = get_logger(__name__)


class AppLogicMiddleware(BaseHTTPMiddleware):
    """
    应用逻辑中间件

    负责管理请求生命周期，调用 AppLogicProvider 的回调方法：
    - on_request_begin(): 请求开始时调用
    - on_request_complete(): 请求结束时调用
    """

    def __init__(self, app: ASGIApp):
        super().__init__(app)
        self._app_logic_provider = get_bean_by_type(AppLogicProvider)

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        app_context_token: Optional[Token] = None
        app_info: Optional[Dict[str, Any]] = None
        error_message: Optional[str] = None
        http_code: int = 200

        try:
            # ========== 请求开始：调用 on_request_begin ==========
            app_info = await self._app_logic_provider.on_request_begin(request)

            # 设置应用上下文
            if app_info:
                app_context_token = set_current_app_info(app_info)

            # 处理请求
            response = await call_next(request)
            http_code = response.status_code

            return response

        except Exception as e:
            logger.error("应用逻辑中间件处理异常: %s", e)
            error_message = str(e)
            http_code = 500
            raise

        finally:
            # ========== 请求结束：调用 on_request_complete ==========
            if app_info:
                try:
                    await self._app_logic_provider.on_request_complete(
                        request=request,
                        app_info=app_info,
                        http_code=http_code,
                        error_message=error_message,
                    )
                except Exception as callback_error:
                    # 回调失败不应影响请求处理
                    logger.warning("on_request_complete 执行失败: %s", callback_error)

            # 清理应用上下文
            if app_context_token:
                try:
                    clear_current_app_info(app_context_token)
                except Exception as cleanup_error:
                    logger.warning("清理应用上下文时发生错误: %s", cleanup_error)
