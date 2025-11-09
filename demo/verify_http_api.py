"""验证 chat_with_memory.py 是否使用 HTTP API

运行方法：
    uv run python demo/verify_http_api.py
"""

import asyncio
import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from demo.chat.session import ChatSession
from demo.memory_config import ChatModeConfig, LLMConfig, ScenarioType
from demo.i18n_texts import I18nTexts


async def verify():
    """验证是否使用 HTTP API"""
    print("=" * 80)
    print("🔍 验证 chat_with_memory.py 是否使用 HTTP API")
    print("=" * 80)
    
    # 创建配置
    config = ChatModeConfig()
    llm_config = LLMConfig()
    texts = I18nTexts("zh")
    
    # 创建会话
    session = ChatSession(
        group_id="test_group",
        config=config,
        llm_config=llm_config,
        scenario_type=ScenarioType.ASSISTANT,
        retrieval_mode="rrf",
        data_source="memcell",
        texts=texts,
    )
    
    print(f"\n✅ API 基础 URL: {session.api_base_url}")
    print(f"✅ Lightweight 检索 URL: {session.retrieve_lightweight_url}")
    print(f"✅ Agentic 检索 URL: {session.retrieve_agentic_url}")
    
    # 检查是否有 MemoryManager（应该没有）
    has_memory_manager = hasattr(session, 'memory_manager')
    print(f"\n{'❌' if has_memory_manager else '✅'} MemoryManager 存在: {has_memory_manager}")
    
    # 检查检索方法
    print(f"\n✅ 检索方法:")
    print(f"   - retrieve_memories: {hasattr(session, 'retrieve_memories')}")
    print(f"   - _call_retrieve_lightweight_api: {hasattr(session, '_call_retrieve_lightweight_api')}")
    print(f"   - _call_retrieve_agentic_api: {hasattr(session, '_call_retrieve_agentic_api')}")
    
    # 检查导入
    print(f"\n✅ 检查导入:")
    import demo.chat.session as session_module
    source = session_module.__file__
    with open(source, 'r') as f:
        content = f.read()
        has_httpx = 'import httpx' in content
        has_memory_manager_import = 'from agentic_layer.memory_manager import MemoryManager' in content
        
        print(f"   - httpx 已导入: {'✅' if has_httpx else '❌'}")
        print(f"   - MemoryManager 已导入: {'❌' if has_memory_manager_import else '✅ (已移除)'}")
    
    print("\n" + "=" * 80)
    if has_httpx and not has_memory_manager_import:
        print("🎉 验证成功！chat_with_memory.py 正在使用 HTTP API")
    else:
        print("⚠️  验证失败，请检查代码")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(verify())

