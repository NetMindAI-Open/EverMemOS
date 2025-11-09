"""MemSys 简单示例 - 最简洁的使用方式

使用方法：
    uv run python src/bootstrap.py demo/simple_demo.py
"""

import asyncio
from dotenv import load_dotenv
from demo.simple_memory_manager import SimpleMemoryManager

load_dotenv()


async def main():
    # 创建记忆管理器
    memory = SimpleMemoryManager()
    
    # 添加记忆
    await memory.add_memory(
        messages=[
            {"role": "user", "content": "我喜欢踢足球，周末经常去球场"},
            {"role": "assistant", "content": "足球是很好的运动！你最喜欢哪个球队？"},
            {"role": "user", "content": "我最喜欢巴塞罗那队，梅西是我的偶像"},
        ],
        group_id="sports_chat",
    )
    
    print("✅ 记忆已添加\n")
    
    # 等待数据写入和索引构建（MongoDB + ES + Milvus）
    print("⏳ 等待 10 秒，确保数据写入和索引构建...")
    await asyncio.sleep(10)
    
    # 验证数据是否已存储
    count = await memory.check_memory_count(group_id="sports_chat")
    print(f"📊 已存储的记忆数量: {count}\n")
    
    # 搜索记忆
    results = await memory.search_memory(
        query="用户喜欢什么运动？",
        group_id="sports_chat",
    )
    
    print(f"🔍 搜索结果: {results}\n")


if __name__ == "__main__":
    asyncio.run(main())
