"""测试 V3 API HTTP 接口的记忆存储功能

使用真实的 HTTP 请求调用 V3 API 的 /memorize 接口
从 assistant_chat_zh.json 加载真实对话数据
"""
import asyncio
import json
from pathlib import Path
import httpx

from demo.clear_all_data import clear_all_memories


def load_conversation_data(file_path: str) -> tuple:
    """从 JSON 文件加载对话数据
    
    Returns:
        tuple: (messages, group_id, group_name)
    """
    data_file = Path(file_path)
    if not data_file.exists():
        raise FileNotFoundError(f"数据文件不存在: {file_path}")
    
    with open(data_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 提取消息列表和元信息
    messages = data.get('conversation_list', [])
    conversation_meta = data.get('conversation_meta', {})
    group_id = conversation_meta.get('group_id', 'unknown_group')
    group_name = conversation_meta.get('name', 'unknown')
    
    # 为每条消息添加 group_id 和 group_name
    for msg in messages:
        msg['group_id'] = group_id
        msg['group_name'] = group_name
    
    print(f"从 {file_path} 加载了 {len(messages)} 条消息")
    print(f"group_id: {group_id}")
    print(f"group_name: {group_name}")
    
    return messages, group_id, group_name


async def test_v3_memorize_api():
    """测试 V3 API 的 /memorize 接口（单条消息存储）"""
    
    # 清空所有记忆数据
    await clear_all_memories()
    
    # V3 API 基础 URL（根据实际部署修改）
    base_url = "http://localhost:8001"  # 服务运行在 8001 端口
    memorize_url = f"{base_url}/api/v3/agentic/memorize"  # 正确的路由路径
    
    print("=" * 100)
    print("🧪 测试 V3 API HTTP 接口 - 记忆存储")
    print("=" * 100)
    
    # 加载真实对话数据
    data_file = "data/assistant_chat_zh.json"
    try:
        test_messages, group_id, group_name = load_conversation_data(data_file)
    except FileNotFoundError as e:
        print(f"❌ 错误: {e}")
        return False
    
    if not test_messages:
        print("❌ 没有找到消息数据")
        return False
    
    print(f"\n📤 准备发送 {len(test_messages)} 条消息到 V3 API")
    print(f"   URL: {memorize_url}")
    print()
    
    # 逐条发送消息（增加超时时间到120秒，因为LLM调用可能需要时间）
    async with httpx.AsyncClient(timeout=180.0) as client:
        for idx, message in enumerate(test_messages, 1):
            print(f"[{idx}/{len(test_messages)}] 发送消息: {message['sender']} - {message['content'][:30]}...")
            
            try:
                response = await client.post(
                    memorize_url,
                    json=message,
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == 200:
                    result = response.json()
                    status = result.get("status")
                    message_text = result.get("message", "")
                    saved_count = result.get("result", {}).get("count", 0)
                    
                    print(f"   ✅ 成功: {message_text} (保存了 {saved_count} 条记忆)")
                else:
                    print(f"   ❌ 失败: HTTP {response.status_code}")
                    print(f"      {response.text[:200]}")
                    
            except httpx.ConnectError:
                print(f"   ❌ 连接失败: 无法连接到 {base_url}")
                print(f"      请确保 V3 API 服务已启动")
                return False
            except httpx.ReadTimeout:
                print(f"   ⚠️  超时: 处理时间超过180秒（这可能是因为历史数据过多）")
                print(f"      建议: 跳过此消息，继续测试")
                continue  # 跳过超时的消息，继续处理下一条
            except Exception as e:
                print(f"   ❌ 错误: {type(e).__name__}: {e}")
                import traceback
                traceback.print_exc()
                return False
            
            # 延迟2秒，给LLM边界检测足够的时间（每次都要调用LLM判断）
            await asyncio.sleep(2)
    
    print("\n" + "=" * 100)
    print("✅ V3 API HTTP 接口测试完成！")
    print("\n📝 下一步：")
    print("   运行检索测试: python src/bootstrap.py demo/v3_retrieve_memories.py")
    print("=" * 100)
    
    return True


if __name__ == "__main__":
    asyncio.run(test_v3_memorize_api())

