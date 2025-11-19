"""
Redis消息分组队列管理器高级测试
专门测试消费者过期和队列容量相关场景
"""

import asyncio
import sys
import os

# Mock依赖
from unittest.mock import MagicMock

sys.modules['tanka_ai_toolkit'] = MagicMock()
sys.modules['tanka_ai_toolkit.utils'] = MagicMock()
sys.modules['tanka_ai_toolkit.utils.log_tools'] = MagicMock()
sys.modules['tanka_ai_toolkit.utils.log_tools.tanka_log'] = MagicMock()

from core.queue.redis_group_queue.redis_group_queue_item import SimpleQueueItem


async def test_consumer_expiry_and_rejoin(manager_factory):
    """测试消费者过期后的重新注册和消费行为"""
    print("🧪 测试消费者过期后重新注册...")

    # 创建管理器，设置短过期时间便于测试
    manager = await manager_factory.get_manager_with_config(
        key_prefix="expiry_test",
        owner_expire_seconds=2,  # 2秒过期
        inactive_threshold_seconds=1,  # 1秒不活跃
        cleanup_interval_seconds=1,  # 1秒清理间隔
        auto_start=True,
    )

    consumer_id = "test_consumer"

    # 消费者加入
    await manager.join_consumer(consumer_id)
    print("✅ 消费者加入成功")

    # 投递消息
    for i in range(3):
        message = SimpleQueueItem(
            data={"id": i, "content": f"消息{i}"}, item_type="test"
        )
        await manager.deliver_message(f"group_{i}", message)
    print("✅ 投递3条消息")

    # 等待消费者过期
    print("⏰ 等待消费者过期...")
    await asyncio.sleep(4)  # 等待过期

    # 手动清理过期消费者
    await manager.cleanup_inactive_owners()

    # 过期消费者尝试获取消息（应该自动重新注册）
    messages = await manager.get_messages(score_threshold=0, owner_id=consumer_id)
    print(f"✅ 过期后获取到 {len(messages)} 条消息")

    # 验证消费者重新注册
    stats = await manager.get_stats(include_consumer_info=True)
    active_consumers = stats.get("active_consumers", [])
    assert consumer_id in active_consumers, "消费者应该重新注册"

    print("✅ 消费者过期重新注册测试通过")


async def test_capacity_limit_concurrent_consumption(manager_factory):
    """测试小容量下多并发消费者消费满队列后继续投递"""
    print("🧪 测试容量限制下的并发消费...")

    # 创建小容量管理器
    max_capacity = 20
    manager = await manager_factory.get_manager_with_config(
        key_prefix="capacity_test", max_total_messages=max_capacity, auto_start=False
    )

    # 投递消息直到队列满
    print(f"📝 投递消息填满队列（容量={max_capacity}）...")
    successful = 0
    for i in range(max_capacity + 10):  # 尝试投递更多消息
        message = SimpleQueueItem(
            data={"id": i, "content": f"消息{i}"}, item_type="capacity_test"
        )
        success = await manager.deliver_message(f"group_{i % 5}", message)
        if success:
            successful += 1

    print(f"✅ 成功投递 {successful} 条消息")
    assert successful == max_capacity, "投递数不应超过容量限制"

    # 创建多个消费者
    consumers = ["consumer_1", "consumer_2", "consumer_3"]
    for consumer_id in consumers:
        await manager.join_consumer(consumer_id)
    print(f"✅ 创建了 {len(consumers)} 个消费者")

    # 并发消费消息
    print("📝 开始并发消费...")
    total_consumed = 0

    async def consume_worker(consumer_id):
        consumed = 0
        for _ in range(10):  # 每个消费者尝试消费10次
            messages = await manager.get_messages(
                score_threshold=0, owner_id=consumer_id
            )
            consumed += len(messages)
            if not messages:
                await asyncio.sleep(0.1)  # 短暂等待
        return consumed

    # 启动并发消费
    tasks = [asyncio.create_task(consume_worker(cid)) for cid in consumers]
    results = await asyncio.gather(*tasks)
    total_consumed = sum(results)

    print(f"✅ 总共消费了 {total_consumed} 条消息")

    # 验证队列腾出空间后能继续投递
    print("📝 测试消费后能否继续投递...")
    new_successful = 0
    for i in range(20):  # 尝试投递20条新消息
        message = SimpleQueueItem(
            data={"id": i + 1000, "content": f"新消息{i}"}, item_type="new_test"
        )
        success = await manager.deliver_message(f"new_group_{i % 3}", message)
        if success:
            new_successful += 1

    print(f"✅ 消费后成功投递 {new_successful} 条新消息")
    assert new_successful > 0, "消费后应该能投递新消息"

    # 验证最终状态
    final_stats = await manager.get_stats()
    final_messages = final_stats.get("total_current_messages", 0)
    assert final_messages <= max_capacity, "最终消息数应该不超过容量限制"

    print("✅ 容量限制并发消费测试通过")


async def run_tests():
    """运行测试"""
    try:
        from core.di.utils import get_bean_by_type
        from core.queue.redis_group_queue.redis_msg_group_queue_manager_factory import (
            RedisGroupQueueManagerFactory,
        )
        from component.redis_provider import RedisProvider

        manager_factory = get_bean_by_type(RedisGroupQueueManagerFactory)
        redis_provider = get_bean_by_type(RedisProvider)
        redis_client = await redis_provider.get_named_client(
            "default", decode_responses=True
        )

        print("✅ 获取管理器工厂成功")
    except (ImportError, AttributeError) as e:
        print(f"❌ 获取管理器工厂失败: {e}")
        return

    tests = [
        test_consumer_expiry_and_rejoin,
        test_capacity_limit_concurrent_consumption,
    ]

    passed = 0
    failed = 0

    for test_func in tests:
        print(f"\n{'='*50}")
        print(f"运行测试: {test_func.__name__}")
        print('=' * 50)

        # 清理数据库
        await redis_client.flushdb()

        try:
            await test_func(manager_factory)
            print(f"✅ {test_func.__name__} 通过")
            passed += 1
        except (AssertionError, RuntimeError) as e:
            print(f"❌ {test_func.__name__} 失败: {e}")
            failed += 1

        # 停止管理器
        await manager_factory.stop_all_managers()
        await redis_client.flushdb()
        await asyncio.sleep(0.2)

    print(f"\n🏁 测试结果: {passed} 通过, {failed} 失败")

    await manager_factory.stop_all_managers()
    await redis_client.close()


if __name__ == "__main__":
    asyncio.run(run_tests())
