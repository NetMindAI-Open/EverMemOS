"""
Redis消息分组队列管理器BSON序列化增强测试

测试覆盖：
1. BSON序列化支持
2. JSON序列化支持
3. 序列化模式隔离性
4. 自定义RedisGroupQueueItem支持
5. 二进制数据完整性
6. 所有管理器方法的BSON兼容性
7. Lua脚本返回值的二进制处理
"""

import asyncio
import sys
import os
import time
import traceback
import base64
import json

# Mock依赖模块
from unittest.mock import MagicMock

sys.modules['tanka_ai_toolkit'] = MagicMock()
sys.modules['tanka_ai_toolkit.utils'] = MagicMock()
sys.modules['tanka_ai_toolkit.utils.log_tools'] = MagicMock()
sys.modules['tanka_ai_toolkit.utils.log_tools.tanka_log'] = MagicMock()

try:
    from core.queue.redis_group_queue.redis_group_queue_item import (
        SimpleQueueItem,
        SerializationMode,
    )
    from core.queue.redis_group_queue.redis_msg_group_queue_manager import (
        RedisGroupQueueManager,
        ShutdownMode,
    )
    from core.queue.redis_group_queue.redis_msg_group_queue_manager_factory import (
        RedisGroupQueueManagerFactory,
        RedisGroupQueueConfig,
    )
    from core.di.utils import get_bean_by_type
    from component.redis_provider import RedisProvider

    IMPORTS_AVAILABLE = True
    print("✅ 成功导入核心模块")
except ImportError as e:
    print(f"❌ 导入失败: {e}")
    IMPORTS_AVAILABLE = False
    sys.exit(1)


# ==================== 自定义队列项类 ====================


class CustomQueueItem(SimpleQueueItem):
    """自定义队列项，用于测试item_class参数"""

    def __init__(self, data, item_type, priority=0, custom_field="default"):
        super().__init__(data, item_type)
        self.priority = priority
        self.custom_field = custom_field

    def to_dict(self):
        """重写to_dict方法，包含自定义字段"""
        base_dict = super().to_dict()
        base_dict.update({"priority": self.priority, "custom_field": self.custom_field})
        return base_dict

    @classmethod
    def from_json_str(cls, json_str: str):
        """重写from_json_str方法"""
        try:
            json_dict = json.loads(json_str)
            return cls(
                data=json_dict["data"],
                item_type=json_dict.get("item_type", "custom"),
                priority=json_dict.get("priority", 0),
                custom_field=json_dict.get("custom_field", "default"),
            )
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"无效的JSON数据: {e}") from e

    @classmethod
    def from_bson_bytes(cls, bson_bytes: bytes):
        """重写from_bson_bytes方法"""
        try:
            import bson

            data = bson.decode(bson_bytes)
            return cls(
                data=data["data"],
                item_type=data.get("item_type", "custom"),
                priority=data.get("priority", 0),
                custom_field=data.get("custom_field", "default"),
            )
        except Exception as e:
            raise ValueError(f"无效的BSON数据: {e}") from e


# ==================== 基础序列化测试 ====================


async def test_bson_serialization_support(manager_factory):
    """测试BSON序列化支持"""

    # 创建使用BSON序列化的管理器
    bson_manager = await manager_factory.get_manager_with_config(
        key_prefix="bson_test_manager",
        serialization_mode=SerializationMode.BSON,
        auto_start=False,
    )

    print("\n=== 测试BSON序列化模式 ===")

    # 创建包含复杂数据的消息
    complex_message = SimpleQueueItem(
        data={
            "user_id": 12345,
            "content": "测试BSON序列化",
            "metadata": {
                "timestamp": time.time(),
                "tags": ["test", "bson", "serialization"],
                "nested": {"level": 2, "value": 3.14159},
            },
            "binary_data": b"some binary content".hex(),  # 模拟二进制数据
        },
        item_type="bson_test",
    )

    # 投递消息
    success = await bson_manager.deliver_message("bson_group", complex_message)
    assert success, "BSON消息投递应该成功"

    # 获取消息
    messages = await bson_manager.get_messages(score_threshold=0)
    assert len(messages) == 1, "应该获取到1条BSON消息"

    retrieved_message = messages[0]
    assert retrieved_message.data["user_id"] == 12345
    assert retrieved_message.data["content"] == "测试BSON序列化"
    assert retrieved_message.data["metadata"]["tags"] == [
        "test",
        "bson",
        "serialization",
    ]
    assert retrieved_message.data["metadata"]["nested"]["level"] == 2
    assert retrieved_message.item_type == "bson_test"

    print("✅ BSON序列化测试通过")


# ==================== 管理器方法全覆盖测试 ====================


async def test_consumer_management_methods_bson(manager_factory):
    """测试消费者管理方法在BSON模式下的兼容性"""

    bson_manager = await manager_factory.get_manager_with_config(
        key_prefix="consumer_mgmt_bson",
        serialization_mode=SerializationMode.BSON,
        auto_start=False,
    )

    print("\n=== 测试消费者管理方法BSON兼容性 ===")

    # 测试 join_consumer
    owner_count, assigned_partitions = await bson_manager.join_consumer()
    assert owner_count >= 1, "加入消费者后应该有至少1个owner"
    assert isinstance(assigned_partitions, dict), "分配结果应该是字典"
    print(
        f"✅ join_consumer: owner_count={owner_count}, partitions={len(assigned_partitions)}"
    )

    # 测试 keepalive_consumer
    keepalive_success = await bson_manager.keepalive_consumer(bson_manager.owner_id)
    assert keepalive_success, "消费者保活应该成功"
    print("✅ keepalive_consumer 成功")

    # 测试 rebalance_partitions
    rebalance_owner_count, rebalance_partitions = (
        await bson_manager.rebalance_partitions()
    )
    assert rebalance_owner_count >= 1, "rebalance后应该有至少1个owner"
    assert isinstance(rebalance_partitions, dict), "rebalance结果应该是字典"
    print(f"✅ rebalance_partitions: owner_count={rebalance_owner_count}")

    # 测试 cleanup_inactive_owners
    cleaned_count, remaining_count, cleanup_partitions = (
        await bson_manager.cleanup_inactive_owners()
    )
    assert cleaned_count >= 0, "清理数量应该>=0"
    assert remaining_count >= 0, "剩余数量应该>=0"
    assert isinstance(cleanup_partitions, dict), "清理结果应该是字典"
    print(
        f"✅ cleanup_inactive_owners: cleaned={cleaned_count}, remaining={remaining_count}"
    )

    # 测试 exit_consumer
    exit_owner_count, exit_partitions = await bson_manager.exit_consumer()
    assert exit_owner_count >= 0, "退出后owner数量应该>=0"
    assert isinstance(exit_partitions, dict), "退出结果应该是字典"
    print(f"✅ exit_consumer: remaining_owners={exit_owner_count}")

    print("✅ 消费者管理方法BSON兼容性测试通过")


async def test_stats_methods_bson(manager_factory):
    """测试统计方法在BSON模式下的兼容性"""

    bson_manager = await manager_factory.get_manager_with_config(
        key_prefix="stats_bson",
        serialization_mode=SerializationMode.BSON,
        auto_start=False,
    )

    print("\n=== 测试统计方法BSON兼容性 ===")

    # 先投递一些消息
    await bson_manager.join_consumer()
    for i in range(3):
        message = SimpleQueueItem(
            data={"index": i, "content": f"统计测试消息{i}"}, item_type="stats_test"
        )
        await bson_manager.deliver_message(f"stats_group_{i}", message)

    # 测试 get_stats (管理器级别)
    manager_stats = await bson_manager.get_stats()
    assert isinstance(manager_stats, dict), "管理器统计应该是字典"
    assert "total_current_messages" in manager_stats, "应该包含总消息数"
    assert "total_queues" in manager_stats, "应该包含队列数"
    print(
        f"✅ get_stats (manager): messages={manager_stats.get('total_current_messages', 0)}"
    )

    # 测试 get_stats (队列级别)
    queue_stats = await bson_manager.get_stats(group_key="stats_group_0")
    assert isinstance(queue_stats, dict), "队列统计应该是字典"
    assert "current_size" in queue_stats, "应该包含当前大小"
    print(f"✅ get_stats (queue): size={queue_stats.get('current_size', 0)}")

    # 测试 get_stats (包含所有分区)
    all_partitions_stats = await bson_manager.get_stats(
        include_all_partitions=True,
        include_partition_details=True,
        include_consumer_info=True,
    )
    assert isinstance(all_partitions_stats, dict), "全分区统计应该是字典"
    assert "partitions" in all_partitions_stats, "应该包含分区信息"
    assert "active_consumers" in all_partitions_stats, "应该包含消费者信息"
    print(
        f"✅ get_stats (all partitions): partitions={len(all_partitions_stats.get('partitions', []))}"
    )

    # 测试兼容性方法
    queue_stats_compat = await bson_manager.get_queue_stats("stats_group_0")
    assert queue_stats_compat is not None, "兼容性队列统计应该不为空"

    manager_stats_compat = await bson_manager.get_manager_stats()
    assert isinstance(manager_stats_compat, dict), "兼容性管理器统计应该是字典"

    print("✅ 统计方法BSON兼容性测试通过")


async def test_message_operations_bson(manager_factory):
    """测试消息操作在BSON模式下的兼容性"""

    bson_manager = await manager_factory.get_manager_with_config(
        key_prefix="msg_ops_bson",
        serialization_mode=SerializationMode.BSON,
        auto_start=False,
    )

    print("\n=== 测试消息操作BSON兼容性 ===")

    await bson_manager.join_consumer()

    # 测试 deliver_message 与复杂数据
    complex_data = {
        "text": "复杂消息测试",
        "numbers": [1, 2, 3.14, -5],
        "nested": {"bool_val": True, "null_val": None, "unicode": "中文测试🚀"},
        "binary_encoded": base64.b64encode(b"binary data \x00\x01\x02").decode(),
    }

    message = SimpleQueueItem(data=complex_data, item_type="complex_bson")
    success = await bson_manager.deliver_message("complex_group", message)
    assert success, "复杂消息投递应该成功"
    print("✅ deliver_message 复杂数据投递成功")

    # 测试 get_messages 与不同参数
    # 测试基本获取
    messages = await bson_manager.get_messages(score_threshold=0)
    assert len(messages) >= 1, "应该获取到至少1条消息"
    retrieved = messages[0]
    assert retrieved.data["text"] == "复杂消息测试"
    assert retrieved.data["nested"]["unicode"] == "中文测试🚀"
    print("✅ get_messages 基本获取成功")

    # 测试带current_score参数的获取
    current_time_ms = int(time.time() * 1000)
    messages_with_score = await bson_manager.get_messages(
        score_threshold=1000, current_score=current_time_ms  # 1秒阈值
    )
    # 这里可能获取到消息也可能不获取到，取决于时间差
    print(
        f"✅ get_messages 带current_score参数: 获取到{len(messages_with_score)}条消息"
    )

    # 测试指定owner_id的获取
    messages_with_owner = await bson_manager.get_messages(
        score_threshold=0, owner_id=bson_manager.owner_id
    )
    print(f"✅ get_messages 指定owner_id: 获取到{len(messages_with_owner)}条消息")

    print("✅ 消息操作BSON兼容性测试通过")


async def test_lifecycle_management_bson(manager_factory):
    """测试生命周期管理在BSON模式下的兼容性"""

    bson_manager = await manager_factory.get_manager_with_config(
        key_prefix="lifecycle_bson",
        serialization_mode=SerializationMode.BSON,
        auto_start=False,  # 手动控制启动
    )

    print("\n=== 测试生命周期管理BSON兼容性 ===")

    # 测试状态获取
    from core.queue.redis_group_queue.redis_msg_group_queue_manager import ManagerState

    initial_state = bson_manager.get_state()
    assert (
        initial_state == ManagerState.CREATED
    ), f"初始状态应该是CREATED，实际是{initial_state}"
    print("✅ get_state 初始状态正确")

    # 测试启动
    await bson_manager.start()
    started_state = bson_manager.get_state()
    assert (
        started_state == ManagerState.STARTED
    ), f"启动后状态应该是STARTED，实际是{started_state}"
    print("✅ start 启动成功")

    # 测试定期任务启动
    await bson_manager.start_periodic_tasks()  # 应该是幂等的
    print("✅ start_periodic_tasks 幂等调用成功")

    # 测试软性关闭（应该失败，因为可能有消息）
    soft_shutdown_result = await bson_manager.shutdown(ShutdownMode.SOFT)
    print(f"✅ shutdown SOFT模式: 结果={soft_shutdown_result}")

    # 测试硬性关闭
    hard_shutdown_result = await bson_manager.shutdown(ShutdownMode.HARD)
    assert hard_shutdown_result, "硬性关闭应该成功"
    shutdown_state = bson_manager.get_state()
    assert (
        shutdown_state == ManagerState.SHUTDOWN
    ), f"关闭后状态应该是SHUTDOWN，实际是{shutdown_state}"
    print("✅ shutdown HARD模式成功")

    print("✅ 生命周期管理BSON兼容性测试通过")


async def test_force_cleanup_bson(manager_factory):
    """测试强制清理在BSON模式下的兼容性"""

    bson_manager = await manager_factory.get_manager_with_config(
        key_prefix="force_cleanup_bson",
        serialization_mode=SerializationMode.BSON,
        auto_start=False,
    )

    print("\n=== 测试强制清理BSON兼容性 ===")

    # 先加入一些消费者
    await bson_manager.join_consumer()
    await bson_manager.join_consumer("test_owner_1")
    await bson_manager.join_consumer("test_owner_2")

    # 检查消费者数量
    stats_before = await bson_manager.get_stats(include_consumer_info=True)
    consumers_before = len(stats_before.get("active_consumers", []))
    print(f"清理前消费者数量: {consumers_before}")

    # 测试强制清理
    cleaned_count = await bson_manager.force_cleanup_and_reset()
    assert cleaned_count >= 0, "清理数量应该>=0"
    print(f"✅ force_cleanup_and_reset: 清理了{cleaned_count}个消费者")

    # 检查清理后状态
    stats_after = await bson_manager.get_stats(include_consumer_info=True)
    consumers_after = len(stats_after.get("active_consumers", []))
    print(f"清理后消费者数量: {consumers_after}")

    print("✅ 强制清理BSON兼容性测试通过")


async def test_custom_item_class_bson(manager_factory):
    """测试自定义item_class在BSON模式下的兼容性"""

    custom_manager = await manager_factory.get_manager_with_config(
        key_prefix="custom_bson",
        serialization_mode=SerializationMode.BSON,
        item_class=CustomQueueItem,
        auto_start=False,
    )

    print("\n=== 测试自定义item_class BSON兼容性 ===")

    await custom_manager.join_consumer()

    # 创建自定义消息
    custom_message = CustomQueueItem(
        data={"content": "自定义BSON测试", "value": 999},
        item_type="custom_bson_test",
        priority=10,
        custom_field="bson_custom_value",
    )

    # 投递和获取
    success = await custom_manager.deliver_message("custom_bson_group", custom_message)
    assert success, "自定义BSON消息投递应该成功"

    messages = await custom_manager.get_messages(score_threshold=0)
    assert len(messages) == 1, "应该获取到1条自定义BSON消息"

    retrieved = messages[0]
    assert isinstance(retrieved, CustomQueueItem), "获取的消息应该是CustomQueueItem类型"
    assert retrieved.priority == 10, "自定义字段priority应该正确"
    assert (
        retrieved.custom_field == "bson_custom_value"
    ), "自定义字段custom_field应该正确"
    assert retrieved.data["content"] == "自定义BSON测试"

    print("✅ 自定义item_class BSON兼容性测试通过")


async def test_binary_data_integrity_bson(manager_factory):
    """测试二进制数据在BSON模式下的完整性"""

    bson_manager = await manager_factory.get_manager_with_config(
        key_prefix="binary_integrity",
        serialization_mode=SerializationMode.BSON,
        auto_start=False,
    )

    print("\n=== 测试二进制数据完整性 ===")

    await bson_manager.join_consumer()

    # 创建包含各种二进制数据的消息
    test_binary_data = [
        b"",  # 空二进制
        b"\x00",  # 单个null字节
        b"\x00\x01\x02\x03\xff\xfe\xfd",  # 混合字节
        "Hello, 世界! 🌍".encode('utf-8'),  # UTF-8编码的文本
        bytes(range(256)),  # 所有可能的字节值
    ]

    for i, binary_data in enumerate(test_binary_data):
        encoded_data = base64.b64encode(binary_data).decode('utf-8')

        message = SimpleQueueItem(
            data={
                "binary_field": encoded_data,
                "original_length": len(binary_data),
                "test_index": i,
            },
            item_type=f"binary_test_{i}",
        )

        success = await bson_manager.deliver_message(f"binary_group_{i}", message)
        assert success, f"二进制消息{i}投递应该成功"

    # 获取所有消息并验证完整性
    for i in range(len(test_binary_data)):
        messages = await bson_manager.get_messages(score_threshold=0)
        if messages:
            retrieved = messages[0]

            # 解码并验证
            decoded_data = base64.b64decode(retrieved.data["binary_field"])
            original_data = test_binary_data[retrieved.data["test_index"]]

            assert decoded_data == original_data, f"二进制数据{i}完整性验证失败"
            assert (
                len(decoded_data) == retrieved.data["original_length"]
            ), f"二进制数据{i}长度不匹配"

    print("✅ 二进制数据完整性测试通过")


async def test_lua_script_return_compatibility(manager_factory):
    """测试Lua脚本返回值在BSON模式下的兼容性"""

    bson_manager = await manager_factory.get_manager_with_config(
        key_prefix="lua_compat",
        serialization_mode=SerializationMode.BSON,
        auto_start=False,
    )

    print("\n=== 测试Lua脚本返回值兼容性 ===")

    # 测试各种会调用Lua脚本的操作，确保返回值正确处理

    # 1. 测试enqueue脚本返回值处理
    message = SimpleQueueItem(data={"test": "lua_compat"}, item_type="lua_test")
    success = await bson_manager.deliver_message("lua_group", message)
    assert success, "enqueue脚本应该正确处理返回值"
    print("✅ enqueue脚本返回值处理正确")

    # 2. 测试join_consumer脚本返回值处理
    owner_count, partitions = await bson_manager.join_consumer()
    assert isinstance(owner_count, int), "join_consumer应该返回整数owner_count"
    assert isinstance(partitions, dict), "join_consumer应该返回字典partitions"
    for owner_id, partition_list in partitions.items():
        assert isinstance(owner_id, str), f"owner_id应该是字符串: {owner_id}"
        assert isinstance(
            partition_list, list
        ), f"partition_list应该是列表: {partition_list}"
        for partition in partition_list:
            assert isinstance(partition, str), f"partition应该是字符串: {partition}"
    print("✅ join_consumer脚本返回值处理正确")

    # 3. 测试get_messages脚本返回值处理
    messages = await bson_manager.get_messages(score_threshold=0)
    assert isinstance(messages, list), "get_messages应该返回列表"
    if messages:
        assert isinstance(messages[0], SimpleQueueItem), "消息应该正确反序列化"
    print("✅ get_messages脚本返回值处理正确")

    # 4. 测试stats脚本返回值处理
    stats = await bson_manager.get_stats()
    assert isinstance(stats, dict), "get_stats应该返回字典"
    for key, value in stats.items():
        assert isinstance(key, str), f"统计键应该是字符串: {key}"
    print("✅ stats脚本返回值处理正确")

    # 5. 测试cleanup脚本返回值处理
    cleaned, remaining, cleanup_partitions = (
        await bson_manager.cleanup_inactive_owners()
    )
    assert isinstance(cleaned, int), "cleanup应该返回整数cleaned_count"
    assert isinstance(remaining, int), "cleanup应该返回整数remaining_count"
    assert isinstance(cleanup_partitions, dict), "cleanup应该返回字典partitions"
    print("✅ cleanup脚本返回值处理正确")

    print("✅ Lua脚本返回值兼容性测试通过")


# ==================== 运行测试 ====================


async def run_enhanced_bson_tests():
    """运行增强的BSON序列化测试"""
    print("开始运行Redis消息分组队列管理器增强BSON序列化测试...")

    try:
        # 获取管理器工厂实例
        manager_factory = get_bean_by_type(RedisGroupQueueManagerFactory)
        redis_provider = get_bean_by_type(RedisProvider)

    except Exception as e:
        print(f"❌ 获取依赖失败: {e}")
        return

    # 定义所有测试函数
    tests = [
        # 基础序列化测试
        test_bson_serialization_support,
        # 管理器方法全覆盖测试
        test_consumer_management_methods_bson,
        test_stats_methods_bson,
        test_message_operations_bson,
        test_lifecycle_management_bson,
        test_force_cleanup_bson,
        # 自定义类和二进制数据测试
        test_custom_item_class_bson,
        test_binary_data_integrity_bson,
        # Lua脚本兼容性测试
        test_lua_script_return_compatibility,
    ]

    passed = 0
    failed = 0

    # 获取Redis客户端用于清理
    redis_client = await redis_provider.get_named_client(
        "default", decode_responses=True
    )

    # 启动时清理数据库
    try:
        await redis_client.flushdb()
        print("🧹 启动时Redis数据库已清理")
    except Exception as e:
        print(f"⚠️ 启动时清理Redis数据库失败: {e}")
        return

    # 运行所有测试
    for test_func in tests:
        print(f"\n{'='*60}")
        print(f"运行测试: {test_func.__name__}")
        print(f"{'='*60}")

        # 测试前清理Redis数据库
        try:
            await redis_client.flushdb()
            print("🧹 Redis数据库已清理")
        except Exception as e:
            print(f"⚠️ 清理Redis数据库失败: {e}")

        try:
            await test_func(manager_factory)
            print(f"✅ {test_func.__name__} 测试通过")
            passed += 1
        except AssertionError as e:
            print(f"❌ {test_func.__name__} 测试失败: {str(e)}")
            failed += 1
            traceback.print_exc()
            break  # 遇到失败就停止
        except Exception as e:
            print(f"❌ {test_func.__name__} 测试出错: {str(e)}")
            failed += 1
            traceback.print_exc()
            break  # 遇到错误就停止

        # 测试后停止所有管理器
        try:
            await manager_factory.stop_all_managers()
            print("🔌 测试后所有管理器已停止")
        except Exception as e:
            print(f"⚠️ 停止管理器失败: {e}")

        # 短暂等待
        await asyncio.sleep(0.1)

    print(f"\n{'='*60}")
    print(f"增强BSON测试结果: {passed} 通过, {failed} 失败")
    print(f"{'='*60}")

    # 清理所有管理器
    await manager_factory.stop_all_managers()
    print("🔌 所有管理器已停止")

    # 最终清理Redis数据库
    try:
        await redis_client.flushdb()
        print("🧹 最终Redis数据库清理完成")
    except Exception as e:
        print(f"⚠️ 最终清理Redis数据库失败: {e}")
    finally:
        await redis_client.close()
        print("🔌 Redis连接已关闭")


if __name__ == "__main__":
    asyncio.run(run_enhanced_bson_tests())
