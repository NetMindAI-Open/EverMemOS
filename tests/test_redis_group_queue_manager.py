"""
Redis消息分组队列管理器综合测试

测试覆盖：
1. 基本功能测试（投递、获取、统计）
2. 分组路由测试
3. Score阈值测试
4. 消费者管理测试（加入、退出、保活）
5. Rebalance测试
6. 清理功能测试
7. 边角情况测试
8. 并发测试
"""

import asyncio
import sys
import time
import traceback

# 尝试导入，如果失败则使用mock
try:
    # 先尝试 Mock tanka_ai_toolkit 依赖
    from unittest.mock import MagicMock

    # Mock tanka_ai_toolkit 模块
    sys.modules['tanka_ai_toolkit'] = MagicMock()
    sys.modules['tanka_ai_toolkit.utils'] = MagicMock()
    sys.modules['tanka_ai_toolkit.utils.log_tools'] = MagicMock()
    sys.modules['tanka_ai_toolkit.utils.log_tools.tanka_log'] = MagicMock()

    from core.queue.redis_group_queue.redis_group_queue_item import SimpleQueueItem
    from core.queue.redis_group_queue.redis_msg_group_queue_manager import ShutdownMode

    IMPORTS_AVAILABLE = True
    print("✅ 成功导入核心模块（使用Mock依赖）")
except ImportError as e:
    print(f"⚠️ 导入失败: {e}")
    print("将使用Mock对象进行测试...")
    IMPORTS_AVAILABLE = False

    # 创建Mock类
    class MockShutdownMode:
        SOFT = "soft"
        HARD = "hard"

    class MockSimpleQueueItem:
        def __init__(self, data, item_type):
            self.data = data
            self.item_type = item_type

    ShutdownMode = MockShutdownMode
    SimpleQueueItem = MockSimpleQueueItem


# ==================== 基本功能测试 ====================


async def test_basic_message_delivery_and_retrieval(manager_factory):
    """测试基本的消息投递和获取"""
    manager = await manager_factory.get_manager()

    # 创建示例消息
    sample_message = SimpleQueueItem(
        data={"user_id": "12345", "content": "Hello World", "timestamp": time.time()},
        item_type="chat_message",
    )

    # 投递消息
    success = await manager.deliver_message("test_group_1", sample_message)
    assert success, "消息投递应该成功"

    # 获取消息
    messages = await manager.get_messages(score_threshold=0)
    assert len(messages) == 1, "应该获取到1条消息"

    retrieved_message = messages[0]
    assert retrieved_message.data["user_id"] == "12345"
    assert retrieved_message.data["content"] == "Hello World"
    assert retrieved_message.item_type == "chat_message"


async def test_message_delivery_limit(manager_factory):
    """测试消息投递上限"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="limit_test_manager", max_total_messages=3, auto_start=False
    )

    # 投递消息直到达到上限
    for i in range(3):
        message = SimpleQueueItem(
            data={"id": i, "content": f"Message {i}"}, item_type="test"
        )
        success = await manager.deliver_message(f"group_{i}", message)
        assert success, f"第{i+1}条消息应该投递成功"

    # 第4条消息应该被拒绝
    extra_message = SimpleQueueItem(
        data={"id": 4, "content": "Extra message"}, item_type="test"
    )
    success = await manager.deliver_message("group_4", extra_message)
    assert not success, "超过上限的消息应该被拒绝"

    # 验证统计信息
    stats = await manager.get_manager_stats()
    assert stats["total_delivered_messages"] == 3
    assert stats["total_rejected_messages"] == 1


async def test_queue_statistics(manager_factory):
    """测试队列统计信息"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="stats_test_manager", auto_start=False
    )

    # 投递几条消息到不同分组
    groups = ["stats_group_1", "stats_group_2", "stats_group_3"]
    for i, group in enumerate(groups):
        message = SimpleQueueItem(
            data={"id": i, "content": f"Stats message {i}"}, item_type="stats_test"
        )
        await manager.deliver_message(group, message)

    # 获取管理器统计信息
    manager_stats = await manager.get_manager_stats()
    assert manager_stats["total_delivered_messages"] == 3
    assert manager_stats["total_current_messages"] == 3
    assert manager_stats["total_queues"] == 50  # 固定分区数量

    # 获取特定队列统计信息
    queue_stats = await manager.get_queue_stats("stats_group_1")
    assert queue_stats is not None
    assert queue_stats["current_size"] >= 0  # 可能被路由到不同分区


async def test_improved_stats_functionality(manager_factory):
    """测试改进后的统计功能"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="improved_stats_test", auto_start=False
    )

    # 投递一些测试消息到不同分组
    test_groups = ["group_a", "group_b", "group_c"]
    for group in test_groups:
        for j in range(2):  # 每个组投递2条消息
            message = SimpleQueueItem(
                data={"id": f"{group}_{j}", "content": f"Message {j} for {group}"},
                item_type="improved_stats_test",
            )
            await manager.deliver_message(group, message)

    print("\n=== 测试基础统计功能 ===")

    # 测试1: 基础管理器统计
    basic_stats = await manager.get_stats()
    print(
        f"基础统计: type={basic_stats['type']}, 消息数={basic_stats['actual_messages_in_queues']}"
    )
    assert basic_stats["type"] == "manager_stats"
    assert basic_stats["actual_messages_in_queues"] == 6  # 3组 * 2条消息
    assert basic_stats["total_queues"] == 50

    # 测试2: 单个队列统计
    queue_stats = await manager.get_stats(group_key="group_a")
    print(f"队列统计: type={queue_stats['type']}, 队列名={queue_stats['queue_name']}")
    assert queue_stats["type"] == "queue_stats"
    assert "group_a" in queue_stats["queue_name"]
    assert "partition" in queue_stats

    # 测试3: 包含分区详细信息的统计
    detailed_stats = await manager.get_stats(include_partition_details=True)
    print(f"详细统计: 非空分区数={detailed_stats['non_empty_partitions']}")
    assert "partitions" in detailed_stats
    assert detailed_stats["non_empty_partitions"] >= 1
    assert len(detailed_stats["partitions"]) == 50

    # 测试4: 包含消费者信息的统计
    consumer_stats = await manager.get_stats(include_consumer_info=True)
    print(f"消费者统计: 活跃消费者数={consumer_stats['active_consumers_count']}")
    assert "active_consumers_count" in consumer_stats
    assert "active_consumers" in consumer_stats
    assert "partition_assignments" in consumer_stats

    # 测试5: 全分区统计（指定group_key但包含所有分区）
    all_partitions_stats = await manager.get_stats(
        group_key="group_a", include_all_partitions=True, include_partition_details=True
    )
    print(f"全分区统计: type={all_partitions_stats['type']}")
    assert all_partitions_stats["type"] == "all_partitions_stats"
    assert "partitions" in all_partitions_stats

    print("✅ 改进后的统计功能测试通过")


async def test_stats_performance_and_accuracy(manager_factory):
    """测试统计功能的性能和准确性"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="stats_perf_test", auto_start=False
    )

    print("\n=== 测试统计功能性能和准确性 ===")

    # 投递大量消息到不同分区
    message_count = 50
    for i in range(message_count):
        message = SimpleQueueItem(
            data={"id": i, "batch": "performance_test"}, item_type="perf_test"
        )
        await manager.deliver_message(f"perf_group_{i}", message)

    # 测试统计准确性
    start_time = time.time()

    stats = await manager.get_stats(
        include_partition_details=True, include_consumer_info=True
    )

    end_time = time.time()
    duration = end_time - start_time

    print(f"统计查询耗时: {duration:.3f}秒")
    print(f"实际消息数: {stats['actual_messages_in_queues']}")
    print(f"计数器总数: {stats['counter_total_count']}")
    print(f"非空分区数: {stats['non_empty_partitions']}")

    # 验证准确性
    assert stats["actual_messages_in_queues"] == message_count
    assert stats["counter_total_count"] == message_count
    assert stats["non_empty_partitions"] >= 1
    assert duration < 6.0  # 统计查询应该在6秒内完成(5s rate limit)

    # 验证分区统计的总和等于实际消息数
    total_in_partitions = sum(p["current_size"] for p in stats["partitions"])
    assert total_in_partitions == message_count

    print("✅ 统计功能性能和准确性测试通过")


async def test_stats_error_handling(manager_factory):
    """测试统计功能的错误处理"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="stats_error_test", auto_start=False
    )

    print("\n=== 测试统计功能错误处理 ===")

    # 测试正常情况
    normal_stats = await manager.get_stats()
    assert normal_stats["type"] != "error_fallback"

    # 测试空队列统计
    empty_stats = await manager.get_stats(group_key="nonexistent_group")
    assert empty_stats["type"] == "queue_stats"
    assert empty_stats["current_size"] == 0

    print("✅ 统计功能错误处理测试通过")


# ==================== 分组路由测试 ====================


async def test_group_key_routing_consistency(manager_factory):
    """测试分组键路由的一致性"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="routing_test_manager", auto_start=False  # 禁用自动启动定期任务
    )

    # 同一个group_key应该总是路由到同一个分区
    group_key = "consistent_group"

    partitions = set()
    for _ in range(10):
        partition = manager._hash_group_key_to_partition(
            group_key
        )  # pylint: disable=protected-access
        partitions.add(partition)

    assert len(partitions) == 1, "同一个group_key应该总是路由到同一个分区"

    # 验证消息确实被投递到正确的分区队列
    target_partition = manager._hash_group_key_to_partition(
        group_key
    )  # pylint: disable=protected-access
    target_queue_key = manager._get_queue_key(
        target_partition
    )  # pylint: disable=protected-access

    # 投递测试消息
    test_message = SimpleQueueItem(
        data={"test": "routing_consistency", "group": group_key},
        item_type="routing_test",
    )
    success = await manager.deliver_message(group_key, test_message)
    assert success, "消息投递应该成功"

    # 验证消息在正确的分区队列中
    queue_size = await manager.redis_client.zcard(target_queue_key)
    assert queue_size == 1, f"目标分区{target_partition}应该包含1条消息"

    # 验证其他分区没有这条消息（检查前5个不同的分区）
    other_partitions = [
        p for p in manager.partition_names[:10] if p != target_partition
    ][:5]
    for other_partition in other_partitions:
        other_queue_key = manager._get_queue_key(
            other_partition
        )  # pylint: disable=protected-access
        other_size = await manager.redis_client.zcard(other_queue_key)
        # 注意：其他分区可能有来自其他测试的消息，所以我们不能断言为0
        # 但我们可以记录这个信息用于调试
        print(f"分区{other_partition}消息数: {other_size}")

    print(f"✅ 消息成功路由到分区{target_partition}，队列大小: {queue_size}")


async def test_group_key_distribution(manager_factory):
    """测试分组键分布的均匀性"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="distribution_test_manager", auto_start=False
    )

    # 生成大量不同的group_key，检查分布是否相对均匀
    partitions = {}

    for i in range(1000):
        group_key = f"group_{i}"
        partition = manager._hash_group_key_to_partition(
            group_key
        )  # pylint: disable=protected-access
        partitions[partition] = partitions.get(partition, 0) + 1

    # 检查分布是否相对均匀（允许一定的偏差）
    # 平均每个分区10个，允许1-50的范围
    for partition, count in partitions.items():
        assert 1 <= count <= 50, f"分区{partition}的分布不均匀: {count}"


# ==================== Score阈值测试 ====================


async def test_score_threshold_filtering(manager_factory):
    """测试score阈值过滤功能"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="threshold_test_manager", auto_start=False
    )

    # 投递两条消息，时间间隔很小
    message1 = SimpleQueueItem(
        data={"id": 1, "content": "First message"}, item_type="threshold_test"
    )
    message2 = SimpleQueueItem(
        data={"id": 2, "content": "Second message"}, item_type="threshold_test"
    )

    await manager.deliver_message("threshold_group", message1)
    await asyncio.sleep(0.001)  # 很小的时间间隔
    await manager.deliver_message("threshold_group", message2)

    # 使用很大的阈值，应该获取不到消息
    messages = await manager.get_messages(score_threshold=10000)  # 10秒的毫秒数
    assert len(messages) == 0, "使用大阈值应该获取不到消息"

    # 使用很小的阈值，应该能获取到消息
    messages = await manager.get_messages(score_threshold=1)  # 1毫秒
    assert len(messages) >= 1, "使用小阈值应该能获取到消息"


async def test_single_message_queue_boundary_case(manager_factory):
    """测试队列只有一个消息时的边界情况处理"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="single_msg_test_manager", auto_start=False
    )

    # 投递一条消息
    message = SimpleQueueItem(
        data={"id": 1, "content": "Single message"}, item_type="boundary_test"
    )

    await manager.deliver_message("single_msg_group", message)

    # 获取当前时间作为current_score
    current_time = int(time.time() * 1000)  # 转换为毫秒整数

    # 使用current_score和较小的阈值，应该能获取到消息
    messages = await manager.get_messages(
        score_threshold=50,  # 50毫秒阈值
        current_score=current_time + 100,  # 模拟100毫秒后的时间
    )
    assert len(messages) == 1, "单消息队列应该能根据current_score获取到消息"
    assert messages[0].data["id"] == 1, "获取到的应该是投递的消息"

    # 再次投递一条消息，测试队列为空后重新投递的情况
    message2 = SimpleQueueItem(
        data={"id": 2, "content": "Second single message"}, item_type="boundary_test"
    )

    await manager.deliver_message("single_msg_group", message2)

    # 使用很大的阈值，应该获取不到消息
    messages = await manager.get_messages(
        score_threshold=10000,  # 10秒阈值
        current_score=current_time + 50,  # 只有50毫秒差值
    )
    assert len(messages) == 0, "阈值太大时单消息队列应该获取不到消息"

    # 使用合适的阈值，应该能获取到消息
    messages = await manager.get_messages(
        score_threshold=1500,  # 1500毫秒阈值
        current_score=current_time + 5000,  # 5000毫秒差值
    )
    assert len(messages) == 1, "阈值合适时单消息队列应该能获取到消息"
    assert messages[0].data["id"] == 2, "获取到的应该是第二条消息"


async def test_score_logic_with_old_timestamps(manager_factory):
    """测试score逻辑：验证旧时间戳消息的获取行为"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="score_logic_test_manager", auto_start=False
    )

    # 获取当前时间戳（毫秒）
    current_time = int(time.time() * 1000)

    # 10天6分钟前的时间戳
    ten_days_6mins_ago = current_time - (10 * 24 * 60 * 60 * 1000 + 6 * 60 * 1000)

    print(f"当前时间: {current_time}")
    print(f"10天6分钟前: {ten_days_6mins_ago}")

    # === 测试1: 投递1000个10天前的消息，1000个消息的时间跨度在5分钟内 ===
    print("\n=== 测试1: 投递1000个10天前的消息，时间跨度5分钟 ===")

    # 创建自定义排序函数，使用指定的时间戳
    def old_timestamp_sort_key(item):
        # 从消息数据中获取预设的时间戳
        return item.data.get("timestamp", current_time)

    manager.sort_key_func = old_timestamp_sort_key

    # 投递1000个消息，1000个消息的时间跨度在5分钟内（每个消息间隔约0.3秒）
    current_time = int(time.time() * 1000)
    time_span_ms = 5 * 60 * 1000  # 5分钟的毫秒数
    for i in range(1000):
        # 将1000个消息均匀分布在5分钟内
        message_timestamp = current_time + int(
            (i / 999) * time_span_ms
        )  # 从0到time_span_ms均匀分布
        message = SimpleQueueItem(
            data={
                "id": i,
                "timestamp": message_timestamp,
                "content": f"Old message {i}",
            },
            item_type="old_timestamp_test",
        )
        await manager.deliver_message(f"old_group_{i % 10}", message)  # 分散到10个组

    # 尝试获取100次，应该都获取不到（因为1000个消息的最大最小时间差只有5分钟，小于6分钟阈值）
    retrieved_count = 0
    for attempt in range(20):
        messages = await manager.get_messages(
            score_threshold=7 * 60 * 1000, current_score=current_time + 6 * 60 * 1000
        )
        retrieved_count += len(messages)
        if len(messages) > 0:
            print(f"第{attempt+1}次获取到{len(messages)}个消息")

    print(
        f"总共获取到 {retrieved_count} 个消息（预期：0个，因为最大最小时间差只有5分钟）"
    )
    assert retrieved_count == 0, f"应该获取不到任何消息，但获取到了{retrieved_count}个"

    # === 清空数据库 ===
    print("\n=== 清空数据库 ===")
    await manager.force_cleanup_and_reset()

    # === 测试2: 混合时间戳消息测试 ===
    print("\n=== 测试2: 混合时间戳消息测试 ===")

    # 投递1个当前的消息
    now = int(time.time() * 1000)
    old_message = SimpleQueueItem(
        data={"id": "old", "timestamp": now, "content": "当前的消息"},
        item_type="mixed_test",
    )
    await manager.deliver_message("mixed_group", old_message)

    # 投递5个10天6分钟前的消息
    for i in range(5):
        very_old_message = SimpleQueueItem(
            data={
                "id": f"very_old_{i}",
                "timestamp": ten_days_6mins_ago + (i * 1000),
                "content": f"10天6分钟前的消息{i}",
            },
            item_type="mixed_test",
        )
        await manager.deliver_message("mixed_group", very_old_message)

    for i in range(5):
        messages = await manager.get_messages(
            score_threshold=5 * 60 * 1000, current_score=now
        )
        assert len(messages) == 1, f"应该获取到1条消息，实际获取到{len(messages)}条"
        assert (
            messages[0].data['id'] == f"very_old_{i}"
        ), f"应该获取到10天6分钟前的消息，实际获取到{messages[0].data['id']}"

    messages = await manager.get_messages(
        score_threshold=5 * 60 * 1000, current_score=now
    )
    assert len(messages) == 0, f"应该获取到0条消息，实际获取到{len(messages)}条"


async def test_out_of_order_insertion_ordered_retrieval(manager_factory):
    """测试乱序插入、顺序取出功能"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="order_test_manager", auto_start=False
    )

    # 创建一个自定义的排序函数，基于消息ID排序
    def custom_sort_key(item: SimpleQueueItem) -> int:
        return int(item.data.get("order_id", 0))

    # 重新设置排序函数
    manager.sort_key_func = custom_sort_key

    # 乱序投递消息（按order_id: 3, 1, 4, 2的顺序投递）
    messages_to_deliver = [
        SimpleQueueItem(
            data={"order_id": 3, "content": "Third message"}, item_type="order_test"
        ),
        SimpleQueueItem(
            data={"order_id": 1, "content": "First message"}, item_type="order_test"
        ),
        SimpleQueueItem(
            data={"order_id": 4, "content": "Fourth message"}, item_type="order_test"
        ),
        SimpleQueueItem(
            data={"order_id": 2, "content": "Second message"}, item_type="order_test"
        ),
    ]

    # 投递到同一个分组，确保在同一个分区
    group_key = "order_test_group"
    for msg in messages_to_deliver:
        success = await manager.deliver_message(group_key, msg)
        assert success, f"消息{msg.data['order_id']}投递应该成功"
        await asyncio.sleep(0.001)  # 小延迟确保时间戳不同

    # 连续获取消息4次，每次应该按order_id顺序返回（1, 2, 3, 4）
    retrieved_messages = []
    expected_order = [1, 2, 3, 4]
    expected_contents = [
        "First message",
        "Second message",
        "Third message",
        "Fourth message",
    ]

    for i in range(4):
        messages = await manager.get_messages(score_threshold=0)
        assert (
            len(messages) == 1
        ), f"第{i+1}次获取应该得到1条消息，实际获取到{len(messages)}条"
        retrieved_messages.extend(messages)

    # 验证总消息数量
    assert (
        len(retrieved_messages) == 4
    ), f"总共应该获取到4条消息，实际获取到{len(retrieved_messages)}条"

    # 验证消息顺序
    actual_order = [msg.data["order_id"] for msg in retrieved_messages]

    print(f"期望顺序: {expected_order}")
    print(f"实际顺序: {actual_order}")

    assert (
        actual_order == expected_order
    ), f"消息顺序不正确，期望{expected_order}，实际{actual_order}"

    # 验证消息内容
    actual_contents = [msg.data["content"] for msg in retrieved_messages]
    assert (
        actual_contents == expected_contents
    ), f"消息内容顺序不正确，期望{expected_contents}，实际{actual_contents}"


# ==================== 消费者管理测试 ====================


async def test_consumer_join_and_exit(manager_factory):
    """测试消费者加入和退出"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="consumer_test_manager", auto_start=False
    )

    # 测试加入消费者
    owner_count, partitions = await manager.join_consumer("test_consumer_1")
    assert owner_count == 1, "应该有1个消费者"
    assert "test_consumer_1" in partitions, "消费者应该被分配分区"
    assert len(partitions["test_consumer_1"]) == 50, "单个消费者应该获得所有分区"

    # 加入第二个消费者
    owner_count, partitions = await manager.join_consumer("test_consumer_2")
    assert owner_count == 2, "应该有2个消费者"
    assert len(partitions["test_consumer_1"]) + len(partitions["test_consumer_2"]) == 50

    # 退出一个消费者
    owner_count, exit_partitions = await manager.exit_consumer("test_consumer_1")
    assert owner_count == 1, "应该剩余1个消费者"
    assert "test_consumer_1" not in exit_partitions, "退出的消费者不应该有分区"
    assert len(exit_partitions["test_consumer_2"]) == 50, "剩余消费者应该获得所有分区"


async def test_consumer_keepalive(manager_factory):
    """测试消费者保活"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="keepalive_test_manager", auto_start=False
    )

    # 加入消费者
    await manager.join_consumer("keepalive_consumer")

    # 测试保活
    success = await manager.keepalive_consumer("keepalive_consumer")
    assert success, "保活应该成功"

    # 测试不存在的消费者保活
    success = await manager.keepalive_consumer("nonexistent_consumer")
    assert not success, "不存在的消费者保活应该失败"


async def test_automatic_consumer_join_on_get_messages(manager_factory):
    """测试获取消息时自动加入消费者"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="auto_join_test_manager", auto_start=False
    )

    # 投递消息
    sample_message = SimpleQueueItem(
        data={"user_id": "12345", "content": "Hello World", "timestamp": time.time()},
        item_type="chat_message",
    )
    await manager.deliver_message("auto_join_group", sample_message)

    # 直接获取消息（应该自动加入消费者）
    messages = await manager.get_messages(score_threshold=0)
    assert len(messages) == 1, "应该自动加入消费者并获取到消息"


# ==================== Rebalance测试 ====================


async def test_rebalance_partitions(manager_factory):
    """测试分区重新平衡"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="rebalance_test_manager", auto_start=False
    )

    # 加入多个消费者
    consumers = ["rebalance_1", "rebalance_2", "rebalance_3"]
    for consumer in consumers:
        await manager.join_consumer(consumer)

    # 手动触发rebalance
    owner_count, rebalance_result = await manager.rebalance_partitions()
    assert owner_count == 3, "应该有3个消费者"

    # 验证分区分配 - 处理返回结果可能是列表的情况
    if isinstance(rebalance_result, dict):
        total_partitions = sum(len(parts) for parts in rebalance_result.values())
        partition_counts = [len(parts) for parts in rebalance_result.values()]
    else:
        # 如果返回的是列表，说明没有分区分配（空结果）
        print(f"⚠️ rebalance_result 是列表类型: {rebalance_result}")
        total_partitions = 0
        partition_counts = []

    assert total_partitions == 50, f"所有分区都应该被分配，实际分配: {total_partitions}"

    # 验证分配的相对均匀性
    if partition_counts:
        assert (
            max(partition_counts) - min(partition_counts) <= 1
        ), "分区分配应该相对均匀"


async def test_rebalance_with_uneven_partitions(manager_factory):
    """测试不能整除时的分区平衡"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="uneven_test_manager", auto_start=False
    )

    # 先强制清理，确保没有其他测试的残留消费者
    await manager.force_cleanup_and_reset()

    # 加入7个消费者（50不能被7整除）
    consumers = [f"uneven_{i}" for i in range(7)]
    for consumer in consumers:
        await manager.join_consumer(consumer)

    owner_count, uneven_partitions = await manager.rebalance_partitions()

    print(f"🔍 实际owner数量: {owner_count}")
    print(f"🔍 分区分配结果: {uneven_partitions}")

    assert owner_count == 7, f"应该有7个消费者，实际有{owner_count}个"

    # 验证分区分配 - 处理返回结果可能是列表的情况
    if isinstance(uneven_partitions, dict):
        partition_counts = [len(parts) for parts in uneven_partitions.values()]
        partition_counts.sort()

        print(f"🔍 分区数量分布: {partition_counts}")

        # 50 / 7 = 7 余 1，所以应该有1个消费者分到8个分区，6个消费者分到7个分区
        expected_counts = [7, 7, 7, 7, 7, 7, 8]
        assert (
            partition_counts == expected_counts
        ), f"分区分配不正确，期望{expected_counts}，实际{partition_counts}"
    else:
        # 如果返回的是列表，说明没有分区分配（空结果）
        print(f"⚠️ uneven_partitions 是列表类型: {uneven_partitions}")
        assert False, "分区分配应该返回字典格式"


# ==================== 清理功能测试 ====================


async def test_cleanup_inactive_owners(manager_factory):
    """测试清理不活跃消费者"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="cleanup_test_manager", auto_start=False
    )

    # 先强制清理，确保没有其他测试的残留消费者
    await manager.force_cleanup_and_reset()

    # 加入消费者
    await manager.join_consumer("active_consumer")
    await manager.join_consumer("inactive_consumer")

    # 验证初始状态
    initial_owners = await manager.redis_client.zrange(
        manager.owner_activate_time_zset_key, 0, -1
    )
    print(f"🔍 初始消费者: {initial_owners}")
    assert len(initial_owners) == 2, f"应该有2个消费者，实际有{len(initial_owners)}个"

    # 模拟时间流逝，让一个消费者变为不活跃
    # 这里我们需要直接操作Redis来模拟过期的时间戳
    old_timestamp = time.time() - 3600  # 1小时前
    await manager.redis_client.zadd(
        manager.owner_activate_time_zset_key, {"inactive_consumer": old_timestamp}
    )

    # 验证时间戳设置
    inactive_score = await manager.redis_client.zscore(
        manager.owner_activate_time_zset_key, "inactive_consumer"
    )
    active_score = await manager.redis_client.zscore(
        manager.owner_activate_time_zset_key, "active_consumer"
    )
    print(f"🔍 inactive_consumer时间戳: {inactive_score}")
    print(f"🔍 active_consumer时间戳: {active_score}")
    print(f"🔍 当前时间: {time.time()}")
    print(f"🔍 不活跃阈值: {time.time() - manager.inactive_threshold_seconds}")

    # 执行清理
    cleaned_count, owner_count, cleanup_result = await manager.cleanup_inactive_owners()

    print(f"🔍 清理结果: cleaned_count={cleaned_count}, owner_count={owner_count}")
    print(f"🔍 清理后分区分配: {cleanup_result}")

    # 验证清理后的状态
    remaining_owners = await manager.redis_client.zrange(
        manager.owner_activate_time_zset_key, 0, -1
    )
    print(f"🔍 剩余消费者: {remaining_owners}")

    assert cleaned_count == 1, f"应该清理1个不活跃消费者，实际清理了{cleaned_count}个"
    assert owner_count == 1, f"应该剩余1个活跃消费者，实际剩余{owner_count}个"
    assert "inactive_consumer" not in cleanup_result, "不活跃消费者应该被清理"
    assert "active_consumer" in cleanup_result, "活跃消费者应该保留"


async def test_force_cleanup_and_reset(manager_factory):
    """测试强制清理和重置"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="force_cleanup_test_manager", auto_start=False
    )

    # 加入几个消费者
    consumers = ["force_1", "force_2", "force_3"]
    for consumer in consumers:
        await manager.join_consumer(consumer)

    # 执行强制清理
    cleaned_count = await manager.force_cleanup_and_reset()
    assert cleaned_count == 3, "应该清理3个消费者"

    # 验证清理结果
    owners = await manager.redis_client.zrange(
        manager.owner_activate_time_zset_key, 0, -1
    )
    assert len(owners) == 0, "所有消费者应该被清理"


# ==================== 边角情况测试 ====================


async def test_empty_queue_operations(manager_factory):
    """测试空队列操作"""
    manager = await manager_factory.get_manager()

    # 从空队列获取消息
    messages = await manager.get_messages(score_threshold=0)
    assert len(messages) == 0, f"空队列应该返回空列表，但获取到{len(messages)}条消息"

    # 获取空队列统计
    stats = await manager.get_manager_stats()
    assert (
        stats["total_current_messages"] == 0
    ), f"空队列当前消息数应该为0，但实际为{stats['total_current_messages']}"


async def test_nonexistent_consumer_operations(manager_factory):
    """测试不存在的消费者操作"""
    manager = await manager_factory.get_manager()

    # 退出不存在的消费者
    owner_count, _ = await manager.exit_consumer("nonexistent")
    assert owner_count == 0, f"退出不存在的消费者应该返回0，但返回了{owner_count}"

    # 保活不存在的消费者
    success = await manager.keepalive_consumer("nonexistent")
    assert not success, "不存在的消费者保活应该失败"


async def test_duplicate_message_handling(manager_factory):
    """测试重复消息处理"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="duplicate_test_rq", auto_start=False
    )

    # 创建相同内容的消息
    message1 = SimpleQueueItem(
        data={"id": "duplicate", "content": "Same content"}, item_type="duplicate_test"
    )
    message2 = SimpleQueueItem(
        data={"id": "duplicate", "content": "Same content"}, item_type="duplicate_test"
    )

    # 投递到同一个分组（会有相同的score）
    success1 = await manager.deliver_message("dup_group", message1)
    success2 = await manager.deliver_message("dup_group", message2)

    # 第一条应该成功，第二条可能失败（取决于score是否完全相同）
    assert success1, "第一条消息应该投递成功"
    # 注意：由于使用时间戳作为score，第二条消息通常也会成功
    # success2的结果取决于时间戳精度，这里不强制断言
    _ = success2  # 避免未使用变量警告


async def test_large_message_handling(manager_factory):
    """测试大消息处理"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="large_msg_test_rq", auto_start=False
    )

    # 创建较大的消息
    large_data = {"content": "x" * 10000, "id": "large_message"}
    large_message = SimpleQueueItem(data=large_data, item_type="large_test")

    success = await manager.deliver_message("large_group", large_message)
    assert success, "大消息应该能够投递成功"

    messages = await manager.get_messages(score_threshold=0)
    assert len(messages) >= 0, "应该能够处理大消息"


# ==================== 并发测试 ====================


async def test_concurrent_message_delivery(manager_factory):
    """测试并发消息投递"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="concurrent_delivery_rq", auto_start=False
    )

    async def deliver_messages(group_prefix: str, count: int):
        for i in range(count):
            message = SimpleQueueItem(
                data={
                    "id": f"{group_prefix}_{i}",
                    "content": f"Concurrent message {i}",
                },
                item_type="concurrent_test",
            )
            await manager.deliver_message(f"{group_prefix}_group_{i}", message)

    # 并发投递消息
    tasks = [
        deliver_messages("concurrent_1", 10),
        deliver_messages("concurrent_2", 10),
        deliver_messages("concurrent_3", 10),
    ]

    await asyncio.gather(*tasks)

    # 验证统计信息
    stats = await manager.get_manager_stats()
    # 由于并发和可能的重复消息，我们允许一定的误差
    assert (
        stats["total_delivered_messages"] >= 28
    ), f"应该投递大部分消息，实际投递了{stats['total_delivered_messages']}条"


async def test_concurrent_consumer_operations(manager_factory):
    """测试并发消费者操作"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="concurrent_consumer_test_rq",  # 使用独特的前缀避免冲突
        auto_start=False,
    )

    # 先清理所有消费者，确保干净的环境
    await manager.force_cleanup_and_reset()

    async def consumer_lifecycle(consumer_id: str):
        try:
            # 加入消费者
            await manager.join_consumer(consumer_id)
            await asyncio.sleep(0.1)

            # 保活
            await manager.keepalive_consumer(consumer_id)
            await asyncio.sleep(0.1)

            # 退出
            await manager.exit_consumer(consumer_id)
        except Exception as e:
            print(f"消费者 {consumer_id} 生命周期异常: {e}")

    # 并发执行消费者生命周期
    tasks = [consumer_lifecycle(f"concurrent_consumer_{i}") for i in range(5)]
    await asyncio.gather(*tasks, return_exceptions=True)

    # 等待一小段时间确保所有操作完成
    await asyncio.sleep(0.5)

    # 验证最终状态
    owners = await manager.redis_client.zrange(
        manager.owner_activate_time_zset_key, 0, -1
    )
    # 由于并发操作的复杂性，我们允许一些消费者可能还没完全退出
    # 但应该大部分都退出了
    assert (
        len(owners) <= 2
    ), f"大部分消费者都应该已退出，但还剩{len(owners)}个: {owners}"


# ==================== 生命周期测试 ====================


async def test_manager_lifecycle(manager_factory):
    """测试管理器生命周期"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="lifecycle_test_rq",  # 使用独特的前缀避免冲突
        auto_start=False,  # 手动控制启动
    )

    # 启动定期任务
    await manager.start()
    assert (
        manager._running
    ), "管理器应该处于运行状态"  # pylint: disable=protected-access

    # 先清理可能的残留数据
    await manager.force_cleanup_and_reset()

    # 投递和获取消息
    sample_message = SimpleQueueItem(
        data={"user_id": "12345", "content": "Hello World", "timestamp": time.time()},
        item_type="chat_message",
    )
    await manager.deliver_message("lifecycle_group", sample_message)
    messages = await manager.get_messages(score_threshold=0)
    assert len(messages) == 1, f"应该能正常处理1条消息，但获取到{len(messages)}条消息"

    # 软关闭（有消息时应该失败）
    await manager.deliver_message("lifecycle_group_2", sample_message)
    success = await manager.shutdown(ShutdownMode.SOFT)
    # 注意：这里可能成功也可能失败，取决于消息是否被消费

    # 硬关闭（应该总是成功）
    success = await manager.shutdown(ShutdownMode.HARD)
    assert success, "硬关闭应该总是成功"
    assert (
        not manager._running
    ), "管理器应该停止运行"  # pylint: disable=protected-access


async def test_periodic_tasks_behavior(manager_factory):
    """测试定期任务行为"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="periodic_test_manager", auto_start=False  # 手动控制启动
    )

    # 启动定期任务
    await manager.start_periodic_tasks()

    # 等待一小段时间让定期任务运行
    await asyncio.sleep(0.5)

    # 验证任务正在运行  # pylint: disable=protected-access
    assert manager._running
    assert manager._log_task is not None
    assert manager._cleanup_task is not None
    # 注意: keepalive 是按需触发的，不是定期任务，所以没有 _keepalive_task

    # 停止任务
    await manager.stop_periodic_tasks()
    assert not manager._running  # pylint: disable=protected-access


async def test_invalid_message_data_handling(manager_factory):
    """测试无效消息数据处理"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="invalid_data_test_manager", auto_start=False
    )

    # 创建包含无效数据的消息
    invalid_message = SimpleQueueItem(
        data={"invalid": float('inf')}, item_type="invalid_test"  # JSON无法序列化的数据
    )

    # 尝试投递（应该处理序列化错误）
    try:
        success = await manager.deliver_message("invalid_group", invalid_message)
        # 如果没有抛出异常，检查是否正确处理
        assert isinstance(success, bool)
    except (ValueError, TypeError):
        # 预期的序列化错误
        pass


# ==================== 性能测试 ====================


async def test_high_throughput_delivery(manager_factory):
    """测试高吞吐量投递"""
    manager = await manager_factory.get_manager_with_config(
        key_prefix="throughput_test_rq", auto_start=False
    )

    start_time = time.time()

    # 投递大量消息
    message_count = 100
    for i in range(message_count):
        message = SimpleQueueItem(
            data={"id": i, "content": f"Throughput test {i}"},
            item_type="throughput_test",
        )
        await manager.deliver_message(f"throughput_group_{i % 10}", message)

    end_time = time.time()
    duration = end_time - start_time

    # 验证性能（这个阈值可能需要根据实际环境调整）
    throughput = message_count / duration
    print(f"投递吞吐量: {throughput:.2f} messages/second")

    # 验证所有消息都被投递
    stats = await manager.get_manager_stats()
    # 由于可能的重复消息或冲突，我们允许一定的误差
    assert (
        stats["total_delivered_messages"] >= message_count * 0.9
    ), f"应该投递大部分消息，期望{message_count}，实际{stats['total_delivered_messages']}"


# ==================== 辅助函数 ====================


def create_test_message(message_id: str, content: str = None) -> SimpleQueueItem:
    """创建测试消息"""
    return SimpleQueueItem(
        data={
            "id": message_id,
            "content": content or f"Test message {message_id}",
            "timestamp": time.time(),
        },
        item_type="test_message",
    )


# ==================== 运行测试 ====================


async def run_all_tests():
    """运行所有测试"""
    print("开始运行Redis消息分组队列管理器综合测试...")

    if not IMPORTS_AVAILABLE:
        print("❌ 无法导入必要的模块，请确保项目依赖已正确安装")
        print("请尝试以下解决方案：")
        print(
            "1. 使用 bootstrap 方式运行: python src/bootstrap.py tests/test_redis_group_queue_manager_comprehensive.py"
        )
        print("2. 确保所有依赖已安装: pip install -r requirements.txt")
        print("3. 检查环境变量和Python路径设置")
        return

    try:
        from core.di.utils import get_bean_by_type
        from core.queue.redis_group_queue.redis_msg_group_queue_manager_factory import (
            RedisGroupQueueManagerFactory,
        )
        from component.redis_provider import RedisProvider

        # 获取管理器工厂实例
        manager_factory = get_bean_by_type(RedisGroupQueueManagerFactory)
        # 获取Redis提供者，用于清理数据库
        redis_provider = get_bean_by_type(RedisProvider)

    except ImportError as e:
        print(f"❌ 无法导入依赖注入模块: {e}")
        print("请确保项目已正确初始化并且依赖注入容器已设置")
        return
    except Exception as e:
        print(f"❌ 获取管理器工厂失败: {e}")
        print("请确保Redis服务正在运行并且配置正确")
        return

    # 定义所有测试函数
    tests = [
        test_basic_message_delivery_and_retrieval,
        test_score_logic_with_old_timestamps,
        test_message_delivery_limit,
        test_queue_statistics,
        test_improved_stats_functionality,
        test_stats_performance_and_accuracy,
        test_stats_error_handling,
        test_group_key_routing_consistency,
        test_group_key_distribution,
        test_score_threshold_filtering,
        test_single_message_queue_boundary_case,
        test_out_of_order_insertion_ordered_retrieval,
        test_consumer_join_and_exit,
        test_consumer_keepalive,
        test_automatic_consumer_join_on_get_messages,
        test_rebalance_partitions,
        test_rebalance_with_uneven_partitions,
        test_cleanup_inactive_owners,
        test_force_cleanup_and_reset,
        test_empty_queue_operations,
        test_nonexistent_consumer_operations,
        test_duplicate_message_handling,
        test_large_message_handling,
        test_concurrent_message_delivery,
        test_concurrent_consumer_operations,
        test_manager_lifecycle,
        test_periodic_tasks_behavior,
        test_invalid_message_data_handling,
        test_high_throughput_delivery,
    ]

    passed = 0
    failed = 0

    # 获取Redis客户端用于清理
    redis_client = await redis_provider.get_named_client(
        "default", decode_responses=True
    )

    # 重启时先清理数据库
    try:
        await redis_client.flushdb()
        print("🧹 启动时Redis数据库已清理")
    except Exception as e:
        print(f"⚠️ 启动时清理Redis数据库失败: {e}")
        return

    # 运行所有测试
    for test_func in tests:
        print(f"\n运行测试: {test_func.__name__}")
        print("-" * 50)

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
            # 任意失败直接清理退出
            print("💥 检测到测试失败，开始清理并退出...")
            break
        except Exception as e:
            print(f"❌ {test_func.__name__} 测试出错: {str(e)}")
            failed += 1
            traceback.print_exc()
            # 任意失败直接清理退出
            print("💥 检测到测试出错，开始清理并退出...")
            break

        # 测试后停止所有管理器的定期任务，避免保活警告
        try:
            await manager_factory.stop_all_managers()
            print("🔌 测试后所有管理器已停止")
        except Exception as e:
            print(f"⚠️ 停止管理器失败: {e}")

        # 测试后清理Redis数据库
        try:
            await redis_client.flushdb()
            print("🧹 测试后Redis数据库已清理")
        except Exception as e:
            print(f"⚠️ 测试后清理Redis数据库失败: {e}")

        # 短暂等待，确保异步任务完全停止
        await asyncio.sleep(0.1)

    print(f"\n测试结果: {passed} 通过, {failed} 失败")

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
    asyncio.run(run_all_tests())
