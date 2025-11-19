"""
Redis消息分组队列管理器BSON序列化高级测试

基于test_redis_group_queue_complex.py，测试BSON序列化模式下的：
1. 多消费者并发投递和消费测试
2. 大数据量内存占用测试
"""

import asyncio
import sys
import os
import time
import random
import string
import json
import traceback
import base64
from typing import Set, Dict, Any

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
    from core.queue.redis_group_queue.redis_msg_group_queue_manager_factory import (
        RedisGroupQueueManagerFactory,
    )
    from core.di.utils import get_bean_by_type
    from component.redis_provider import RedisProvider

    IMPORTS_AVAILABLE = True
    print("✅ 成功导入核心模块")
except ImportError as e:
    print(f"❌ 导入失败: {e}")
    IMPORTS_AVAILABLE = False
    sys.exit(1)


def generate_random_data(size_kb: int = 1) -> Dict[str, Any]:
    """生成指定大小的随机数据"""
    # 计算需要的字符数（大约）
    target_size = size_kb * 1024

    # 生成随机字符串作为主要内容
    content_size = target_size - 200  # 预留一些空间给其他字段
    random_content = ''.join(
        random.choices(string.ascii_letters + string.digits, k=content_size)
    )

    return {
        "id": f"data_{random.randint(100000, 999999)}",
        "content": random_content,
        "timestamp": time.time(),
        "metadata": {
            "type": "test_data",
            "size_kb": size_kb,
            "generated_at": time.time(),
        },
    }


def generate_random_group_key() -> str:
    """生成随机分组键"""
    return f"group_{random.randint(1, 1000)}"


def generate_binary_data(size_bytes: int = 1024) -> bytes:
    """生成指定大小的随机二进制数据"""
    return bytes([random.randint(0, 255) for _ in range(size_bytes)])


async def bson_producer_worker(
    manager,
    producer_id: int,
    target_count: int,
    delivered_ids: Set[str],
    delay_range: tuple = (0.01, 0.1),
):
    """
    BSON生产者工作协程

    Args:
        manager: 队列管理器实例（BSON模式）
        producer_id: 生产者ID
        target_count: 目标投递数量
        delivered_ids: 已投递消息ID集合（线程安全）
        delay_range: 延迟范围（秒）
    """
    delivered_count = 0

    print(f"🚀 BSON生产者 {producer_id} 开始工作，目标投递 {target_count} 条消息")

    while delivered_count < target_count:
        try:
            # 创建消息，包含二进制数据
            message_id = f"bson_producer_{producer_id}_msg_{delivered_count + 1}_{int(time.time() * 1000000)}"

            # 生成一些二进制数据并编码为base64
            binary_data = generate_binary_data(256)  # 256字节的二进制数据
            encoded_binary = base64.b64encode(binary_data).decode('utf-8')

            message = SimpleQueueItem(
                data={
                    "producer_id": producer_id,
                    "message_id": message_id,
                    "sequence": delivered_count + 1,
                    "content": f"BSON Message from producer {producer_id}, sequence {delivered_count + 1}",
                    "timestamp": time.time(),
                    "binary_payload": encoded_binary,
                    "metadata": {
                        "serialization": "bson",
                        "binary_size": len(binary_data),
                        "unicode_test": f"中文测试{producer_id}🚀",
                    },
                },
                item_type="bson_test_message",
            )

            # 随机选择分组
            group_key = generate_random_group_key()

            # 尝试投递
            success = await manager.deliver_message(group_key, message)

            if success:
                delivered_ids.add(message_id)
                delivered_count += 1

                if delivered_count % 50 == 0:
                    print(
                        f"📤 BSON生产者 {producer_id} 已投递 {delivered_count}/{target_count} 条消息"
                    )
            else:
                # 投递失败，可能是达到了上限，稍等一下再试
                await asyncio.sleep(random.uniform(0.1, 0.5))

            # 随机延迟，模拟断断续续的投递
            await asyncio.sleep(random.uniform(*delay_range))

        except Exception as e:
            print(f"❌ BSON生产者 {producer_id} 投递消息时出错: {e}")
            await asyncio.sleep(0.1)

    print(f"✅ BSON生产者 {producer_id} 完成工作，实际投递 {delivered_count} 条消息")


async def bson_consumer_worker(
    manager,
    consumer_id: int,
    consumed_ids: Set[str],
    consumer_consumed_ids: Dict[str, Set[str]],
    stop_event: asyncio.Event,
    owner_id: str,
    rebalance_probability: float = 0.05,
):
    """
    BSON消费者工作协程

    Args:
        manager: 队列管理器实例（BSON模式）
        consumer_id: 消费者ID
        consumed_ids: 全局已消费消息ID集合（线程安全）
        consumer_consumed_ids: 每个消费者的消费ID集合
        stop_event: 停止事件
        owner_id: 消费者的owner_id
        rebalance_probability: 每次循环触发rebalance的概率
    """
    consumed_count = 0
    rebalance_count = 0
    binary_verification_count = 0

    # 先加入消费者
    await manager.join_consumer(owner_id)
    print(f"🔄 BSON消费者 {consumer_id} (owner_id: {owner_id}) 已加入并开始工作")

    # 初始化该消费者的消费ID集合
    if owner_id not in consumer_consumed_ids:
        consumer_consumed_ids[owner_id] = set()

    while not stop_event.is_set():
        try:
            # 随机触发rebalance
            if random.random() < rebalance_probability:
                try:
                    result = await manager.rebalance_partitions()
                    rebalance_count += 1
                    print(
                        f"🔄 BSON消费者 {consumer_id} 触发第 {rebalance_count} 次 rebalance，结果: {result}"
                    )
                except Exception as rebalance_error:
                    print(
                        f"⚠️ BSON消费者 {consumer_id} rebalance失败: {rebalance_error}"
                    )

            # 获取消息（使用指定的owner_id）
            messages = await manager.get_messages(score_threshold=0, owner_id=owner_id)

            if messages:
                for message in messages:
                    # 从消息数据中提取message_id
                    if hasattr(message, 'data') and isinstance(message.data, dict):
                        message_id = message.data.get("message_id")
                        if message_id:
                            consumed_ids.add(message_id)
                            consumer_consumed_ids[owner_id].add(message_id)
                            consumed_count += 1

                            # 验证二进制数据完整性（随机抽样）
                            if random.random() < 0.1:  # 10%的概率验证
                                try:
                                    binary_payload = message.data.get("binary_payload")
                                    if binary_payload:
                                        decoded_binary = base64.b64decode(
                                            binary_payload
                                        )
                                        expected_size = message.data.get(
                                            "metadata", {}
                                        ).get("binary_size", 0)
                                        if len(decoded_binary) == expected_size:
                                            binary_verification_count += 1
                                except Exception as verify_error:
                                    print(
                                        f"⚠️ BSON消费者 {consumer_id} 二进制数据验证失败: {verify_error}"
                                    )

                if consumed_count % 50 == 0:
                    print(
                        f"📥 BSON消费者 {consumer_id} 已消费 {consumed_count} 条消息 "
                        f"(二进制验证: {binary_verification_count})"
                    )
            else:
                # 没有消息，短暂等待
                await asyncio.sleep(0.1)

        except Exception as e:
            print(f"❌ BSON消费者 {consumer_id} 消费消息时出错: {e}")
            await asyncio.sleep(0.1)

    print(
        f"✅ BSON消费者 {consumer_id} (owner_id: {owner_id}) 停止工作，"
        f"总共消费 {consumed_count} 条消息，触发 {rebalance_count} 次 rebalance，"
        f"二进制验证 {binary_verification_count} 次"
    )


async def test_bson_concurrent_producers_consumers():
    """
    测试1：BSON模式下多个消费者并发投递和消费测试
    """
    print("\n" + "=" * 80)
    print("🧪 开始测试：BSON模式多消费者并发投递和消费")
    print("=" * 80)

    try:
        # 获取管理器工厂和Redis提供者
        manager_factory = get_bean_by_type(RedisGroupQueueManagerFactory)
        redis_provider = get_bean_by_type(RedisProvider)

        # 清理Redis数据库
        redis_client = await redis_provider.get_named_client(
            "default", decode_responses=True
        )
        await redis_client.flushdb()
        print("🧹 Redis数据库已清理")

        # 创建BSON模式的测试管理器（限制1000条消息）
        test_manager = await manager_factory.get_manager_with_config(
            key_prefix="bson_concurrent_test",
            serialization_mode=SerializationMode.BSON,
            max_total_messages=1000,
            auto_start=True,
        )

        # 清理测试数据
        await test_manager.force_cleanup_and_reset()

        # 共享数据结构（使用set存储消息ID）
        delivered_ids: Set[str] = set()
        consumed_ids: Set[str] = set()
        consumer_consumed_ids: Dict[str, Set[str]] = {}

        # 配置参数
        num_producers = 5  # 5个生产者
        num_consumers = 3  # 3个消费者
        messages_per_producer = 200  # 每个生产者投递200条（总共1000条）

        print("📋 BSON测试配置:")
        print(f"   - 序列化模式: BSON")
        print(f"   - 生产者数量: {num_producers}")
        print(f"   - 消费者数量: {num_consumers}")
        print(f"   - 每个生产者目标投递: {messages_per_producer}")
        print("   - 最大总消息数: 1000")
        print("   - 包含二进制数据验证")

        # 创建停止事件
        stop_event = asyncio.Event()

        # 启动消费者（每个消费者使用不同的owner_id）
        consumer_tasks = []
        rebalance_probabilities = [0.03, 0.05, 0.07]
        for i in range(num_consumers):
            owner_id = f"bson_consumer_{i + 1}_{int(time.time() * 1000)}"
            rebalance_prob = rebalance_probabilities[i % len(rebalance_probabilities)]
            task = asyncio.create_task(
                bson_consumer_worker(
                    test_manager,
                    i + 1,
                    consumed_ids,
                    consumer_consumed_ids,
                    stop_event,
                    owner_id,
                    rebalance_probability=rebalance_prob,
                )
            )
            consumer_tasks.append(task)

        # 等待消费者启动和加入
        await asyncio.sleep(2)

        # 启动生产者
        producer_tasks = []
        for i in range(num_producers):
            task = asyncio.create_task(
                bson_producer_worker(
                    test_manager,
                    i + 1,
                    messages_per_producer,
                    delivered_ids,
                    delay_range=(0.01, 0.05),
                )
            )
            producer_tasks.append(task)

        # 等待所有生产者完成
        print("⏳ 等待所有BSON生产者完成...")
        await asyncio.gather(*producer_tasks)

        print(f"📊 BSON生产阶段完成，已投递消息数: {len(delivered_ids)}")

        # 等待消费者消费完所有消息
        print("⏳ 等待BSON消费者消费完所有消息...")
        max_wait_time = 200  # 最多等待200秒
        wait_start = time.time()

        while (
            len(consumed_ids) < len(delivered_ids)
            and (time.time() - wait_start) < max_wait_time
        ):
            await asyncio.sleep(1)
            print(f"📈 BSON消费进度: {len(consumed_ids)}/{len(delivered_ids)}")
            if len(consumed_ids) >= len(delivered_ids):
                break

        # 停止消费者
        stop_event.set()
        await asyncio.gather(*consumer_tasks, return_exceptions=True)

        # 验证结果
        print("\n📊 BSON测试结果统计:")
        print(f"   - 投递消息数: {len(delivered_ids)}")
        print(f"   - 消费消息数: {len(consumed_ids)}")
        print(f"   - 投递ID集合大小: {len(delivered_ids)}")
        print(f"   - 消费ID集合大小: {len(consumed_ids)}")

        # 检查ID集合是否相等
        missing_in_consumed = delivered_ids - consumed_ids
        extra_in_consumed = consumed_ids - delivered_ids

        print(f"   - 投递但未消费的消息: {len(missing_in_consumed)}")
        print(f"   - 消费但未投递的消息: {len(extra_in_consumed)}")

        if missing_in_consumed:
            print(f"   - 缺失的消息ID示例: {list(missing_in_consumed)[:5]}")

        if extra_in_consumed:
            print(f"   - 多余的消息ID示例: {list(extra_in_consumed)[:5]}")

        # 验证每个消费者的消费情况
        print("\n📊 各BSON消费者消费统计:")
        total_consumer_messages = 0
        all_consumer_ids = set()

        for owner_id, ids in consumer_consumed_ids.items():
            print(f"   - {owner_id}: {len(ids)} 条消息")
            total_consumer_messages += len(ids)
            all_consumer_ids.update(ids)

        # 验证消费者之间没有重复处理消息
        overlap_count = total_consumer_messages - len(all_consumer_ids)
        print(f"   - 消费者间重复处理的消息数: {overlap_count}")

        # 验证分区间消息ID不重合
        partition_overlap = len(consumed_ids) - len(all_consumer_ids)
        print(f"   - 分区间重复的消息数: {partition_overlap}")

        # 断言验证
        assert len(delivered_ids) > 0, "应该有投递的消息"
        assert len(consumed_ids) > 0, "应该有消费的消息"
        assert (
            delivered_ids == consumed_ids
        ), f"投递ID集合应该等于消费ID集合，投递={len(delivered_ids)}, 消费={len(consumed_ids)}"
        assert (
            overlap_count == 0
        ), f"消费者之间不应该重复处理消息，重复数: {overlap_count}"
        assert (
            partition_overlap == 0
        ), f"分区间不应该有重复消息，重复数: {partition_overlap}"

        print("✅ BSON测试通过：投递ID集合 = 消费ID集合，且各消费者/分区间无重复")

        # 清理
        await test_manager.shutdown()
        await test_manager.force_cleanup_and_reset()
        await manager_factory.stop_all_managers()
        await redis_client.flushdb()
        await redis_client.close()

    except Exception as e:
        print(f"❌ BSON并发测试失败: {e}")
        print(f"错误详情: {traceback.format_exc()}")
        raise


async def test_bson_large_data_memory_usage():
    """
    测试2：BSON模式下大数据量内存占用测试
    """
    print("\n" + "=" * 80)
    print("🧪 开始测试：BSON模式大数据量内存占用")
    print("=" * 80)

    try:
        # 获取管理器工厂和Redis提供者
        manager_factory = get_bean_by_type(RedisGroupQueueManagerFactory)
        redis_provider = get_bean_by_type(RedisProvider)

        # 获取Redis客户端用于内存统计
        redis_client = await redis_provider.get_named_client(
            "default", decode_responses=True
        )
        await redis_client.flushdb()

        # 创建BSON模式的测试管理器（允许大量消息）
        test_manager = await manager_factory.get_manager_with_config(
            key_prefix="bson_memory_test",
            serialization_mode=SerializationMode.BSON,
            max_total_messages=250000,  # 允许25万条消息
            auto_start=True,
        )

        # 清理测试数据
        await test_manager.force_cleanup_and_reset()

        # 获取初始内存使用情况
        initial_info = await redis_client.info("memory")
        initial_memory = initial_info.get("used_memory", 0)
        initial_memory_human = initial_info.get("used_memory_human", "0B")

        print(f"📊 初始Redis内存使用: {initial_memory_human} ({initial_memory} bytes)")

        # 配置参数（相比JSON版本，减少数量因为BSON数据更大）
        total_messages = 100000  # 10万条消息（BSON数据更大）
        message_size_kb = 1  # 每条消息1KB
        batch_size = 1000  # 批量投递大小
        binary_data_size = 512  # 每条消息包含512字节二进制数据

        print("📋 BSON测试配置:")
        print(f"   - 序列化模式: BSON")
        print(f"   - 总消息数: {total_messages:,}")
        print(f"   - 每条消息大小: {message_size_kb}KB")
        print(f"   - 二进制数据大小: {binary_data_size} bytes")
        print(f"   - 批量投递大小: {batch_size}")
        print(f"   - 预计数据量: {total_messages * message_size_kb / 1024:.1f}MB")

        # 生成并投递消息
        delivered_count = 0
        failed_count = 0
        start_time = time.time()

        print("🚀 开始生成和投递BSON消息...")

        for batch_start in range(0, total_messages, batch_size):
            batch_end = min(batch_start + batch_size, total_messages)
            batch_tasks = []

            # 创建批量投递任务
            for i in range(batch_start, batch_end):
                # 生成随机数据
                random_data = generate_random_data(message_size_kb)
                message_id = f"bson_large_msg_{i:06d}_{int(time.time() * 1000000)}"

                # 生成二进制数据
                binary_data = generate_binary_data(binary_data_size)
                encoded_binary = base64.b64encode(binary_data).decode('utf-8')

                # 随机分组
                group_key = generate_random_group_key()
                message = SimpleQueueItem(
                    data={
                        "message_id": message_id,
                        "sequence": i,
                        "group_key": group_key,
                        "payload": random_data,
                        "binary_data": encoded_binary,
                        "timestamp": time.time(),
                        "metadata": {
                            "serialization": "bson",
                            "binary_size": len(binary_data),
                            "unicode_test": f"BSON大数据测试{i}🔥",
                            "nested_data": {
                                "level1": {"level2": {"value": f"nested_{i}"}},
                                "array": [1, 2, 3, f"item_{i}"],
                                "boolean": i % 2 == 0,
                                "null_field": None,
                            },
                        },
                    },
                    item_type="bson_large_test_message",
                )

                # 创建投递任务
                task = test_manager.deliver_message(group_key, message)
                batch_tasks.append(task)

            # 并发执行批量投递
            results = await asyncio.gather(*batch_tasks, return_exceptions=True)

            # 统计结果
            for result in results:
                if isinstance(result, Exception):
                    failed_count += 1
                elif result:
                    delivered_count += 1
                else:
                    failed_count += 1

            # 进度报告
            if (batch_end) % 10000 == 0:
                elapsed = time.time() - start_time
                rate = delivered_count / elapsed if elapsed > 0 else 0
                print(
                    f"📈 BSON进度: {batch_end:,}/{total_messages:,} "
                    f"(成功: {delivered_count:,}, 失败: {failed_count:,}, "
                    f"速率: {rate:.0f} msg/s)"
                )

        total_time = time.time() - start_time

        print("\n📊 BSON投递完成统计:")
        print(f"   - 总耗时: {total_time:.2f}秒")
        print(f"   - 成功投递: {delivered_count:,}")
        print(f"   - 失败投递: {failed_count:,}")
        print(f"   - 平均速率: {delivered_count / total_time:.0f} msg/s")

        # 获取投递后的内存使用情况
        final_info = await redis_client.info("memory")
        final_memory = final_info.get("used_memory", 0)
        final_memory_human = final_info.get("used_memory_human", "0B")

        # 计算内存增长
        memory_increase = final_memory - initial_memory
        memory_increase_mb = memory_increase / (1024 * 1024)

        print("\n💾 BSON内存使用统计:")
        print(f"   - 初始内存: {initial_memory_human} ({initial_memory:,} bytes)")
        print(f"   - 最终内存: {final_memory_human} ({final_memory:,} bytes)")
        print(f"   - 内存增长: {memory_increase_mb:.2f}MB ({memory_increase:,} bytes)")

        if delivered_count > 0:
            avg_memory_per_msg = memory_increase / delivered_count
            print(f"   - 平均每条消息内存开销: {avg_memory_per_msg:.2f} bytes")
            print(
                f"   - 内存效率: {(message_size_kb * 1024) / avg_memory_per_msg * 100:.1f}%"
            )

        # 获取队列统计信息
        stats = await test_manager.get_stats(
            include_all_partitions=True, include_partition_details=True
        )
        total_queue_size = stats.get("actual_messages_in_queues", 0)

        print("\n📋 BSON队列统计:")
        print(f"   - 队列中消息总数: {total_queue_size:,}")
        print(f"   - 分区数量: {stats.get('total_queues', 0)}")
        print(f"   - 非空分区数量: {stats.get('non_empty_partitions', 0)}")
        print(f"   - 最大分区大小: {stats.get('max_partition_size', 0)}")
        print(f"   - 最小分区大小: {stats.get('min_partition_size', 0)}")

        # 验证结果
        assert delivered_count > 0, "应该有成功投递的消息"
        assert memory_increase > 0, "内存使用应该有增长"

        print("✅ BSON大数据量内存占用测试完成")

        # BSON特有：单个消费者测试，验证二进制数据完整性
        print("\n" + "=" * 60)
        print("🧪 开始BSON单个消费者二进制数据验证测试")
        print("=" * 60)

        # 创建单个消费者
        consumer_owner_id = f"bson_single_consumer_{int(time.time() * 1000)}"
        await test_manager.join_consumer(consumer_owner_id)
        print(f"🔄 BSON消费者 {consumer_owner_id} 已加入")

        # 进行10次poll测试，每次验证二进制数据
        poll_count = 10
        target_messages_per_poll = 50
        binary_verification_success = 0
        binary_verification_total = 0

        print(f"📋 BSON消费者测试配置:")
        print(f"   - Poll次数: {poll_count}")
        print(f"   - 每次目标消息数: {target_messages_per_poll}")
        print(f"   - 包含二进制数据完整性验证")

        for poll_idx in range(poll_count):
            print(f"\n🔍 第 {poll_idx + 1}/{poll_count} 次 BSON Poll:")

            # 获取消息
            messages = await test_manager.get_messages(
                score_threshold=0, owner_id=consumer_owner_id
            )

            actual_count = len(messages)
            print(f"   - 实际获取消息数: {actual_count}")
            assert (
                actual_count == 50
            ), f"应该获取到50条消息，实际获取到{actual_count}条消息"

            # 验证消息和二进制数据
            all_group_keys = set()
            for message in messages:
                if hasattr(message, 'data') and isinstance(message.data, dict):
                    group_key = message.data["group_key"]
                    all_group_keys.add(group_key)

                    # 验证二进制数据完整性
                    try:
                        binary_data_encoded = message.data.get("binary_data")
                        if binary_data_encoded:
                            decoded_binary = base64.b64decode(binary_data_encoded)
                            expected_size = message.data.get("metadata", {}).get(
                                "binary_size", 0
                            )
                            binary_verification_total += 1
                            if len(decoded_binary) == expected_size:
                                binary_verification_success += 1
                    except Exception as verify_error:
                        print(f"⚠️ 二进制数据验证失败: {verify_error}")

            print(f"   - 批次内唯一分区键数: {len(all_group_keys)}")
            assert (
                len(all_group_keys) == 50
            ), f"应该获取到50个唯一分区键，实际获取到{len(all_group_keys)}个唯一分区键"

            # 验证分区hash
            hash_group_keys = set()
            for group_key in all_group_keys:
                hash_group_keys.add(
                    test_manager._hash_group_key_to_partition(group_key)
                )

            print(f"   - 批次内唯一hash分区数: {len(hash_group_keys)}")
            assert (
                len(hash_group_keys) == 50
            ), f"应该获取到50个唯一hash分区，实际获取到{len(hash_group_keys)}个唯一hash分区"

        # 二进制数据验证结果
        print(f"\n📊 BSON二进制数据验证结果:")
        print(f"   - 验证总数: {binary_verification_total}")
        print(f"   - 验证成功: {binary_verification_success}")
        print(
            f"   - 成功率: {binary_verification_success / binary_verification_total * 100:.1f}%"
        )

        assert (
            binary_verification_success == binary_verification_total
        ), "所有二进制数据验证都应该成功"

        # 清理测试数据
        print("🧹 清理BSON测试数据...")
        await test_manager.force_cleanup_and_reset()

        # 获取清理后的内存使用情况
        cleanup_info = await redis_client.info("memory")
        cleanup_memory = cleanup_info.get("used_memory", 0)
        cleanup_memory_human = cleanup_info.get("used_memory_human", "0B")

        print(f"   - 清理后内存: {cleanup_memory_human} ({cleanup_memory:,} bytes)")
        print(f"   - 释放内存: {(final_memory - cleanup_memory) / (1024 * 1024):.2f}MB")

        # 清理
        await test_manager.shutdown()
        await manager_factory.stop_all_managers()
        await redis_client.flushdb()
        await redis_client.close()

    except Exception as e:
        print(f"❌ BSON大数据量测试失败: {e}")
        print(f"错误详情: {traceback.format_exc()}")
        raise


async def run_bson_complex_tests():
    """运行所有BSON复杂测试"""
    print("🚀 开始运行Redis分组队列BSON序列化高级测试...")

    # 检查Redis连接
    try:
        redis_provider = get_bean_by_type(RedisProvider)
        redis_client = await redis_provider.get_named_client(
            "default", decode_responses=True
        )
        await redis_client.ping()
        print("✅ Redis连接正常")
        await redis_client.close()
    except Exception as e:
        print(f"❌ Redis连接失败: {e}")
        print("请确保Redis服务正在运行并且配置正确")
        return

    # 定义测试函数
    tests = [
        test_bson_concurrent_producers_consumers,
        test_bson_large_data_memory_usage,
    ]

    passed = 0
    failed = 0

    # 运行所有测试
    for test_func in tests:
        test_name = test_func.__name__
        print(f"\n{'='*60}")
        print(f"🧪 运行BSON测试: {test_name}")
        print(f"{'='*60}")

        try:
            start_time = time.time()
            await test_func()
            elapsed = time.time() - start_time
            print(f"✅ BSON测试通过: {test_name} (耗时: {elapsed:.2f}秒)")
            passed += 1
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"❌ BSON测试失败: {test_name} (耗时: {elapsed:.2f}秒)")
            print(f"错误: {e}")
            print(f"详细错误信息:\n{traceback.format_exc()}")
            failed += 1
            break  # 遇到失败就停止

    # 输出总结
    print(f"\n{'='*60}")
    print("📊 BSON测试总结")
    print(f"{'='*60}")
    print(f"✅ 通过: {passed}")
    print(f"❌ 失败: {failed}")
    print(f"📈 成功率: {passed / (passed + failed) * 100:.1f}%")

    if failed == 0:
        print("🎉 所有BSON测试都通过了！")
    else:
        print(f"⚠️ 有 {failed} 个BSON测试失败")


if __name__ == "__main__":
    asyncio.run(run_bson_complex_tests())
