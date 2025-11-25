#!/usr/bin/env python3
"""
专门测试conv_memcell和episode_memory提取的测试文件
使用tests文件夹下的928_group.json数据
"""

import asyncio
import json
import sys
import os
import pickle
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

from api_specs.types import RawDataType, MemCell
from memory_layer.memcell_extractor.base_memcell_extractor import RawData
from memory_layer.memcell_extractor.conv_memcell_extractor import (
    ConvMemCellExtractor,
    ConversationMemCellExtractRequest,
)
from memory_layer.memory_extractor.episode_memory_extractor import (
    EpisodeMemoryExtractor,
    EpisodeMemoryExtractRequest,
)
from memory_layer.llm.openai_provider import OpenAIProvider


class TestConvMemcellEpisodeExtraction:
    """专门测试conv_memcell和episode_memory提取的测试类"""

    def __init__(self):
        self.llm_provider = OpenAIProvider()
        self.conv_extractor = ConvMemCellExtractor(self.llm_provider)
        self.episode_extractor = EpisodeMemoryExtractor(self.llm_provider)

        # MemCell缓存目录
        self.cache_dir = Path(__file__).parent / "memcell_cache"
        self.cache_dir.mkdir(exist_ok=True)

    def save_memcells_to_file(
        self,
        memcells: List[MemCell],
        episode_memories: List = None,
        filename: str = None,
    ) -> str:
        """保存MemCell列表和EpisodeMemory到本地文件"""
        if filename is None:
            # 使用第一个MemCell的event_id和时间戳作为文件名
            if memcells and len(memcells) > 0:
                first_memcell = memcells[0]
                event_id = first_memcell.event_id[:8]  # 取前8位避免文件名过长
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{event_id}_{timestamp}.json"
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"memcells_{timestamp}.json"

        # 确保文件名是JSON格式
        if not filename.endswith('.json'):
            filename = filename.replace('.pkl', '.json')

        cache_file = self.cache_dir / filename

        try:
            # 将MemCell转换为JSON可序列化的格式
            memcells_json = []
            for memcell in memcells:
                memcell_dict = {
                    "event_id": memcell.event_id,
                    "user_id_list": memcell.user_id_list,
                    "original_data": memcell.original_data,
                    "timestamp": memcell.timestamp,
                    "summary": memcell.summary,
                    "group_id": memcell.group_id,
                    "participants": memcell.participants,
                    "type": memcell.type.value if memcell.type else None,
                    "keywords": memcell.keywords,
                    "subject": memcell.subject,
                    "linked_entities": memcell.linked_entities,
                    "episode": memcell.episode,
                }
                memcells_json.append(memcell_dict)

            # 将EpisodeMemory转换为JSON可序列化的格式
            episode_memories_json = []
            if episode_memories:
                for memory in episode_memories:
                    memory_dict = {
                        "memory_type": (
                            memory.memory_type.value
                            if hasattr(memory.memory_type, 'value')
                            else str(memory.memory_type)
                        ),
                        "event_id": memory.event_id,
                        "user_id": memory.user_id,
                        "timestamp": memory.timestamp,
                        "ori_event_id": memory.ori_event_id,
                        "title": memory.title,
                        "summary": memory.summary,
                        "tags": memory.tags,
                        "group_id": memory.group_id,
                        "participants": memory.participants,
                        "type": memory.type,
                    }
                    episode_memories_json.append(memory_dict)

            # 组合数据
            save_data = {
                "memcells": memcells_json,
                "episode_memories": episode_memories_json,
                "metadata": {
                    "created_at": datetime.now().isoformat(),
                    "memcell_count": len(memcells),
                    "episode_memory_count": (
                        len(episode_memories) if episode_memories else 0
                    ),
                },
            }

            # 保存为JSON文件
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(save_data, f, ensure_ascii=False, indent=2)

            print(f"💾 成功保存到: {cache_file}")
            print(f"   MemCell数量: {len(memcells)}")
            print(
                f"   EpisodeMemory数量: {len(episode_memories) if episode_memories else 0}"
            )
            print(f"   文件大小: {cache_file.stat().st_size / 1024:.2f} KB")
            print(f"   格式: JSON (便于查看和编辑)")

            return str(cache_file)

        except Exception as e:
            print(f"❌ 保存失败: {e}")
            import traceback

            traceback.print_exc()
            return None

    def load_memcells_from_file(self, filename: str) -> tuple[List[MemCell], List]:
        """从本地文件加载MemCell列表和EpisodeMemory"""
        cache_file = self.cache_dir / filename

        if not cache_file.exists():
            print(f"❌ 缓存文件不存在: {cache_file}")
            return [], []

        try:
            # 根据文件扩展名选择加载方式
            if filename.endswith('.json'):
                # 加载JSON格式
                with open(cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # 检查是否是新格式（包含memcells和episode_memories）
                if isinstance(data, dict) and 'memcells' in data:
                    # 新格式
                    memcells_data = data.get('memcells', [])
                    episode_memories_data = data.get('episode_memories', [])
                    metadata = data.get('metadata', {})

                    print(f"📂 加载新格式数据:")
                    print(f"   创建时间: {metadata.get('created_at', '未知')}")
                    print(
                        f"   MemCell数量: {metadata.get('memcell_count', len(memcells_data))}"
                    )
                    print(
                        f"   EpisodeMemory数量: {metadata.get('episode_memory_count', len(episode_memories_data))}"
                    )
                else:
                    # 旧格式（直接是MemCell列表）
                    memcells_data = data if isinstance(data, list) else [data]
                    episode_memories_data = []

                # 将JSON数据转换回MemCell对象
                memcells = []
                for data_item in memcells_data:
                    # 处理type字段
                    type_value = None
                    if data_item.get('type'):
                        type_value = RawDataType(data_item['type'])

                    memcell = MemCell(
                        event_id=data_item['event_id'],
                        user_id_list=data_item['user_id_list'],
                        original_data=data_item['original_data'],
                        timestamp=data_item['timestamp'],
                        summary=data_item['summary'],
                        group_id=data_item.get('group_id'),
                        participants=data_item.get('participants'),
                        type=type_value,
                        keywords=data_item.get('keywords'),
                        subject=data_item.get('subject'),
                        linked_entities=data_item.get('linked_entities'),
                        episode=data_item.get('episode'),
                    )
                    memcells.append(memcell)

                # 将JSON数据转换回EpisodeMemory对象（这里只返回原始数据，不转换为对象）
                episode_memories = episode_memories_data

            else:
                # 兼容旧的pickle格式
                with open(cache_file, 'rb') as f:
                    data = pickle.load(f)
                    if isinstance(data, list):
                        memcells = data
                        episode_memories = []
                    else:
                        memcells = [data]
                        episode_memories = []

            print(f"📂 成功加载从: {cache_file}")
            print(f"   文件大小: {cache_file.stat().st_size / 1024:.2f} KB")
            print(f"   格式: {'JSON' if filename.endswith('.json') else 'Pickle'}")
            print(f"   MemCell数量: {len(memcells)}")
            print(f"   EpisodeMemory数量: {len(episode_memories)}")

            # 显示加载的MemCell摘要
            for i, memcell in enumerate(memcells):
                print(f"   📝 MemCell #{i+1}: {memcell.summary[:50]}...")

            return memcells, episode_memories

        except Exception as e:
            print(f"❌ 加载失败: {e}")
            import traceback

            traceback.print_exc()
            return [], []

    def list_cached_files(self) -> List[str]:
        """列出所有缓存的MemCell文件"""
        # 查找JSON和pickle文件
        json_files = list(self.cache_dir.glob("*.json"))
        pkl_files = list(self.cache_dir.glob("*.pkl"))

        # 过滤掉摘要文件（如果JSON文件名包含streaming_test等，说明是主文件）
        main_json_files = [
            f for f in json_files if 'streaming_test' in f.name or 'memcells_' in f.name
        ]
        all_files = main_json_files + pkl_files

        if not all_files:
            print("📁 缓存目录为空，没有找到MemCell文件")
            return []

        print(f"📁 找到 {len(all_files)} 个MemCell缓存文件:")
        files = []
        for cache_file in sorted(
            all_files, key=lambda f: f.stat().st_mtime, reverse=True
        ):
            file_size = cache_file.stat().st_size / 1024
            mod_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
            file_format = "JSON" if cache_file.suffix == '.json' else "Pickle"

            # 获取MemCell数量
            memcell_count = "未知"
            try:
                if cache_file.suffix == '.json':
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        memcell_count = len(data)
                else:
                    # 对于pickle文件，尝试快速计算
                    with open(cache_file, 'rb') as f:
                        data = pickle.load(f)
                        memcell_count = len(data) if isinstance(data, list) else 1
            except:
                pass

            print(f"   📄 {cache_file.name}")
            print(
                f"      格式: {file_format} | 大小: {file_size:.2f} KB | MemCell数: {memcell_count} | 修改时间: {mod_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            files.append(cache_file.name)

        return files

    def load_928_group_data(self) -> List[Dict[str, Any]]:
        """加载928_group.json数据"""
        json_file = Path(__file__).parent / "928_group.json"
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        print(f"📚 加载了 {len(data)} 条消息数据")
        return data

    def convert_to_raw_data(
        self, messages: List[Dict[str, Any]], start_idx: int = 0, count: int = 10
    ) -> List[RawData]:
        """将JSON消息转换为RawData格式"""
        raw_data_list = []

        # 取指定范围的消息
        selected_messages = messages[start_idx : start_idx + count]

        for msg in selected_messages:
            # 跳过系统消息
            if 'sender' not in msg or not msg.get('content'):
                continue

            # 转换时间格式
            create_time = msg.get('createTime', '')
            if create_time:
                try:
                    # 保持ISO时间格式，供边界检测使用
                    timestamp_str = create_time
                    # 也计算timestamp用于其他地方
                    dt = datetime.fromisoformat(create_time.replace('Z', '+00:00'))
                    timestamp = int(dt.timestamp())
                except:
                    timestamp_str = datetime.now().isoformat()
                    timestamp = int(datetime.now().timestamp())
            else:
                timestamp_str = datetime.now().isoformat()
                timestamp = int(datetime.now().timestamp())

            content = {
                "content": msg.get('content', ''),
                "sender": msg.get('sender', ''),
                "timestamp": timestamp_str,  # 使用ISO格式字符串
                "sender_title": msg.get('sender_title', ''),
                "tanka_mag_id": msg.get('tanka_mag_id', ''),
            }

            raw_data = RawData(
                content=content,
                data_id=msg.get(
                    'tanka_mag_id', f'msg_{start_idx + len(raw_data_list)}'
                ),
            )
            raw_data_list.append(raw_data)

        print(f"✅ 转换了 {len(raw_data_list)} 条消息为RawData")
        return raw_data_list

    async def test_conv_memcell_extraction(
        self, raw_data_list: List[RawData]
    ) -> MemCell:
        """测试对话边界检测和MemCell提取"""
        print("\n" + "=" * 80)
        print("🧪 测试ConvMemCellExtraction - 对话边界检测")
        print("=" * 80)

        if len(raw_data_list) < 5:
            print("❌ 数据不足，需要至少5条消息")
            return None

        # 分割历史和新消息
        history_raw_data_list = raw_data_list[:-3]  # 前面的作为历史
        new_raw_data_list = raw_data_list[-3:]  # 最后3条作为新消息

        participants = []
        for data in raw_data_list:
            sender = data.content.get('sender', '')
            if sender and sender not in participants:
                participants.append(sender)

        print(f"📊 历史消息: {len(history_raw_data_list)} 条")
        print(f"📊 新消息: {len(new_raw_data_list)} 条")
        print(f"👥 参与者: {participants}")

        # 显示消息内容
        print("\n💬 历史消息预览:")
        for i, data in enumerate(history_raw_data_list[-3:]):  # 只显示最后3条历史消息
            content = data.content.get('content', '')[:100]
            sender = data.content.get('sender', '')
            print(f"   [{i+1}] {sender}: {content}...")

        print("\n💬 新消息预览:")
        for i, data in enumerate(new_raw_data_list):
            content = data.content.get('content', '')[:100]
            sender = data.content.get('sender', '')
            print(f"   [{i+1}] {sender}: {content}...")

        # 创建请求
        request = ConversationMemCellExtractRequest(
            history_raw_data_list=history_raw_data_list,
            new_raw_data_list=new_raw_data_list,
            user_id_list=participants,
            participants=participants,
            group_id="928_group_test",
            old_memory_list=[],
        )

        print(f"\n🔄 执行对话边界检测...")
        try:
            result = await self.conv_extractor.extract_memcell(request)

            if result is None:
                print("❌ extract_memcell返回None")
                return None

            if isinstance(result, tuple):
                if len(result) == 2:
                    memcell, status_result = result
                elif len(result) == 3:
                    memcell, status_result, episode_memories = result
                else:
                    print(f"❌ 意外的返回值格式: {len(result)} 个元素")
                    return None
            else:
                memcell = result
                status_result = None

            print(f"\n📋 边界检测结果:")
            if status_result:
                print(f"   should_wait: {status_result.should_wait}")

            if memcell:
                print(f"✅ 成功提取MemCell:")
                print(f"   event_id: {memcell.event_id}")
                print(f"   user_id_list: {memcell.user_id_list}")
                print(f"   participants: {memcell.participants}")
                print(f"   summary: {memcell.summary[:200]}...")
                print(f"   timestamp: {memcell.timestamp}")
                print(f"   group_id: {memcell.group_id}")
                return memcell
            else:
                print("ℹ️ 未检测到对话边界，没有生成MemCell")
                return None

        except Exception as e:
            print(f"❌ ConvMemCell提取失败: {e}")
            import traceback

            traceback.print_exc()
            return None

    async def test_episode_memory_extraction(self, memcell: MemCell) -> None:
        """测试情景记忆提取"""
        print("\n" + "=" * 80)
        print("🧪 测试EpisodeMemoryExtraction - 情景记忆提取")
        print("=" * 80)

        if not memcell:
            print("❌ 没有MemCell，跳过情景记忆提取测试")
            return

        # 从MemCell的原始数据中提取参与者列表作为user_id_list
        conversation_participants = []
        if hasattr(memcell, 'original_data') and memcell.original_data:
            for data in memcell.original_data:
                sender = data.get('sender', '')
                if sender and sender not in conversation_participants:
                    conversation_participants.append(sender)

        # 如果从原始数据提取失败，则使用memcell的user_id_list作为后备
        if not conversation_participants:
            conversation_participants = memcell.user_id_list or []

        print(f"📊 从对话中提取的参与者: {conversation_participants}")

        # 创建请求
        request = EpisodeMemoryExtractRequest(
            memcell_list=[memcell],
            user_id_list=conversation_participants,  # 直接从对话中提取
            participants=memcell.participants,
            group_id=memcell.group_id,
            old_memory_list=[],
        )

        print(f"🔄 执行情景记忆提取...")
        print(f"   MemCell数量: {len(request.memcell_list)}")
        print(f"   参与者: {request.participants}")
        print(f"   群组ID: {request.group_id}")

        try:
            episode_memories = await self.episode_extractor.extract_memory(request)

            if episode_memories:
                print(f"\n✅ 成功提取 {len(episode_memories)} 个情景记忆:")
                for i, memory in enumerate(episode_memories):
                    print(f"\n   📝 情景记忆 #{i+1}:")
                    print(f"      event_id: {memory.event_id}")
                    print(f"      user_id: {memory.user_id}")
                    print(f"      title: {memory.title}")
                    print(f"      summary: {memory.summary[:200]}...")
                    print(f"      timestamp: {memory.timestamp}")
                    print(f"      memory_type: {memory.memory_type}")
                    if hasattr(memory, 'tags') and memory.tags:
                        print(f"      tags: {memory.tags}")
            else:
                print("ℹ️ 没有提取到情景记忆")

        except Exception as e:
            print(f"❌ 情景记忆提取失败: {e}")
            import traceback

            traceback.print_exc()

    async def run_streaming_test(self, start_idx: int = 0, max_messages: int = 30):
        """运行流式输入测试 - 模拟真实对话场景"""
        print("🚀 开始运行流式对话边界检测测试")
        print("=" * 80)

        # 1. 加载数据
        messages = self.load_928_group_data()
        all_raw_data = self.convert_to_raw_data(messages, start_idx, max_messages)

        if len(all_raw_data) < 5:
            print("❌ 转换后的数据不足，至少需要5条有效消息")
            return

        print(f"📊 准备流式处理 {len(all_raw_data)} 条消息")

        # 模拟流式输入：累积历史，逐条添加新消息
        history_buffer = []
        memcells_generated = []
        all_episode_memories = []  # 收集所有生成的episode memories

        for i, new_raw_data in enumerate(all_raw_data):
            print(f"\n{'='*60}")
            print(f"📨 流式输入第 {i+1}/{len(all_raw_data)} 条消息")
            print(f"{'='*60}")

            # 显示当前消息
            content = new_raw_data.content.get('content', '')[:100]
            sender = new_raw_data.content.get('sender', '')
            timestamp = new_raw_data.content.get('timestamp', '')
            print(f"👤 {sender}: {content}...")
            print(f"⏰ 时间: {timestamp}")

            # 如果历史消息少于3条，先积累历史
            if len(history_buffer) < 3:
                history_buffer.append(new_raw_data)
                print(f"📚 积累历史消息中... ({len(history_buffer)}/3)")
                continue

            # 当有足够历史消息时，开始边界检测
            print(f"📊 历史消息: {len(history_buffer)} 条")
            print(f"📊 新消息: 1 条")

            # 获取参与者
            participants = []
            for data in history_buffer + [new_raw_data]:
                sender = data.content.get('sender', '')
                if sender and sender not in participants:
                    participants.append(sender)

            print(f"👥 当前参与者: {participants}")

            # 创建请求进行边界检测
            # 为了测试participants合并功能，我们故意传入一些额外的participants
            test_participants = participants + [
                "Admin",
                "System",
            ]  # 添加一些可能不在对话中的参与者

            request = ConversationMemCellExtractRequest(
                history_raw_data_list=history_buffer.copy(),
                new_raw_data_list=[new_raw_data],
                user_id_list=participants,
                participants=test_participants,  # 使用扩展的participants列表
                group_id="928_group_streaming",
                old_memory_list=[],
            )

            print(f"🧪 测试participants合并功能:")
            print(f"   传入的participants: {test_participants}")
            print(
                f"   当前对话中的speakers: {[d.content.get('sender', '') for d in history_buffer + [new_raw_data] if d.content.get('sender')]}"
            )
            print(f"   期望合并结果应包含所有唯一的participants和speakers")

            print(f"🔄 执行边界检测...")
            try:
                result = await self.conv_extractor.extract_memcell(request)

                if result is None:
                    print("❌ extract_memcell返回None")
                    boundary_detected = False
                    memcell = None
                else:
                    if isinstance(result, tuple):
                        if len(result) == 2:
                            memcell, status_result = result
                        elif len(result) == 3:
                            memcell, status_result, episode_memories = result
                        else:
                            print(f"❌ 意外的返回值格式: {len(result)} 个元素")
                            memcell = None
                            status_result = None
                    else:
                        memcell = result
                        status_result = None

                    print(f"📋 边界检测结果:")
                    if status_result:
                        print(f"   should_wait: {status_result.should_wait}")

                    if memcell:
                        boundary_detected = True
                        print(f"🎯 检测到对话边界! 生成MemCell:")
                        print(f"   event_id: {memcell.event_id}")
                        print(f"   summary: {memcell.summary[:100]}...")

                        memcells_generated.append(memcell)

                        # 测试Episode Memory提取 - 从对话中提取user_id_list
                        print(f"🔄 自动触发情景记忆提取...")

                        # 从历史缓冲区和新消息中提取所有参与者作为user_id_list
                        conversation_participants = []
                        for data in history_buffer + [new_raw_data]:
                            sender = data.content.get('sender', '')
                            if sender and sender not in conversation_participants:
                                conversation_participants.append(sender)

                        print(f"📊 从对话中提取的参与者: {conversation_participants}")

                        # 创建专门的Episode Memory提取请求
                        episode_request = EpisodeMemoryExtractRequest(
                            memcell_list=[memcell],
                            user_id_list=conversation_participants,  # 直接从对话中提取
                            participants=participants,
                            group_id="928_group_streaming",
                            old_memory_list=[],
                        )

                        try:
                            episode_memories = (
                                await self.episode_extractor.extract_memory(
                                    episode_request
                                )
                            )
                            if episode_memories:
                                print(f"✅ 成功提取 {len(episode_memories)} 个情景记忆")
                                all_episode_memories.extend(
                                    episode_memories
                                )  # 收集episode memories
                                for i, memory in enumerate(episode_memories):
                                    print(
                                        f"   📝 情景记忆 #{i+1}: {memory.user_id} - {memory.title[:50]}..."
                                    )
                            else:
                                print("ℹ️ 没有提取到情景记忆")
                        except Exception as e:
                            print(f"❌ 情景记忆提取失败: {e}")

                        # 清空历史，重新开始积累
                        print(f"🔄 重置历史缓冲区，开始新的对话片段")
                        history_buffer = []
                    else:
                        boundary_detected = False
                        print("ℹ️ 未检测到对话边界，继续积累对话")

            except Exception as e:
                print(f"❌ 边界检测失败: {e}")
                boundary_detected = False

            # 如果没有检测到边界，将新消息加入历史
            if not boundary_detected:
                history_buffer.append(new_raw_data)

                # 限制历史缓冲区大小，避免过长
                if len(history_buffer) > 10:
                    history_buffer = history_buffer[-8:]  # 保留最近8条
                    print(f"📚 历史缓冲区已满，保留最近 {len(history_buffer)} 条消息")

            # 流式处理间隔（可选）
            import asyncio

            await asyncio.sleep(0.1)  # 模拟真实消息间隔

        # 总结和保存
        print(f"\n{'='*80}")
        print(f"🎉 流式测试完成!")
        print(f"📊 总共处理: {len(all_raw_data)} 条消息")
        print(f"🎯 检测到边界: {len(memcells_generated)} 次")
        print(f"💾 生成MemCell: {len(memcells_generated)} 个")
        print(f"📚 生成EpisodeMemory: {len(all_episode_memories)} 个")

        if memcells_generated:
            print(f"\n📝 生成的MemCell摘要:")
            for i, memcell in enumerate(memcells_generated):
                print(f"   {i+1}. {memcell.summary[:80]}...")

        if all_episode_memories:
            print(f"\n📚 生成的EpisodeMemory摘要:")
            for i, memory in enumerate(all_episode_memories):
                print(f"   {i+1}. {memory.user_id}: {memory.title[:60]}...")

        if memcells_generated or all_episode_memories:
            # 自动保存MemCell和EpisodeMemory到本地文件
            print(f"\n💾 自动保存数据到本地文件...")
            # 使用第一个MemCell的event_id作为文件名的一部分
            if memcells_generated:
                first_event_id = memcells_generated[0].event_id[:8]
            else:
                first_event_id = "no_memcell"

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            cache_filename = f"{first_event_id}_{timestamp}.json"

            saved_file = self.save_memcells_to_file(
                memcells_generated, all_episode_memories, cache_filename
            )

            if saved_file:
                print(f"✅ 数据已保存，下次可使用以下代码快速加载:")
                print(
                    f"   memcells, episode_memories = tester.load_memcells_from_file('{cache_filename}')"
                )

        return memcells_generated

    async def run_test(self, start_idx: int = 0, count: int = 15):
        """运行完整测试流程"""
        print("🚀 开始运行Conv MemCell和Episode Memory提取测试")
        print("=" * 80)

        # 1. 加载数据
        messages = self.load_928_group_data()

        # 2. 转换为RawData
        raw_data_list = self.convert_to_raw_data(messages, start_idx, count)

        if len(raw_data_list) < 5:
            print("❌ 转换后的数据不足，至少需要5条有效消息")
            return

        # 3. 测试ConvMemCell提取
        memcell = await self.test_conv_memcell_extraction(raw_data_list)

        # 4. 测试EpisodeMemory提取
        await self.test_episode_memory_extraction(memcell)

        print("\n🎉 测试完成!")

    async def test_cached_memcells(self, filename: str = None):
        """测试加载缓存的MemCell并进行情景记忆提取"""
        print("🚀 开始测试缓存的MemCell加载和情景记忆提取")
        print("=" * 80)

        # 如果没有指定文件名，列出所有可用的缓存文件
        if filename is None:
            cached_files = self.list_cached_files()
            if not cached_files:
                print("❌ 没有找到缓存的MemCell文件，请先运行流式测试生成MemCell")
                return

            # 使用最新的缓存文件
            filename = cached_files[0]
            print(f"🔄 自动选择最新的缓存文件: {filename}")

        # 加载MemCell和EpisodeMemory
        memcells, episode_memories = self.load_memcells_from_file(filename)

        if not memcells:
            print("❌ 加载MemCell失败或文件为空")
            return

        # 检查是否已有缓存的episode memories
        if episode_memories:
            print(f"\n✅ 发现 {len(episode_memories)} 个缓存的EpisodeMemory，直接展示:")
            for i, memory in enumerate(episode_memories):
                if isinstance(memory, dict):
                    user_id = memory.get('user_id', 'Unknown')
                    title = memory.get('title', 'No title')[:60]
                    print(f"   📚 #{i+1}: {user_id} - {title}...")
                else:
                    print(f"   📚 #{i+1}: {memory.user_id} - {memory.title[:60]}...")

            print(f"\n💡 所有数据已缓存，无需重新生成！")
            return

        print(f"\n🧪 开始对 {len(memcells)} 个缓存的MemCell进行情景记忆提取测试...")

        total_episode_memories = 0

        for i, memcell in enumerate(memcells):
            print(f"\n{'='*60}")
            print(f"📝 测试MemCell #{i+1}/{len(memcells)}")
            print(f"{'='*60}")
            print(f"🆔 Event ID: {memcell.event_id}")
            print(f"📄 摘要: {memcell.summary[:100]}...")
            print(f"👥 参与者: {memcell.participants}")
            print(f"⏰ 时间戳: {memcell.timestamp}")

            # 执行情景记忆提取
            await self.test_episode_memory_extraction(memcell)

            # 简单计数（这里可以根据实际返回结果计数）
            if memcell.participants:
                total_episode_memories += len(memcell.participants)

        print(f"\n{'='*80}")
        print(f"🎉 缓存MemCell测试完成!")
        print(f"📊 测试了 {len(memcells)} 个MemCell")
        print(f"📝 预期生成约 {total_episode_memories} 个情景记忆")
        print(f"💡 优势：直接加载MemCell，跳过了对话边界检测步骤，大大提高了效率！")


async def main():
    """主函数"""
    print("🎬 启动928群组数据的Conv MemCell和Episode Memory提取测试")
    print("=" * 80)

    tester = TestConvMemcellEpisodeExtraction()

    print("选择测试模式:")
    print("1. 🔄 流式输入测试（模拟真实对话场景，会生成并保存MemCell）")
    print("2. 📦 批量测试（一次性处理多条消息）")
    print("3. 📂 缓存测试（加载已保存的MemCell进行情景记忆提取）")

    # 首先检查是否有缓存文件
    cached_files = tester.list_cached_files()

    if cached_files:
        print(f"\n💡 发现缓存文件！推荐先尝试缓存测试模式，速度更快")
        print(f"🚀 运行缓存测试模式...")
        await tester.test_cached_memcells()

        print(f"\n" + "=" * 80)
        print("🔄 附加：运行流式测试生成新的MemCell...")
    else:
        print(f"\n📁 没有发现缓存文件，运行流式测试...")

    # 流式测试：模拟真实对话场景，一条条输入消息
    memcells = await tester.run_streaming_test(
        start_idx=15, max_messages=10
    )  # 减少消息数量用于快速测试

    print(f"\n📈 流式测试结果总结:")
    print(f"   - 成功检测到 {len(memcells)} 个对话边界")
    print(f"   - 生成了 {len(memcells)} 个MemCell")
    print(f"   - MemCell已保存到本地，下次可直接加载使用")

    # 可选：也运行一次批量测试作为对比
    print(f"\n" + "=" * 80)
    print("🔍 附加：运行批量测试作为对比...")
    await tester.run_test(start_idx=15, count=15)  # 从第15条消息开始，取15条


if __name__ == "__main__":
    asyncio.run(main())
