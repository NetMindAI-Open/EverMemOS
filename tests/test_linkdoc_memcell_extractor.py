"""
LinkDocMemCellExtractor 测试

测试文档记忆单元提取功能，包括：
- 文档MemCell生成
- 多数据源支持（Notion、Google Drive、Dropbox）
- 文档过滤逻辑
- 长文档处理

使用方法：
    python src/bootstrap.py tests/test_linkdoc_memcell_extractor.py
"""

import pytest
import asyncio
import json
import os
from datetime import datetime
from typing import Dict, Any, List

# 导入依赖注入相关模块
from core.di.utils import get_bean_by_type
from core.observation.logger import get_logger

# 导入要测试的模块
from memory_layer.memcell_extractor.linkdoc_memcell_extractor import (
    LinkDocMemCellExtractor,
    LinkDocMemCellExtractRequest,
    FilterConfig,
)
from memory_layer.memcell_extractor.base_memcell_extractor import (
    RawData,
    MemCell,
    StatusResult,
)
from memory_layer.llm.llm_provider import LLMProvider
from memory_layer.llm.openai_provider import OpenAIProvider
from memory_layer.types import RawDataType
from infra_layer.adapters.input.mq.mapper.linkdoc_mapper import (
    convert_notion_document_to_raw_data,
    convert_dropbox_document_to_raw_data,
    convert_google_document_to_raw_data,
    convert_memo_document_to_raw_data,
)

# 获取日志记录器
logger = get_logger(__name__)


def get_llm_provider() -> LLMProvider:
    """获取LLM Provider，先尝试DI容器，失败则直接创建"""
    try:
        # 尝试从DI容器获取
        return get_bean_by_type(LLMProvider)
    except:
        # 如果DI容器中没有，则直接创建
        logger.info("DI容器中未找到LLMProvider，直接创建...")
        return LLMProvider(
            "openai", model="google/gemini-2.5-flash", temperature=0.3, max_tokens=16384
        )


def get_llm_provider_with_stats() -> OpenAIProvider:
    """获取带统计功能的LLM Provider"""
    return OpenAIProvider(
        model="google/gemini-2.5-flash",
        temperature=0.3,
        max_tokens=16384,
        enable_stats=True,
    )


class TestLinkDocMemCellExtractor:
    """LinkDocMemCellExtractor 测试类"""

    def setup_method(self):
        """每个测试方法前的设置"""
        self.test_user_id = "test_user_123"
        self.test_timestamp = int(datetime.now().timestamp())

        # 创建过滤配置
        self.filter_config = FilterConfig(
            enable_filtering=False,  # 测试时默认关闭过滤
            min_content_length=20,
            max_content_length=500000,
            filter_preview_length=500,
        )

    def create_test_notion_document(
        self,
        title: str = "Test Document",
        content: str = "This is a test document for LinkDoc extraction.",
        is_deleted: bool = False,
    ) -> Dict[str, Any]:
        """创建测试用的Notion文档数据"""
        return {
            'id': 'test_notion_123',
            'third_party_user_id': "test_user@notion.com",
            'title': title,
            'body_content': content,
            'is_delete': is_deleted,
            'last_update_timestamp': str(self.test_timestamp),
            'notion_url': 'https://notion.so/test',
            'create_timestamp': str(self.test_timestamp),
            'object_id': 'test_obj_id',
            'parent_id': 'parent_123',
            'object_type': '1',
            'parent_type': '3',
        }

    def create_test_google_document(
        self,
        name: str = "Test Google Doc",
        content: str = "This is a test Google Drive document.",
        is_trashed: bool = False,
    ) -> Dict[str, Any]:
        """创建测试用的Google Drive文档数据"""
        return {
            '_id': 'google_test_123',
            'third_party_user_id': 'test_user@gmail.com',
            'name': name,
            'content': content,
            'explicitlyTrashed': is_trashed,
            'modify_timestamp': str(self.test_timestamp),
            'downloadUrl': 'https://drive.google.com/test',
            'type': 'application/vnd.google-apps.document',
            'owners': ['test_user@gmail.com'],
            'file_id': 'google_file_123',
        }

    def create_test_dropbox_document(
        self,
        name: str = "Test Dropbox Doc",
        content: str = "This is a test Dropbox document.",
        is_deleted: bool = False,
    ) -> Dict[str, Any]:
        """创建测试用的Dropbox文档数据"""
        return {
            '_id': 'dropbox_test_123',
            'third_party_user_id': 'test_user@dropbox.com',
            'name': name,
            'content': content,
            'deleteFlag': 1 if is_deleted else 0,
            'modify_timestamp': str(self.test_timestamp),
            'downloadUrl': 'https://dropbox.com/test',
            'type': 'application/pdf',
            'file_id': 'dropbox_file_123',
        }

    def create_test_memo_document(
        self,
        title: str = "Test Memo",
        body: str = "This is a test memo document.",
        is_deleted: bool = False,
    ) -> Dict[str, Any]:
        """创建测试用的Memo文档数据"""
        return {
            'id': 'memo_test_123',
            'readerids': [self.test_user_id],
            'title': title,
            'body': body,
            'files': [],
            'importsource': None,
            'delete_flag': 1 if is_deleted else 0,
            'updatetime': self.test_timestamp * 1000,  # Memo使用毫秒时间戳
            'create_timestamp': f"{datetime.fromtimestamp(self.test_timestamp).strftime('%Y-%m-%d %H:%M:%S')}.123456",
            'last_update_timestamp': f"{datetime.fromtimestamp(self.test_timestamp).strftime('%Y-%m-%d %H:%M:%S')}.123456",
            'creatorid': self.test_user_id,
            'bodyhtml': None,
            'labels': [],
            'shareids': [],
        }

    def create_long_document_content(self, repeat_count: int = 5) -> str:
        """创建长文档内容用于测试分块处理"""
        base_content = """
# 测试文档标题

这是一个用于测试长文档处理的示例文档。文档包含多个段落和章节。

## 第一章：项目概述

本项目旨在开发一个高效的文档处理系统，能够处理各种格式的文档，包括但不限于 Notion、Google Drive 和 Dropbox 中的文档。

### 1.1 项目目标

- 实现高效的文档内容提取
- 支持多种文档源
- 提供智能的内容分析和摘要
- 确保系统的可扩展性和稳定性

### 1.2 技术栈

我们采用现代化的技术栈来确保系统的性能和可维护性：
- Python 3.8+
- FastAPI 框架
- PostgreSQL 数据库
- Redis 缓存
- Docker 容器化部署

## 第二章：系统架构

系统采用微服务架构，主要包含以下几个核心组件：

1. 文档提取服务
2. 内容分析服务
3. 存储服务
4. API 网关
5. 用户管理服务

每个服务都具有独立的数据库和缓存，通过消息队列进行异步通信。
"""
        return base_content * repeat_count

    @pytest.mark.asyncio
    async def test_basic_notion_extraction(self):
        """测试基础Notion文档提取"""
        print("\n🧪 测试基础Notion文档提取")

        # 获取LLM Provider
        llm_provider = get_llm_provider()
        extractor = LinkDocMemCellExtractor(
            llm_provider, filter_config=self.filter_config
        )

        # 创建测试文档
        test_doc = self.create_test_notion_document(
            title="项目需求文档",
            content="这是一个详细的项目需求文档，包含了用户故事、功能需求、非功能需求等内容。文档旨在为开发团队提供清晰的项目指导。",
        )

        # 转换为RawData
        raw_data = await convert_notion_document_to_raw_data(test_doc)

        print(f"📋 测试文档信息:")
        print(f"   - 标题: {test_doc['title']}")
        print(f"   - 内容长度: {len(test_doc['body_content'])} 字符")
        print(f"   - 用户ID: {test_doc['third_party_user_id']}")

        # 创建请求
        request = LinkDocMemCellExtractRequest(
            history_raw_data_list=[],
            new_raw_data_list=[raw_data],
            user_id_list=[self.test_user_id],
        )
        raw_data.content['user_id_list'] = request.user_id_list
        # 执行提取
        result = await extractor.extract_memcell(request)

        # 验证结果
        assert result is not None, "提取结果不应该为None"
        memcell, status_result = result

        print(f"✅ 提取完成:")
        print(f"   - MemCell: {memcell is not None}")
        print(f"   - should_wait: {status_result.should_wait}")

        if memcell:
            print(f"\n📄 MemCell详细信息:")
            print(f"   - event_id: {memcell.event_id}")
            print(f"   - user_id_list: {memcell.user_id_list}")
            print(f"   - file_name: {memcell.file_name}")
            print(f"   - file_type: {memcell.file_type}")
            print(f"   - source_type: {memcell.source_type}")
            print(f"   - type: {memcell.type}")
            print(f"   - timestamp: {memcell.timestamp}")
            print(f"   - subject: {memcell.subject}")
            print(f"   - summary: {memcell.summary}")
            print(f"   - keywords: {memcell.keywords}")
            print(f"   - clips数量: {len(memcell.clips) if memcell.clips else 0}")

            # 验证基本字段
            assert memcell.event_id is not None
            assert len(memcell.user_id_list) > 0
            assert memcell.file_name == test_doc['title']
            assert memcell.source_type == 'notion'
            assert memcell.type == RawDataType.LINKDOC
            assert memcell.summary is not None

        else:
            print("⚠️ 没有生成MemCell")

    @pytest.mark.asyncio
    async def test_multiple_data_sources(self):
        """测试多数据源支持"""
        print("\n🧪 测试多数据源支持")

        # 获取LLM Provider
        llm_provider = get_llm_provider()
        extractor = LinkDocMemCellExtractor(
            llm_provider, filter_config=self.filter_config
        )

        # 准备不同数据源的测试用例
        test_cases = [
            {
                "name": "Notion文档",
                "doc": self.create_test_notion_document(
                    title="技术设计文档",
                    content="这是一份详细的技术设计文档，包含系统架构、数据库设计、API设计等内容。",
                ),
                "mapper": convert_notion_document_to_raw_data,
                "expected_source": "notion",
            },
            {
                "name": "Google Drive文档",
                "doc": self.create_test_google_document(
                    name="用户手册",
                    content="这是一份用户操作手册，详细介绍了产品的各项功能和使用方法。",
                ),
                "mapper": convert_google_document_to_raw_data,
                "expected_source": "google",
            },
            {
                "name": "Dropbox文档",
                "doc": self.create_test_dropbox_document(
                    name="项目总结报告",
                    content="这是项目完成后的总结报告，包含项目成果、经验教训和改进建议。",
                ),
                "mapper": convert_dropbox_document_to_raw_data,
                "expected_source": "dropbox",
            },
            {
                "name": "Memo文档",
                "doc": self.create_test_memo_document(
                    title="工作笔记",
                    body="这是一份工作笔记，记录了今天的工作进展和重要决策。",
                ),
                "mapper": convert_memo_document_to_raw_data,
                "expected_source": "memo",
            },
        ]

        for i, test_case in enumerate(test_cases):
            print(f"\n📋 测试用例 {i+1}: {test_case['name']}")

            # 转换为RawData
            raw_data = await test_case["mapper"](test_case["doc"])

            # 创建请求
            user_id = self.test_user_id
            request = LinkDocMemCellExtractRequest(
                history_raw_data_list=[],
                new_raw_data_list=[raw_data],
                user_id_list=[user_id],
            )

            # 执行提取
            result = await extractor.extract_memcell(request)

            if result and result[0]:
                memcell, status_result = result
                print(f"✅ {test_case['name']} 提取成功:")
                print(f"   - 数据源: {memcell.source_type}")
                print(f"   - 文件名: {memcell.file_name}")
                print(f"   - 摘要: {memcell.summary}")

                # 验证数据源类型
                assert memcell.source_type == test_case["expected_source"]
                assert memcell.type == RawDataType.LINKDOC

            else:
                print(f"⚠️ {test_case['name']} 未能提取MemCell")

    @pytest.mark.asyncio
    async def test_document_filtering(self):
        """测试文档过滤功能"""
        print("\n🧪 测试文档过滤功能")

        # 获取LLM Provider
        llm_provider = get_llm_provider()

        # 创建启用过滤的配置
        filter_config = FilterConfig(
            enable_filtering=True,
            min_content_length=30,
            max_content_length=500000,
            exclude_keywords=["游戏", "娱乐", "购物"],
        )

        extractor = LinkDocMemCellExtractor(llm_provider, filter_config=filter_config)

        # 测试用例：应该被过滤的文档
        filter_test_cases = [
            {
                "name": "内容过短文档",
                "doc": self.create_test_notion_document(title="短笔记", content="Hi"),
                "should_be_filtered": True,
                "reason": "内容太短",
            },
            {
                "name": "包含排除关键词文档",
                "doc": self.create_test_notion_document(
                    title="我的游戏收藏",
                    content="这是我最喜欢的游戏列表，包含各种类型的游戏推荐。",
                ),
                "should_be_filtered": True,
                "reason": "包含排除关键词",
            },
            {
                "name": "已删除文档",
                "doc": self.create_test_notion_document(
                    title="重要文档",
                    content="这是一个重要的工作文档，包含项目相关信息。",
                    is_deleted=True,
                ),
                "should_be_filtered": True,
                "reason": "文档已删除",
            },
            {
                "name": "包含公司部门名但内容无意义文档",
                "doc": self.create_test_notion_document(
                    title="tanka 随机文档",
                    content="tanka 啊啊啊啊 随便写写 123456 xyz abc tanka 哈哈哈 无聊的内容 blah blah tanka 测试测试 随机字符串 qwerty asdf 没有任何意义的文字堆砌",
                ),
                "should_be_filtered": True,
                "reason": "虽然包含公司部门名，但内容无意义",
            },
            {
                "name": "正常工作文档",
                "doc": self.create_test_notion_document(
                    title="项目会议纪要",
                    content="今天的项目会议讨论了技术方案、时间安排和资源分配等重要议题。团队决定采用微服务架构。",
                ),
                "should_be_filtered": False,
                "reason": "正常文档",
            },
        ]

        for i, test_case in enumerate(filter_test_cases):
            print(f"\n📋 过滤测试 {i+1}: {test_case['name']}")

            # 转换为RawData
            raw_data = await convert_notion_document_to_raw_data(test_case["doc"])

            # 测试预处理过滤
            should_process, reason = await extractor.pre_process(raw_data)

            print(f"   - 预期: {'过滤' if test_case['should_be_filtered'] else '通过'}")
            print(f"   - 实际: {'过滤' if not should_process else '通过'}")
            print(f"   - 原因: {reason}")

            if test_case["should_be_filtered"]:
                assert (
                    not should_process
                ), f"文档应该被过滤但却通过了: {test_case['name']}"
                print(f"✅ 正确过滤: {test_case['reason']}")
            else:
                # 注意：正常文档可能在LLM过滤阶段被过滤，这里只验证规则过滤
                print(f"✅ 通过规则过滤阶段")

    @pytest.mark.asyncio
    async def test_long_document_processing(self):
        """测试长文档处理"""
        print("\n🧪 测试长文档处理")

        # 获取LLM Provider
        llm_provider = get_llm_provider()

        # 创建支持长文档的extractor（较小的chunk size用于测试）
        extractor = LinkDocMemCellExtractor(
            llm_provider,
            max_chars_per_chunk=1000,  # 较小的chunk用于测试
            filter_config=self.filter_config,
        )

        # 创建长文档
        long_content = self.create_long_document_content(repeat_count=3)
        long_doc = self.create_test_notion_document(
            title="详细技术文档", content=long_content
        )

        print(f"📋 长文档信息:")
        print(f"   - 标题: {long_doc['title']}")
        print(f"   - 内容长度: {len(long_content)} 字符")
        print(f"   - 预期会被分块处理")

        # 转换为RawData
        raw_data = await convert_notion_document_to_raw_data(long_doc)

        # 创建请求
        request = LinkDocMemCellExtractRequest(
            history_raw_data_list=[],
            new_raw_data_list=[raw_data],
            user_id_list=[self.test_user_id],
        )

        # 执行提取
        result = await extractor.extract_memcell(request)

        # 验证结果
        if result and result[0]:
            memcell, status_result = result
            print(f"✅ 长文档处理成功:")
            print(f"   - 文件名: {memcell.file_name}")
            print(f"   - 摘要长度: {len(memcell.summary)} 字符")
            print(f"   - clips数量: {len(memcell.clips) if memcell.clips else 0}")
            print(f"   - 关键词: {memcell.keywords[:5] if memcell.keywords else []}")

            # 验证长文档处理
            assert memcell.clips is not None
            assert len(memcell.clips) > 1, "长文档应该被分成多个clips"
            assert memcell.summary is not None
            assert len(memcell.summary) > 50, "长文档应该有较详细的摘要"

            print(f"✅ 长文档成功分成 {len(memcell.clips)} 个clips")

        else:
            print("⚠️ 长文档处理失败")

    @pytest.mark.asyncio
    async def test_error_handling(self):
        """测试错误处理"""
        print("\n🧪 测试错误处理")

        # 获取LLM Provider
        llm_provider = get_llm_provider()
        extractor = LinkDocMemCellExtractor(
            llm_provider, filter_config=self.filter_config
        )

        # 测试空请求
        print("\n📋 测试空请求:")
        empty_request = LinkDocMemCellExtractRequest(
            history_raw_data_list=[], new_raw_data_list=[], user_id_list=[]
        )

        result = await extractor.extract_memcell(empty_request)
        assert result is not None
        memcell, status_result = result
        assert memcell is None
        print("✅ 空请求正确返回None")

        # 测试无效数据
        print("\n📋 测试无效数据:")
        invalid_doc = {
            'id': 'invalid_doc',
            'title': '',  # 空标题
            'body_content': '',  # 空内容
            'tanka_user_id': self.test_user_id,
        }

        try:
            raw_data = await convert_notion_document_to_raw_data(invalid_doc)
            request = LinkDocMemCellExtractRequest(
                history_raw_data_list=[],
                new_raw_data_list=[raw_data],
                user_id_list=[self.test_user_id],
            )

            result = await extractor.extract_memcell(request)
            print("✅ 无效数据处理完成（可能被过滤或生成默认MemCell）")

        except Exception as e:
            print(f"✅ 无效数据正确抛出异常: {type(e).__name__}")

    @pytest.mark.asyncio
    async def test_memo_extraction(self):
        """测试Memo文档提取"""
        print("\n🧪 测试Memo文档提取")

        # 获取LLM Provider
        llm_provider = get_llm_provider()
        extractor = LinkDocMemCellExtractor(
            llm_provider, filter_config=self.filter_config
        )

        # 测试不同类型的memo文档
        memo_test_cases = [
            {
                "name": "工作笔记",
                "doc": self.create_test_memo_document(
                    title="项目会议纪要",
                    body="今日会议讨论了新功能开发计划：\n1. 用户界面优化\n2. 性能提升方案\n3. 安全性改进\n决定下周开始实施第一阶段。",
                ),
            },
            {
                "name": "聊天记录",
                "doc": self.create_test_memo_document(
                    title="Chat History with 团队成员 on 2025-01-15",
                    body="团队成员: 关于新项目的技术选型，我建议使用微服务架构。\n我: 同意，这样可以提高系统的可扩展性和维护性。",
                ),
            },
            {
                "name": "长内容memo",
                "doc": self.create_test_memo_document(
                    title="技术调研报告",
                    body=self.create_long_document_content(
                        repeat_count=2
                    ),  # 使用长内容
                ),
            },
        ]

        for i, test_case in enumerate(memo_test_cases):
            print(f"\n📋 Memo测试 {i+1}: {test_case['name']}")

            # 转换为RawData
            raw_data = await convert_memo_document_to_raw_data(test_case["doc"])

            print(f"   - 标题: {test_case['doc']['title']}")
            print(f"   - 内容长度: {len(test_case['doc']['body'])} 字符")
            print(f"   - 创建者ID: {test_case['doc']['creatorid']}")

            # 创建请求
            request = LinkDocMemCellExtractRequest(
                history_raw_data_list=[],
                new_raw_data_list=[raw_data],
                user_id_list=[self.test_user_id],
            )

            # 执行提取
            result = await extractor.extract_memcell(request)

            # 验证结果
            if result and result[0]:
                memcell, status_result = result
                print(f"✅ Memo提取成功:")
                print(f"   - 数据源: {memcell.source_type}")
                print(f"   - 文件名: {memcell.file_name}")
                print(f"   - 摘要: {memcell.summary}")

                # 验证memo特定字段
                assert memcell.source_type == "memo"
                assert memcell.type == RawDataType.LINKDOC
                assert memcell.file_name == test_case["doc"]["title"]

                # 验证参与者信息（包含创建者、读者、分享者）
                expected_participants = test_case["doc"].get(
                    "readerids", []
                ) + test_case["doc"].get("shareids", [])
                print(f"   - 参与者数量: {len(expected_participants)}")

            else:
                print(f"⚠️ Memo {test_case['name']} 未能提取MemCell")

    @pytest.mark.asyncio
    async def test_extractor_configuration(self):
        """测试提取器配置"""
        print("\n🧪 测试提取器配置")

        # 获取LLM Provider
        llm_provider = get_llm_provider()

        # 测试不同配置
        configs = [
            {"name": "默认配置", "config": FilterConfig()},
            {
                "name": "严格过滤配置",
                "config": FilterConfig(
                    enable_filtering=True,
                    min_content_length=100,
                    exclude_keywords=["测试", "demo"],
                ),
            },
            {
                "name": "小chunk配置",
                "config": FilterConfig(enable_filtering=False),
                "max_chars": 1000,
            },
        ]

        for config_test in configs:
            print(f"\n📋 测试 {config_test['name']}:")

            # 创建extractor
            kwargs = {'filter_config': config_test['config']}
            if 'max_chars' in config_test:
                kwargs['max_chars_per_chunk'] = config_test['max_chars']

            extractor = LinkDocMemCellExtractor(llm_provider, **kwargs)

            # 验证配置
            assert extractor.llm_provider is not None
            assert extractor.filter_config is not None

            if 'max_chars' in config_test:
                assert extractor.max_chars_per_chunk == config_test['max_chars']

            print(f"✅ {config_test['name']} 创建成功")

    @pytest.mark.asyncio
    async def test_token_statistics(self):
        """测试token统计功能"""
        print("\n🧪 测试token统计功能")

        # 使用带统计功能的LLM Provider
        llm_provider = get_llm_provider_with_stats()
        extractor = LinkDocMemCellExtractor(
            llm_provider, filter_config=self.filter_config
        )

        # 测试文档列表
        test_docs = [
            self.create_test_notion_document(
                title="技术文档1",
                content="这是一个技术文档，包含系统架构设计、数据库设计和API设计等内容。",
            ),
            self.create_test_google_document(
                name="用户手册1",
                content="这是用户操作手册，详细介绍了产品的各项功能和使用方法。",
            ),
            self.create_test_dropbox_document(
                name="项目报告1",
                content="这是项目完成后的总结报告，包含项目成果、经验教训和改进建议。",
            ),
        ]

        # 统计变量
        total_tokens = 0
        total_calls = 0
        file_stats = []

        for i, doc in enumerate(test_docs):
            print(
                f"\n📄 处理文档 {i+1}: {doc.get('title', doc.get('name', 'Unknown'))}"
            )

            # 转换为RawData
            if 'title' in doc:  # Notion文档
                raw_data = await convert_notion_document_to_raw_data(doc)
            elif 'name' in doc and 'explicitlyTrashed' in doc:  # Google文档
                raw_data = await convert_google_document_to_raw_data(doc)
            else:  # Dropbox文档
                raw_data = await convert_dropbox_document_to_raw_data(doc)

            # 创建请求
            user_id = self.test_user_id
            request = LinkDocMemCellExtractRequest(
                history_raw_data_list=[],
                new_raw_data_list=[raw_data],
                user_id_list=[user_id],
            )

            # 执行提取
            result = await extractor.extract_memcell(request)

            # 获取token统计
            current_stats = llm_provider.get_current_call_stats()
            if current_stats:
                tokens = current_stats.get('total_tokens', 0)
                total_tokens += tokens
                total_calls += 1

                file_stat = {
                    'file_name': doc.get('title', doc.get('name', f'Document_{i+1}')),
                    'tokens': tokens,
                    'prompt_tokens': current_stats.get('prompt_tokens', 0),
                    'completion_tokens': current_stats.get('completion_tokens', 0),
                }
                file_stats.append(file_stat)

                print(
                    f"   ✅ 成功: {tokens} tokens (Prompt: {current_stats.get('prompt_tokens', 0)}, Completion: {current_stats.get('completion_tokens', 0)})"
                )
            else:
                print(f"   ⚠️ 无token统计信息")

        # 输出统计结果
        print(f"\n📊 === TOKEN统计结果 ===")
        print(f"📁 处理文件数: {len(file_stats)}")
        print(f"🔄 API调用次数: {total_calls}")
        print(f"📝 总Token数: {total_tokens:,}")
        print(
            f"📈 平均每文件Token数: {total_tokens / len(file_stats) if file_stats else 0:.1f}"
        )
        print(
            f"📈 平均每次调用Token数: {total_tokens / total_calls if total_calls > 0 else 0:.1f}"
        )

        print(f"\n📋 === 文件详情 ===")
        for i, stat in enumerate(file_stats, 1):
            print(f"{i}. {stat['file_name']}: {stat['tokens']} tokens")

        # 验证统计结果
        assert total_calls > 0, "应该有API调用"
        assert total_tokens > 0, "应该有token使用"
        assert len(file_stats) == len(test_docs), "应该有对应数量的文件统计"


async def run_all_tests():
    """运行所有测试"""
    print("🚀 开始运行LinkDocMemCellExtractor测试")
    print("=" * 60)

    test_instance = TestLinkDocMemCellExtractor()

    try:
        # 运行测试方法
        test_instance.setup_method()
        await test_instance.test_basic_notion_extraction()

        test_instance.setup_method()
        await test_instance.test_multiple_data_sources()

        test_instance.setup_method()
        await test_instance.test_document_filtering()

        test_instance.setup_method()
        await test_instance.test_long_document_processing()

        test_instance.setup_method()
        await test_instance.test_error_handling()

        test_instance.setup_method()
        await test_instance.test_memo_extraction()

        test_instance.setup_method()
        await test_instance.test_extractor_configuration()

        test_instance.setup_method()
        await test_instance.test_token_statistics()

        print("\n" + "=" * 60)
        print("🎉 所有测试完成！")

    except Exception as e:
        logger.error(f"❌ 测试执行失败: {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    # 当直接运行此脚本时执行
    # 注意：通过 bootstrap.py 运行时，环境已经初始化完成
    asyncio.run(run_all_tests())
