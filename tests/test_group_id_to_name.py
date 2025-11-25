#!/usr/bin/env python3
"""
群组ID到名称转换功能的真实数据库测试

测试 ChatGroupRawRepository.get_name_by_id 方法，该方法在 tanka_memorize.py 中用于
根据群组ID获取群组名称进行对话处理。

使用方法:
    uv run python src/bootstrap.py tests/test_group_id_to_name.py                    # 运行所有测试
    uv run python src/bootstrap.py tests/test_group_id_to_name.py --group-id <id>    # 测试特定群组ID
    uv run python src/bootstrap.py tests/test_group_id_to_name.py --list-groups      # 列出可用的群组
    uv run python src/bootstrap.py tests/test_group_id_to_name.py --performance      # 性能测试
"""

import asyncio
import sys
import argparse
import time
from datetime import datetime
from typing import Optional, List
from pathlib import Path

# 导入依赖
from infra_layer.adapters.out.persistence.repository.tanka.chat_group_raw_repository import (
    ChatGroupRawRepository,
)
from infra_layer.adapters.out.persistence.document.tanka.chat_group import ChatGroup
from api_specs.types import RawDataType
from core.di import get_bean_by_type
from core.observation.logger import get_logger
from beanie import PydanticObjectId
from bson.errors import InvalidId

logger = get_logger(__name__)


class GroupIdToNameTester:
    """群组ID到名称转换功能的测试类"""

    def __init__(self):
        self.group_repo: Optional[ChatGroupRawRepository] = None
        self.test_results = {'passed': 0, 'failed': 0, 'total': 0, 'details': []}

    async def initialize(self):
        """初始化测试环境"""
        try:
            logger.info("🔧 初始化群组仓库...")
            self.group_repo = get_bean_by_type(ChatGroupRawRepository)
            logger.info("✅ 群组仓库初始化成功")
        except Exception as e:
            logger.error(f"❌ 群组仓库初始化失败: {e}")
            raise

    async def test_basic_functionality(self, group_id: str = None):
        """测试基本功能"""
        logger.info("🧪 开始基本功能测试...")

        if group_id:
            # 测试指定的群组ID
            await self._test_group_id(group_id)
        else:
            # 获取一些真实的群组ID进行测试
            test_groups = await self._get_sample_groups()
            if not test_groups:
                logger.warning("⚠️  数据库中没有找到群组数据，跳过基本功能测试")
                return

            for group in test_groups[:5]:  # 测试前5个群组
                await self._test_group_id(str(group.id))

    async def _test_group_id(self, group_id: str):
        """测试单个群组ID"""
        test_name = f"测试群组ID: {group_id}"
        self.test_results['total'] += 1

        try:
            start_time = time.time()

            # 执行查询
            group_name = await self.group_repo.get_name_by_id(group_id)

            end_time = time.time()
            query_time = (end_time - start_time) * 1000  # 转换为毫秒

            if group_name:
                logger.info(
                    f"✅ {test_name} -> '{group_name}' (耗时: {query_time:.2f}ms)"
                )
                self.test_results['passed'] += 1
                self.test_results['details'].append(
                    {
                        'test': test_name,
                        'status': 'PASS',
                        'result': group_name,
                        'time_ms': query_time,
                    }
                )
            else:
                logger.warning(f"⚠️  {test_name} -> 未找到群组名称")
                self.test_results['details'].append(
                    {
                        'test': test_name,
                        'status': 'NO_NAME',
                        'result': None,
                        'time_ms': query_time,
                    }
                )

        except Exception as e:
            logger.error(f"❌ {test_name} 失败: {e}")
            self.test_results['failed'] += 1
            self.test_results['details'].append(
                {'test': test_name, 'status': 'FAIL', 'error': str(e), 'time_ms': 0}
            )

    async def test_invalid_cases(self):
        """测试异常情况"""
        logger.info("🧪 开始异常情况测试...")

        # 测试用例
        invalid_cases = [
            ("无效ObjectId格式", "invalid_id_format"),
            ("空字符串", ""),
            ("不存在的ObjectId", "507f1f77bcf86cd799439999"),
            ("24位但无效的hex", "gggggggggggggggggggggggg"),
        ]

        for case_name, test_id in invalid_cases:
            self.test_results['total'] += 1
            try:
                start_time = time.time()
                result = await self.group_repo.get_name_by_id(test_id)
                end_time = time.time()
                query_time = (end_time - start_time) * 1000

                if result is None:
                    logger.info(
                        f"✅ {case_name}: 正确返回None (耗时: {query_time:.2f}ms)"
                    )
                    self.test_results['passed'] += 1
                else:
                    logger.warning(f"⚠️  {case_name}: 期望None但得到 '{result}'")

                self.test_results['details'].append(
                    {
                        'test': case_name,
                        'status': 'PASS' if result is None else 'UNEXPECTED',
                        'result': result,
                        'time_ms': query_time,
                    }
                )

            except Exception as e:
                logger.error(f"❌ {case_name} 测试失败: {e}")
                self.test_results['failed'] += 1
                self.test_results['details'].append(
                    {'test': case_name, 'status': 'FAIL', 'error': str(e), 'time_ms': 0}
                )

    async def test_tanka_memorize_integration(self):
        """测试与tanka_memorize.py的集成模式"""
        logger.info("🧪 开始tanka_memorize集成测试...")

        # 模拟tanka_memorize中的使用模式
        class MockRequest:
            def __init__(self, raw_data_type, group_id):
                self.raw_data_type = raw_data_type
                self.group_id = group_id

        # 获取一个真实的群组进行测试
        test_groups = await self._get_sample_groups()
        if not test_groups:
            logger.warning("⚠️  没有可用的群组数据，跳过集成测试")
            return

        test_group = test_groups[0]
        test_group_id = str(test_group.id)

        # 测试场景1: 对话类型 + 有群组ID
        self.test_results['total'] += 1
        try:
            request = MockRequest(RawDataType.CONVERSATION, test_group_id)

            if request.raw_data_type == RawDataType.CONVERSATION and request.group_id:
                now = time.time()
                logger.debug(
                    f"[集成测试] 获取群组名称开始: group_id={request.group_id}"
                )

                group_name = await self.group_repo.get_name_by_id(request.group_id)

                logger.debug(f"[集成测试] 获取群组名称耗时: {time.time() - now:.4f}秒")

                if group_name:
                    logger.info(
                        f"✅ 集成测试-对话场景: 成功获取群组名称 '{group_name}'"
                    )
                    self.test_results['passed'] += 1
                else:
                    logger.warning("⚠️  集成测试-对话场景: 群组名称为空")

                self.test_results['details'].append(
                    {
                        'test': '集成测试-对话场景',
                        'status': 'PASS' if group_name else 'NO_NAME',
                        'result': group_name,
                        'time_ms': (time.time() - now) * 1000,
                    }
                )

        except Exception as e:
            logger.error(f"❌ 集成测试-对话场景失败: {e}")
            self.test_results['failed'] += 1

        # 测试场景2: 非对话类型
        self.test_results['total'] += 1
        try:
            request = MockRequest(RawDataType.EMAIL, test_group_id)

            if request.raw_data_type == RawDataType.CONVERSATION and request.group_id:
                group_name = await self.group_repo.get_name_by_id(request.group_id)
            else:
                group_name = None

            if group_name is None:
                logger.info("✅ 集成测试-非对话场景: 正确跳过群组名称获取")
                self.test_results['passed'] += 1
            else:
                logger.warning("⚠️  集成测试-非对话场景: 期望跳过但执行了查询")

        except Exception as e:
            logger.error(f"❌ 集成测试-非对话场景失败: {e}")
            self.test_results['failed'] += 1

    async def test_performance(self, concurrent_requests: int = 10):
        """性能测试"""
        logger.info(f"🧪 开始性能测试 ({concurrent_requests}个并发请求)...")

        # 获取测试群组
        test_groups = await self._get_sample_groups()
        if not test_groups:
            logger.warning("⚠️  没有可用的群组数据，跳过性能测试")
            return

        test_group_id = str(test_groups[0].id)

        self.test_results['total'] += 1
        try:
            start_time = time.time()

            # 创建并发任务
            tasks = []
            for i in range(concurrent_requests):
                task = self.group_repo.get_name_by_id(test_group_id)
                tasks.append(task)

            # 并发执行
            results = await asyncio.gather(*tasks, return_exceptions=True)

            end_time = time.time()
            total_time = end_time - start_time
            avg_time = (total_time / concurrent_requests) * 1000  # 毫秒

            # 统计结果
            success_count = sum(1 for r in results if isinstance(r, str) and r)
            error_count = sum(1 for r in results if isinstance(r, Exception))

            logger.info(f"✅ 性能测试完成:")
            logger.info(f"   📊 总耗时: {total_time:.3f}秒")
            logger.info(f"   📊 平均耗时: {avg_time:.2f}ms/请求")
            logger.info(f"   📊 成功请求: {success_count}/{concurrent_requests}")
            logger.info(f"   📊 失败请求: {error_count}/{concurrent_requests}")

            if error_count == 0 and total_time < 5.0:  # 5秒内完成
                self.test_results['passed'] += 1
                self.test_results['details'].append(
                    {
                        'test': f'性能测试-{concurrent_requests}并发',
                        'status': 'PASS',
                        'total_time_s': total_time,
                        'avg_time_ms': avg_time,
                        'success_rate': f"{success_count}/{concurrent_requests}",
                    }
                )
            else:
                self.test_results['failed'] += 1

        except Exception as e:
            logger.error(f"❌ 性能测试失败: {e}")
            self.test_results['failed'] += 1

    async def _get_sample_groups(self, limit: int = 10) -> List[ChatGroup]:
        """获取一些示例群组"""
        try:
            # 使用仓库的find方法获取群组
            groups = await self.group_repo.model.find(
                ChatGroup.name != None, limit=limit  # 只获取有名称的群组
            ).to_list()

            logger.debug(f"📊 找到 {len(groups)} 个有名称的群组")
            return groups

        except Exception as e:
            logger.error(f"❌ 获取示例群组失败: {e}")
            return []

    async def list_available_groups(self, limit: int = 20):
        """列出可用的群组"""
        logger.info(f"📋 列出前{limit}个可用群组...")

        try:
            groups = await self._get_sample_groups(limit)

            if not groups:
                logger.warning("⚠️  数据库中没有找到群组数据")
                return

            logger.info(f"📊 找到 {len(groups)} 个群组:")
            logger.info("=" * 80)

            for i, group in enumerate(groups, 1):
                created_time = (
                    group.createTime.strftime("%Y-%m-%d %H:%M:%S")
                    if group.createTime
                    else "未知"
                )
                member_count = group.memberCount or "未知"

                logger.info(f"{i:2d}. ID: {group.id}")
                logger.info(f"    名称: {group.name or '(无名称)'}")
                logger.info(f"    成员数: {member_count}")
                logger.info(f"    创建时间: {created_time}")
                logger.info(f"    状态: {group.status or '未知'}")
                logger.info("-" * 60)

        except Exception as e:
            logger.error(f"❌ 列出群组失败: {e}")

    def print_summary(self):
        """打印测试总结"""
        logger.info("=" * 80)
        logger.info("📊 测试总结")
        logger.info("=" * 80)

        total = self.test_results['total']
        passed = self.test_results['passed']
        failed = self.test_results['failed']

        if total > 0:
            pass_rate = (passed / total) * 100
            logger.info(f"总测试数: {total}")
            logger.info(f"通过数: {passed}")
            logger.info(f"失败数: {failed}")
            logger.info(f"通过率: {pass_rate:.1f}%")

            if pass_rate >= 80:
                logger.info("🎉 测试结果: 优秀!")
            elif pass_rate >= 60:
                logger.info("👍 测试结果: 良好")
            else:
                logger.info("⚠️  测试结果: 需要改进")
        else:
            logger.info("⚠️  没有执行任何测试")


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="群组ID到名称转换功能测试")
    parser.add_argument('--group-id', type=str, help='测试指定的群组ID')
    parser.add_argument('--list-groups', action='store_true', help='列出可用的群组')
    parser.add_argument('--performance', action='store_true', help='执行性能测试')
    parser.add_argument(
        '--concurrent', type=int, default=10, help='性能测试的并发数 (默认10)'
    )

    args = parser.parse_args()

    logger.info("🚀 开始群组ID到名称转换功能测试...")
    logger.info("=" * 80)

    tester = GroupIdToNameTester()

    try:
        # 初始化
        await tester.initialize()

        if args.list_groups:
            # 列出可用群组
            await tester.list_available_groups()
        else:
            # 运行功能测试
            await tester.test_basic_functionality(args.group_id)
            await tester.test_invalid_cases()
            await tester.test_tanka_memorize_integration()

            if args.performance:
                await tester.test_performance(args.concurrent)

        # 打印总结
        if not args.list_groups:
            tester.print_summary()

    except Exception as e:
        logger.error(f"❌ 测试执行失败: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
