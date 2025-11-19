#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 ChatGroupRawRepository 的功能

测试内容包括:
1. 基于group_id的查询操作 (get_by_id)
2. 基于orgId和departmentId的查询操作
3. 统计方法
4. 名称查找功能

注意：本测试只进行只读操作，不修改任何数据
"""

import asyncio

from core.di import get_bean_by_type
from common_utils.datetime_utils import to_iso_format
from infra_layer.adapters.out.persistence.repository.tanka.chat_group_raw_repository import (
    ChatGroupRawRepository,
)
from infra_layer.adapters.out.persistence.document.tanka.chat_group import ChatGroup
from core.observation.logger import get_logger

logger = get_logger(__name__)


async def test_get_by_id():
    """测试根据ID获取群组信息的功能"""
    logger.info("开始测试 get_by_id 方法...")

    repo = get_bean_by_type(ChatGroupRawRepository)

    # 测试用的群组ID列表 - 使用从数据库获取的真实ID
    test_group_ids = [
        "6807627893f1135b4cf1bd57",  # 2's Official Group
        "668df435281d945b3a279e95",  # Dream And Toy-拒绝回复's Official Group
        "66e195b933932f1e3dacdfab",  # Uat--行走1-QA's Official Group
        "66742f0ab6040b71945c4833",  # 继承-2's Official Group
        "67af24fb7c713806173414d8",  # UAT-GPT公司付费账号's Official Group
        "6305ea754622a03cdb7b67b1",  # 百山大, 🐸精灵备注 (tempName)
        "671f28c8b723e602e0a2707f",  # waitlist's Official Group
        "67ecef7d1240477f52a87859",  # 哈哈哈's Official Group
    ]

    found_groups = []

    try:
        for group_id in test_group_ids:
            logger.info("🔍 测试群组ID: %s", group_id)

            # 测试 get_by_id 方法
            result = await repo.get_by_id(group_id)

            if result:
                logger.info(
                    "✅ 找到群组: ID=%s, Name=%s, OrgId=%s",
                    result.id,
                    result.name,
                    result.orgId,
                )
                found_groups.append(result)

                # 验证基本字段
                assert (
                    str(result.id) == group_id
                ), f"ID不匹配: 期望{group_id}, 实际{result.id}"
                assert isinstance(result, ChatGroup), "返回对象类型不正确"

                # 如果有名称，测试 get_name_by_id 方法
                if result.name:
                    name = await repo.get_name_by_id(group_id)
                    assert (
                        name == result.name
                    ), f"名称不匹配: 期望{result.name}, 实际{name}"
                    logger.info("✅ get_name_by_id 验证成功: %s", name)

            else:
                logger.info("ℹ️  未找到群组: %s", group_id)

        logger.info("✅ get_by_id 测试完成，共找到 %d 个群组", len(found_groups))
        return found_groups

    except Exception as e:
        logger.error("❌ 测试 get_by_id 失败: %s", e)
        raise


async def test_get_by_orgId():
    """测试根据组织ID获取群组列表"""
    logger.info("开始测试 get_by_orgId 方法...")

    repo = get_bean_by_type(ChatGroupRawRepository)

    # 测试用的组织ID列表 - 使用从数据库获取的真实orgId
    test_org_ids = [
        "68076276972b91319ff0de8c",  # 2's Official Group的组织
        "6662c01f747e7808194846b9",  # Dream And Toy等群组的组织
        "67af24f92d69443025cd80b1",  # UAT-GPT公司付费账号的组织
        "6601416bf33c96c5a32fdd20",  # 百山大等群组的组织
        "671f28c8c1085b4f6006a7da",  # waitlist's Official Group的组织
    ]

    try:
        for org_id in test_org_ids:
            logger.info("🔍 测试组织ID: %s", org_id)

            # 不限制状态
            groups = await repo.get_by_orgId(org_id)
            logger.info("ℹ️  组织 %s 下共有 %d 个群组", org_id, len(groups))

            # 限制状态为1（假设1是活跃状态）
            active_groups = await repo.get_by_orgId(org_id, status=1)
            logger.info("ℹ️  组织 %s 下共有 %d 个活跃群组", org_id, len(active_groups))

            # 限制返回数量
            limited_groups = await repo.get_by_orgId(org_id, limit=3)
            logger.info("ℹ️  组织 %s 下限制返回 %d 个群组", org_id, len(limited_groups))

            # 验证返回的群组都属于该组织
            for group in groups:
                if group.orgId:
                    assert (
                        str(group.orgId) == org_id
                    ), f"群组组织ID不匹配: 期望{org_id}, 实际{group.orgId}"

            if groups:
                logger.info("✅ 找到组织 %s 的群组数据", org_id)
                break

        logger.info("✅ get_by_orgId 测试完成")

    except Exception as e:
        logger.error("❌ 测试 get_by_orgId 失败: %s", e)
        raise


async def test_get_by_departmentId():
    """测试根据部门ID获取群组列表"""
    logger.info("开始测试 get_by_departmentId 方法...")

    repo = get_bean_by_type(ChatGroupRawRepository)

    # 测试用的部门ID列表 - 使用从数据库获取的真实departmentId
    test_dept_ids = [
        "68076276972b91319ff0de8c",  # 2's Official Group的部门
        "6662c01f747e7808194846b9",  # Dream And Toy等群组的部门
        "66e1938bbe0867039977a283",  # Uat--行走1-QA's Official Group的部门
        "66742edae6415f0848e1aafa",  # 继承-2's Official Group的部门
        "67af24f92d69443025cd80b1",  # UAT-GPT公司付费账号的部门
    ]

    try:
        for dept_id in test_dept_ids:
            logger.info("🔍 测试部门ID: %s", dept_id)

            # 不限制状态
            groups = await repo.get_by_departmentId(dept_id)
            logger.info("ℹ️  部门 %s 下共有 %d 个群组", dept_id, len(groups))

            # 限制状态为1
            active_groups = await repo.get_by_departmentId(dept_id, status=1)
            logger.info("ℹ️  部门 %s 下共有 %d 个活跃群组", dept_id, len(active_groups))

            # 限制返回数量
            limited_groups = await repo.get_by_departmentId(dept_id, limit=5)
            logger.info("ℹ️  部门 %s 下限制返回 %d 个群组", dept_id, len(limited_groups))

            # 验证返回的群组都属于该部门
            for group in groups:
                if group.departmentId:
                    assert (
                        str(group.departmentId) == dept_id
                    ), f"群组部门ID不匹配: 期望{dept_id}, 实际{group.departmentId}"

            if groups:
                logger.info("✅ 找到部门 %s 的群组数据", dept_id)
                break

        logger.info("✅ get_by_departmentId 测试完成")

    except Exception as e:
        logger.error("❌ 测试 get_by_departmentId 失败: %s", e)
        raise


async def test_find_by_name():
    """测试根据名称查找群组"""
    logger.info("开始测试 find_by_name 方法...")

    repo = get_bean_by_type(ChatGroupRawRepository)

    # 测试用的群组名称关键字 - 基于真实数据中的名称
    test_names = [
        "Official",  # 大部分群组都有"Official Group"
        "Group",  # 群组关键字
        "测试",  # "测试水水水水水水", "测试's Official Group"
        "UAT",  # "UAT-GPT公司付费账号", "Uat--行走1-QA"
        "GPT",  # "UAT-GPT公司付费账号"
        "哈哈",  # "哈哈哈's Official Group"
        "Dream",  # "Dream And Toy-拒绝回复"
        "waitlist",  # "waitlist's Official Group"
    ]

    try:
        for name in test_names:
            logger.info("🔍 测试名称关键字: %s", name)

            # 不限制组织
            groups = await repo.find_by_name(name)
            logger.info("ℹ️  包含 '%s' 的群组共有 %d 个", name, len(groups))

            # 限制组织（使用第一个找到的组织ID）
            if groups and groups[0].orgId:
                org_groups = await repo.find_by_name(name, orgId=groups[0].orgId)
                logger.info(
                    "ℹ️  在组织 %s 中包含 '%s' 的群组共有 %d 个",
                    groups[0].orgId,
                    name,
                    len(org_groups),
                )

            # 验证返回的群组名称都包含搜索关键字
            for group in groups:
                if group.name:
                    assert (
                        name.lower() in group.name.lower()
                    ), f"群组名称不包含关键字: {group.name} 不包含 {name}"

            if groups:
                logger.info("✅ 找到包含 '%s' 的群组数据", name)
                # 显示前几个结果
                for i, group in enumerate(groups[:3]):
                    logger.info(
                        "  - 群组 %d: ID=%s, Name=%s", i + 1, group.id, group.name
                    )
                break

        logger.info("✅ find_by_name 测试完成")

    except Exception as e:
        logger.error("❌ 测试 find_by_name 失败: %s", e)
        raise


async def test_comprehensive_group_info():
    """综合测试：获取一个群组的完整信息"""
    logger.info("开始综合测试群组信息...")

    repo = get_bean_by_type(ChatGroupRawRepository)

    try:
        # 首先尝试找到一个存在的群组
        found_group = None

        # 方法1: 通过名称搜索找到群组
        common_names = ["group", "chat", "team", "技术", "讨论"]
        for name in common_names:
            groups = await repo.find_by_name(name)
            if groups:
                found_group = groups[0]
                logger.info("✅ 通过名称 '%s' 找到测试群组: %s", name, found_group.id)
                break

        if not found_group:
            logger.info("ℹ️  未找到可用的测试群组，跳过综合测试")
            return

        group_id = found_group.id
        logger.info("🔍 开始测试群组: %s", group_id)

        # 测试 get_by_id
        group = await repo.get_by_id(group_id)
        assert group is not None, "get_by_id 应该返回群组信息"
        assert group.id == group_id, "群组ID不匹配"

        # 测试 get_name_by_id
        name = await repo.get_name_by_id(group_id)
        if group.name:
            assert name == group.name, f"群组名称不匹配: 期望{group.name}, 实际{name}"
            logger.info("✅ 群组名称: %s", name)
        elif name is None:
            logger.info("ℹ️  群组无名称或名称为空")

        # 显示群组详细信息
        logger.info("📋 群组详细信息:")
        logger.info("  - ID: %s", group.id)
        logger.info("  - 名称: %s", group.name)
        logger.info("  - 状态: %s", group.status)
        logger.info("  - 类型: %s", group.groupType)
        logger.info("  - 子类型: %s", group.groupSubType)
        logger.info("  - 组织ID: %s", group.orgId)
        logger.info("  - 部门ID: %s", group.departmentId)
        logger.info("  - 团队ID: %s", group.teamId)
        logger.info("  - 成员数量: %s", group.count)
        logger.info("  - 创建者: %s", group.createBy)
        logger.info(
            "  - 创建时间: %s",
            to_iso_format(group.createTime) if group.createTime else None,
        )
        logger.info(
            "  - 更新时间: %s",
            to_iso_format(group.updateTime) if group.updateTime else None,
        )

        # 如果有组织ID，测试相关查询
        if group.orgId:
            org_groups = await repo.get_by_orgId(group.orgId, limit=5)
            logger.info(
                "✅ 该群组所属组织 %s 共有 %d 个群组", group.orgId, len(org_groups)
            )

        # 如果有部门ID，测试相关查询
        if group.departmentId:
            dept_groups = await repo.get_by_departmentId(group.departmentId, limit=5)
            logger.info(
                "✅ 该群组所属部门 %s 共有 %d 个群组",
                group.departmentId,
                len(dept_groups),
            )

        logger.info("✅ 综合测试完成")

    except Exception as e:
        logger.error("❌ 综合测试失败: %s", e)
        raise


async def run_all_tests():
    """运行所有测试"""
    logger.info("🚀 开始运行 ChatGroupRawRepository 所有测试...")

    try:
        # 重点测试 get_by_id 接口
        await test_get_by_id()

        # 其他查询方法测试
        await test_get_by_orgId()
        await test_get_by_departmentId()
        await test_find_by_name()

        # 综合测试
        await test_comprehensive_group_info()

        logger.info("✅ 所有测试完成")

        # 总结测试结果
        logger.info("📊 测试总结:")
        logger.info("  - 所有测试均为只读操作，未修改任何数据")
        logger.info("  - 重点测试了 get_by_id 接口的功能")
        logger.info("  - 验证了各种查询方法的正确性")

    except Exception as e:
        logger.error("❌ 测试过程中出现错误: %s", e)
        raise


if __name__ == "__main__":
    asyncio.run(run_all_tests())
