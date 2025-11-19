#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 ThirdPartyOAuthTokenRawRepository 的功能

测试内容包括:
1. 基于app和thirdUserId的查询操作（特别是get_userIds_by_app_and_thirdUserId接口）
2. 其他查询方法测试
3. 统计方法测试

测试数据来源：
- 使用 db_ai_habitat_test.thirdparty_oauth_token 集合中的真实数据
- 包含 Gmail、Outlook、Google Calendar 等应用的实际OAuth Token记录
- 测试数据包括已激活、过期等不同状态的Token

参数类型兼容性：
- ThirdPartyApp 和 TokenStatus 枚举仅作为参考，不强制验证
- 支持传入枚举类型（如 ThirdPartyApp.GMAIL）或字符串类型（如 "gmail"）
- 测试同时验证两种参数类型的兼容性

注意：本测试只进行只读操作，不修改数据库数据
"""

import asyncio

from core.di import get_bean_by_type
from common_utils.datetime_utils import to_iso_format
from infra_layer.adapters.out.persistence.repository.tanka.thirdparty_oauth_token_raw_repository import (
    ThirdPartyOAuthTokenRawRepository,
)
from infra_layer.adapters.out.persistence.document.tanka.thirdparty_oauth_token import (
    ThirdPartyApp,
    TokenStatus,
)
from core.observation.logger import get_logger

logger = get_logger(__name__)


async def test_get_userIds_by_app_and_thirdUserId():
    """测试 get_userIds_by_app_and_thirdUserId 方法（重点测试）"""
    logger.info("开始测试 get_userIds_by_app_and_thirdUserId 方法...")

    repo = get_bean_by_type(ThirdPartyOAuthTokenRawRepository)

    try:
        # 测试用例：使用数据库中的真实数据
        # 基于 db_ai_habitat_test.thirdparty_oauth_token 集合中的实际数据
        # 测试枚举类型和字符串类型的兼容性
        test_cases = [
            {
                "app": ThirdPartyApp.GMAIL,
                "thirdUserId": "shandatanka@gmail.com",
                "description": "Gmail应用 - 使用枚举类型",
            },
            {
                "app": "outlook",
                "thirdUserId": "wangnoora@outlook.com",
                "description": "Outlook应用 - 使用字符串类型",
            },
            {
                "app": ThirdPartyApp.OUTLOOK,
                "thirdUserId": "luwuchao@tanka.ai",
                "description": "Outlook应用 - 使用枚举类型",
            },
            {
                "app": "googleCalendar",
                "thirdUserId": "hxxiaoxiongmao@gmail.com",
                "description": "Google Calendar应用 - 使用字符串类型",
            },
            {
                "app": ThirdPartyApp.GOOGLE_CALENDAR,
                "thirdUserId": "ahukpyu@gmail.com",
                "description": "Google Calendar应用 - 使用枚举类型",
            },
            # 测试不存在的数据
            {
                "app": "slack",
                "thirdUserId": "nonexistent@test.com",
                "description": "Slack应用 - 字符串类型，不存在的数据测试",
            },
            {
                "app": ThirdPartyApp.NOTION,
                "thirdUserId": "fake-notion-user",
                "description": "Notion应用 - 枚举类型，不存在的数据测试",
            },
        ]

        for case in test_cases:
            logger.info("测试用例: %s", case["description"])

            # 调用目标方法
            user_ids = await repo.get_userIds_by_app_and_thirdUserId(
                app=case["app"], thirdUserId=case["thirdUserId"]
            )

            # 验证返回结果
            assert isinstance(user_ids, list), "返回结果应该是列表类型"
            logger.info(
                "✅ 获取用户ID列表: app=%s, thirdUserId=%s, userIds=%s",
                case["app"],
                case["thirdUserId"],
                user_ids,
            )

            if user_ids:
                logger.info("  - 找到 %d 个关联用户ID", len(user_ids))
                for i, user_id in enumerate(user_ids):
                    logger.info("  - 用户ID[%d]: %s", i + 1, user_id)
            else:
                logger.info("  - 未找到激活状态的OAuth Token或无关联用户")

        logger.info("✅ get_userIds_by_app_and_thirdUserId 方法测试完成")

    except Exception as e:
        logger.error("❌ 测试 get_userIds_by_app_and_thirdUserId 方法失败: %s", e)
        raise


async def test_get_by_app_and_thirdUserId():
    """测试 get_by_app_and_thirdUserId 方法"""
    logger.info("开始测试 get_by_app_and_thirdUserId 方法...")

    repo = get_bean_by_type(ThirdPartyOAuthTokenRawRepository)

    try:
        # 测试不同应用的查询，使用真实数据，混合使用枚举和字符串
        test_cases = [
            (ThirdPartyApp.GMAIL, "weilinwang65@gmail.com"),
            ("outlook", "yukunpeng@tanka.ai"),
            (ThirdPartyApp.OUTLOOK, "luwuchao@tanka.ai"),
            ("googleCalendar", "hxxiaoxiongmao@gmail.com"),
            (ThirdPartyApp.GOOGLE_CALENDAR, "ahukpyu@gmail.com"),
            # 测试不存在的数据
            ("slack", "nonexistent_user"),
            (ThirdPartyApp.NOTION, "fake_notion_user"),
        ]

        for app, test_third_user_id in test_cases:

            result = await repo.get_by_app_and_thirdUserId(
                app=app, thirdUserId=test_third_user_id
            )

            if result:
                logger.info(
                    "✅ 找到OAuth Token: app=%s, thirdUserId=%s",
                    app,
                    test_third_user_id,
                )
                logger.info("  - Token ID: %s", result.id)
                logger.info(
                    "  - 状态: %s", result.status if result.status else "未设置"
                )
                logger.info(
                    "  - 关联用户数量: %d", len(result.userIds) if result.userIds else 0
                )
                if result.accessTokenExpireTime:
                    logger.info(
                        "  - 访问令牌过期时间: %s",
                        to_iso_format(result.accessTokenExpireTime),
                    )
            else:
                logger.info(
                    "ℹ️  未找到OAuth Token: app=%s, thirdUserId=%s",
                    app,
                    test_third_user_id,
                )

        logger.info("✅ get_by_app_and_thirdUserId 方法测试完成")

    except Exception as e:
        logger.error("❌ 测试 get_by_app_and_thirdUserId 方法失败: %s", e)
        raise


async def test_get_by_app():
    """测试 get_by_app 方法"""
    logger.info("开始测试 get_by_app 方法...")

    repo = get_bean_by_type(ThirdPartyOAuthTokenRawRepository)

    try:
        # 测试不同应用的查询，使用数据库中实际存在的应用，混合使用枚举和字符串
        test_apps = [ThirdPartyApp.GMAIL, "outlook", ThirdPartyApp.GOOGLE_CALENDAR]

        for app in test_apps:
            # 不带状态过滤的查询
            tokens = await repo.get_by_app(app=app, limit=5)
            logger.info(
                "✅ 查询应用Token (无状态过滤): app=%s, count=%d", app, len(tokens)
            )

            # 带状态过滤的查询 - 混合使用枚举和字符串
            app_str = str(app) if not hasattr(app, 'value') else app.value
            if app_str == "gmail":
                activated_tokens = await repo.get_by_app(
                    app=app, status=TokenStatus.ACTIVATED, limit=5
                )
            else:
                activated_tokens = await repo.get_by_app(
                    app=app, status="activated", limit=5
                )
            logger.info(
                "✅ 查询应用Token (激活状态): app=%s, count=%d",
                app,
                len(activated_tokens),
            )

            # 显示一些详细信息
            for i, token in enumerate(activated_tokens[:2]):  # 只显示前2个
                logger.info(
                    "  - Token[%d]: thirdUserId=%s, userIds=%s",
                    i + 1,
                    token.thirdUserId,
                    token.userIds,
                )

        logger.info("✅ get_by_app 方法测试完成")

    except Exception as e:
        logger.error("❌ 测试 get_by_app 方法失败: %s", e)
        raise


async def test_find_by_user_id():
    """测试 find_by_user_id 方法"""
    logger.info("开始测试 find_by_user_id 方法...")

    repo = get_bean_by_type(ThirdPartyOAuthTokenRawRepository)

    try:
        # 测试用户ID列表，使用数据库中实际存在的用户ID
        test_user_ids = [
            "678f3395f1d74b27bbc26c1b",  # weilinwang65@gmail.com 关联的用户
            "6858fb5c172bdc3cf84e90af",  # yukunpeng@tanka.ai 关联的用户
            "688b1e444b6cd02fc3b4e216",  # yukunpeng@tanka.ai 关联的另一个用户
            "6790a3d51c84af0edde0ddbf",  # luwuchao@tanka.ai 关联的用户
            "66f104d7c523ee7df3999fea",  # luwuchao@tanka.ai 关联的另一个用户
            "nonexistent_user_id",  # 不存在的用户ID测试
        ]

        for user_id in test_user_ids:
            # 不带应用过滤的查询
            tokens = await repo.find_by_user_id(user_id=user_id)
            logger.info(
                "✅ 根据用户ID查询Token (无应用过滤): user_id=%s, count=%d",
                user_id,
                len(tokens),
            )

            if tokens:
                # 显示找到的应用信息
                apps = [token.app for token in tokens]
                logger.info("  - 关联的应用: %s", ", ".join(set(apps)))

            # 带应用过滤的查询
            gmail_tokens = await repo.find_by_user_id(
                user_id=user_id, app=ThirdPartyApp.GMAIL, status=TokenStatus.ACTIVATED
            )
            if gmail_tokens:
                logger.info(
                    "✅ 根据用户ID查询Gmail Token (激活状态): user_id=%s, count=%d",
                    user_id,
                    len(gmail_tokens),
                )

        logger.info("✅ find_by_user_id 方法测试完成")

    except Exception as e:
        logger.error("❌ 测试 find_by_user_id 方法失败: %s", e)
        raise


async def test_get_app_and_thirdUserId_by_userid():
    """测试 get_app_and_thirdUserId_by_userid 方法"""
    logger.info("开始测试 get_app_and_thirdUserId_by_userid 方法...")

    repo = get_bean_by_type(ThirdPartyOAuthTokenRawRepository)

    try:
        # 测试用户ID列表，使用数据库中实际存在的用户ID
        test_user_ids = [
            "678f3395f1d74b27bbc26c1b",  # weilinwang65@gmail.com 关联的用户
            "6858fb5c172bdc3cf84e90af",  # yukunpeng@tanka.ai 关联的用户
            "688b1e444b6cd02fc3b4e216",  # yukunpeng@tanka.ai 关联的另一个用户
            "6790a3d51c84af0edde0ddbf",  # luwuchao@tanka.ai 关联的用户
            "66f104d7c523ee7df3999fea",  # luwuchao@tanka.ai 关联的另一个用户
            "nonexistent_user_id",  # 不存在的用户ID测试
        ]

        for user_id in test_user_ids:
            app_third_user_pairs = await repo.get_app_and_thirdUserId_by_userid(
                user_id=user_id
            )

            logger.info(
                "✅ 根据用户ID获取应用和第三方用户ID: user_id=%s, count=%d",
                user_id,
                len(app_third_user_pairs),
            )

            if app_third_user_pairs:
                logger.info("  关联的应用和第三方用户ID:")
                for i, (app, third_user_id) in enumerate(app_third_user_pairs):
                    logger.info(
                        "  - [%d] app=%s, thirdUserId=%s", i + 1, app, third_user_id
                    )
            else:
                logger.info("  - 未找到关联的应用和第三方用户ID")

        logger.info("✅ get_app_and_thirdUserId_by_userid 方法测试完成")

    except Exception as e:
        logger.error("❌ 测试 get_app_and_thirdUserId_by_userid 方法失败: %s", e)
        raise


async def test_statistics():
    """测试统计方法（注意：count_by_app方法已被移除）"""
    logger.info("开始测试统计方法...")

    repo = get_bean_by_type(ThirdPartyOAuthTokenRawRepository)

    try:
        # 由于count_by_app方法已被移除，这里改为通过get_by_app来统计
        test_apps = [
            ThirdPartyApp.GMAIL,
            ThirdPartyApp.OUTLOOK,
            ThirdPartyApp.GOOGLE_CALENDAR,
        ]

        total_tokens = 0
        for app in test_apps:
            # 通过查询所有Token来统计总数
            all_tokens = await repo.get_by_app(app=app)
            total_tokens += len(all_tokens)
            logger.info("✅ 应用Token总数: app=%s, count=%d", app, len(all_tokens))

            # 统计激活状态的Token
            activated_tokens = await repo.get_by_app(
                app=app, status=TokenStatus.ACTIVATED
            )
            logger.info(
                "✅ 应用激活Token数: app=%s, count=%d", app, len(activated_tokens)
            )

            # 统计过期状态的Token
            expired_tokens = await repo.get_by_app(app=app, status=TokenStatus.EXPIRED)
            logger.info(
                "✅ 应用过期Token数: app=%s, count=%d", app, len(expired_tokens)
            )

        logger.info("✅ 所有测试应用Token总数: %d", total_tokens)
        logger.info("✅ 统计方法测试完成")

    except Exception as e:
        logger.error("❌ 测试统计方法失败: %s", e)
        raise


async def test_get_by_id():
    """测试 get_by_id 方法"""
    logger.info("开始测试 get_by_id 方法...")

    repo = get_bean_by_type(ThirdPartyOAuthTokenRawRepository)

    try:
        # 首先获取一些现有的Token来测试
        gmail_tokens = await repo.get_by_app(app=ThirdPartyApp.GMAIL, limit=2)

        if gmail_tokens:
            for token in gmail_tokens:
                # 测试根据ID获取
                retrieved = await repo.get_by_id(str(token.id))

                if retrieved:
                    logger.info("✅ 根据ID获取Token成功: id=%s", token.id)
                    logger.info("  - 应用: %s", retrieved.app)
                    logger.info("  - 第三方用户ID: %s", retrieved.thirdUserId)
                    logger.info(
                        "  - 状态: %s",
                        retrieved.status if retrieved.status else "未设置",
                    )

                    # 验证数据一致性
                    assert retrieved.app == token.app
                    assert retrieved.thirdUserId == token.thirdUserId
                    logger.info("  - 数据一致性验证通过")
                else:
                    logger.warning("⚠️  根据ID未找到Token: id=%s", token.id)
        else:
            logger.info("ℹ️  未找到Gmail Token用于ID测试")

        # 测试不存在的ID
        fake_id = "507f1f77bcf86cd799439011"  # 假的ObjectId格式
        not_found = await repo.get_by_id(fake_id)
        assert not_found is None
        logger.info("✅ 测试不存在的ID返回None: id=%s", fake_id)

        logger.info("✅ get_by_id 方法测试完成")

    except Exception as e:
        logger.error("❌ 测试 get_by_id 方法失败: %s", e)
        raise


async def test_timezone_handling():
    """测试时区处理"""
    logger.info("开始测试时区处理...")

    repo = get_bean_by_type(ThirdPartyOAuthTokenRawRepository)

    try:
        # 获取一些带有时间字段的Token
        tokens = await repo.get_by_app(app=ThirdPartyApp.GMAIL, limit=3)

        for token in tokens:
            if token.accessTokenExpireTime:
                logger.info("Token时间信息:")
                logger.info("  - ID: %s", token.id)
                logger.info(
                    "  - 访问令牌过期时间: %s",
                    to_iso_format(token.accessTokenExpireTime),
                )
                logger.info("  - 时区信息: %s", token.accessTokenExpireTime.tzinfo)

            if token.createTime:
                logger.info("  - 创建时间: %s", to_iso_format(token.createTime))

            if token.updateTime:
                logger.info("  - 更新时间: %s", to_iso_format(token.updateTime))

        logger.info("✅ 时区处理测试完成")

    except Exception as e:
        logger.error("❌ 测试时区处理失败: %s", e)
        raise


async def run_all_tests():
    """运行所有测试"""
    logger.info("🚀 开始运行ThirdPartyOAuthTokenRawRepository所有测试...")

    try:
        # 重点测试目标方法
        await test_get_userIds_by_app_and_thirdUserId()

        # 其他查询方法测试
        await test_get_by_app_and_thirdUserId()
        await test_get_by_app()
        await test_find_by_user_id()
        await test_get_app_and_thirdUserId_by_userid()
        await test_get_by_id()

        # 统计方法测试
        await test_statistics()

        # 时区处理测试
        await test_timezone_handling()

        logger.info("✅ 所有测试完成")

    except Exception as e:
        logger.error("❌ 测试过程中出现错误: %s", e)
        raise


if __name__ == "__main__":
    asyncio.run(run_all_tests())
