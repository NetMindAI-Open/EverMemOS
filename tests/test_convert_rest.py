#!/usr/bin/env python3
"""
测试 convert_rest_to_request 函数并打印详细的输入输出
"""

import sys
import os
import json

# 添加项目根目录到 Python 路径


async def test_convert_rest_to_request():
    """测试 convert_rest_to_request 函数"""
    print("🚀 开始测试 convert_rest_to_request 函数\n")

    try:
        from agentic_layer.converter import convert_rest_to_request

        print("✅ convert_rest_to_request 导入成功\n")
    except Exception as e:
        print(f"❌ convert_rest_to_request 导入失败: {e}")
        return

    # 创建一个模拟的 FastAPI 请求对象
    class MockFastAPIRequest:
        def __init__(self, json_data):
            self._json_data = json_data

        async def json(self):
            return self._json_data

    # 测试数据：模拟聊天消息
    test_messages = [
        {
            "_id": "msg_1",
            "fullName": "用户A",
            "receiverId": "user_b",
            "roomId": "room_123",
            "userIdList": ["user_a", "user_b"],
            "referList": [],
            "content": "你好，今天天气怎么样？",
            "createTime": "2024-01-01T10:00:00Z",
            "createBy": "user_a",
            "updateTime": "2024-01-01T10:00:00Z",
            "orgId": "org_1",
        },
        {
            "_id": "msg_2",
            "fullName": "用户B",
            "receiverId": "user_a",
            "roomId": "room_123",
            "userIdList": ["user_a", "user_b"],
            "referList": [],
            "content": "今天天气很好，适合出门",
            "createTime": "2024-01-01T10:01:00Z",
            "createBy": "user_b",
            "updateTime": "2024-01-01T10:01:00Z",
            "orgId": "org_1",
        },
        {
            "_id": "msg_3",
            "fullName": "用户A",
            "receiverId": "user_b",
            "roomId": "room_123",
            "userIdList": ["user_a", "user_b"],
            "referList": [],
            "content": "那我们一起去公园吧",
            "createTime": "2024-01-01T10:02:00Z",
            "createBy": "user_a",
            "updateTime": "2024-01-01T10:02:00Z",
            "orgId": "org_1",
        },
    ]

    # 测试场景1：完整的 REST 请求
    print("=== 测试场景1：完整的 REST 请求 ===")
    rest_body_1 = {
        "mode": "work",
        "request_type": "memorize",
        "memorize_request": {
            "messages": test_messages,
            "participants": ["user_a", "user_b"],
            "group_id": "room_123",
            "raw_data_type": "Conversation",
        },
        "source": "smart_reply",
    }

    print("📥 输入数据 (REST 请求体):")
    print(json.dumps(rest_body_1, indent=2, ensure_ascii=False))
    print()

    try:
        mock_request_1 = MockFastAPIRequest(rest_body_1)
        result_1 = await convert_rest_to_request(mock_request_1)

        print("📤 输出数据 (Request 对象):")
        print(f"  - mode: {result_1.mode} (类型: {type(result_1.mode)})")
        print(
            f"  - request_type: {result_1.request_type} (类型: {type(result_1.request_type)})"
        )
        print(f"  - source: {result_1.source} (类型: {type(result_1.source)})")
        print(f"  - memorize_request: {type(result_1.memorize_request)}")

        if result_1.memorize_request:
            print("\n📋 memorize_request 详细信息:")
            print(
                f"  - 历史消息数量: {len(result_1.memorize_request.history_raw_data_list)}"
            )
            print(f"  - 新消息数量: {len(result_1.memorize_request.new_raw_data_list)}")
            print(f"  - 参与者: {result_1.memorize_request.participants}")
            print(f"  - 群组ID: {result_1.memorize_request.group_id}")
            print(f"  - 数据类型: {result_1.memorize_request.raw_data_type}")

            # 打印 RawData 详细信息
            if result_1.memorize_request.history_raw_data_list:
                print("\n📜 历史消息 RawData 示例:")
                for i, raw_data in enumerate(
                    result_1.memorize_request.history_raw_data_list[:2]
                ):  # 只显示前2条
                    print(f"  [{i+1}] data_id: {raw_data.data_id}")
                    print(f"      content: {raw_data.content}")
                    print(f"      metadata: {raw_data.metadata}")
                    print()

            if result_1.memorize_request.new_raw_data_list:
                print("📝 新消息 RawData 示例:")
                for i, raw_data in enumerate(
                    result_1.memorize_request.new_raw_data_list
                ):
                    print(f"  [{i+1}] data_id: {raw_data.data_id}")
                    print(f"      content: {raw_data.content}")
                    print(f"      metadata: {raw_data.metadata}")
                    print()

        print("✅ 场景1测试通过！\n")

    except Exception as e:
        print(f"❌ 场景1测试失败: {e}")
        import traceback

        traceback.print_exc()
        print()

    # 测试场景2：最小化的 REST 请求
    print("=== 测试场景2：最小化的 REST 请求 ===")
    rest_body_2 = {
        "request_type": "memorize",
        "memorize_request": {
            "messages": test_messages[:1],  # 只有一条消息
            "participants": ["user_a"],
        },
    }

    print("📥 输入数据 (最小化 REST 请求体):")
    print(json.dumps(rest_body_2, indent=2, ensure_ascii=False))
    print()

    try:
        mock_request_2 = MockFastAPIRequest(rest_body_2)
        result_2 = await convert_rest_to_request(mock_request_2)

        print("📤 输出数据 (Request 对象):")
        print(f"  - mode: {result_2.mode} (类型: {type(result_2.mode)})")
        print(
            f"  - request_type: {result_2.request_type} (类型: {type(result_2.request_type)})"
        )
        print(f"  - source: {result_2.source} (类型: {type(result_2.source)})")
        print(f"  - memorize_request: {type(result_2.memorize_request)}")

        if result_2.memorize_request:
            print("\n📋 memorize_request 详细信息:")
            print(
                f"  - 历史消息数量: {len(result_2.memorize_request.history_raw_data_list)}"
            )
            print(f"  - 新消息数量: {len(result_2.memorize_request.new_raw_data_list)}")
            print(f"  - 参与者: {result_2.memorize_request.participants}")
            print(f"  - 群组ID: {result_2.memorize_request.group_id}")
            print(f"  - 数据类型: {result_2.memorize_request.raw_data_type}")

        print("✅ 场景2测试通过！\n")

    except Exception as e:
        print(f"❌ 场景2测试失败: {e}")
        import traceback

        traceback.print_exc()
        print()

    # 测试场景3：错误处理
    print("=== 测试场景3：错误处理 ===")
    rest_body_3 = {
        "request_type": "memorize",
        "memorize_request": {"invalid_field": "invalid_value"},
    }

    print("📥 输入数据 (无效的 REST 请求体):")
    print(json.dumps(rest_body_3, indent=2, ensure_ascii=False))
    print()

    try:
        mock_request_3 = MockFastAPIRequest(rest_body_3)
        result_3 = await convert_rest_to_request(mock_request_3)
        print("❌ 应该抛出异常但没有抛出")
    except ValueError as e:
        print(f"✅ 错误处理正确: {e}")
    except Exception as e:
        print(f"❌ 意外错误: {e}")
        import traceback

        traceback.print_exc()

    # 测试场景4：Tanka 消息格式
    print("=== 测试场景4：Tanka 消息格式 ===")

    # Tanka 格式的测试数据
    tanka_messages = [
        {
            "tanka_mag_id": "68a82bde35e96010bc1e4360",
            "sender": "Yafeng DENG",
            "content": "这是我之前和xxx、xxx一起讨论确定的基本思路",
            "createTime": "2025-08-22T08:35:42.841Z",
            "sender_title": "Vice President,Artificial Intelligence / Head of the AI Innovation Center",
            "origin": {
                "id": "68a82bde35e96010bc1e4360",
                "createTime": 1755851742841,
                "createBy": "67f33794e609ad70e252d6f8",
                "updateTime": 1755851742841,
                "orgId": "6601416bf33c96c5a32fdd20",
                "version": 1,
                "localId": "7_50c0e2d7-4699-467a-832e-c3fed3f5c262_1552",
                "deviceType": 7,
                "fullName": "Yafeng DENG",
                "headImgUrl": "YD",
                "taskType": 3,
                "receiverId": "68a82b78d8d9c467f9605908",
                "msgType": 1,
                "content": "这是我之前和xxx、xxx一起讨论确定的基本思路",
                "isReplySuggest": 0,
                "notifyType": 0,
                "status": 1,
                "deleteFlag": 0,
                "playStatus": 0,
                "readUpdateTime": 1755851742841,
            },
        },
        {
            "tanka_mag_id": "68a82bec35e96010bc1e4369",
            "sender": "Yafeng DENG",
            "content": "具体安排以xxx这边的方案为主",
            "createTime": "2025-08-22T08:35:56.520Z",
            "sender_title": "Vice President,Artificial Intelligence / Head of the AI Innovation Center",
            "origin": {
                "id": "68a82bec35e96010bc1e4369",
                "createTime": 1755851756520,
                "createBy": "67f33794e609ad70e252d6f8",
                "updateTime": 1755851756520,
                "orgId": "6601416bf33c96c5a32fdd20",
                "version": 1,
                "localId": "7_ff46e641-41fd-4174-9f02-e84b946ea8ae_4644",
                "deviceType": 7,
                "fullName": "Yafeng DENG",
                "headImgUrl": "YD",
                "taskType": 3,
                "receiverId": "68a82b78d8d9c467f9605908",
                "msgType": 1,
                "content": "具体安排以xxx这边的方案为主",
                "isReplySuggest": 0,
                "notifyType": 0,
                "status": 1,
                "deleteFlag": 0,
                "playStatus": 0,
                "readUpdateTime": 1755851756520,
            },
        },
    ]

    # 将 Tanka 格式转换为标准消息格式
    standard_messages = []
    for tanka_msg in tanka_messages:
        origin = tanka_msg.get("origin", {})
        standard_msg = {
            "_id": tanka_msg.get("tanka_mag_id"),
            "fullName": tanka_msg.get("sender"),
            "receiverId": origin.get("receiverId"),
            "roomId": "tanka_room_123",  # 假设的房间ID
            "userIdList": [origin.get("createBy"), origin.get("receiverId")],
            "referList": [],
            "content": tanka_msg.get("content"),
            "createTime": tanka_msg.get("createTime"),
            "createBy": origin.get("createBy"),
            "updateTime": tanka_msg.get("createTime"),
            "orgId": origin.get("orgId"),
            # 保留 Tanka 特有的字段
            "sender_title": tanka_msg.get("sender_title"),
            "tanka_origin": origin,
        }
        standard_messages.append(standard_msg)

    rest_body_4 = {
        "mode": "work",
        "request_type": "memorize",
        "memorize_request": {
            "messages": standard_messages,
            "participants": ["67f33794e609ad70e252d6f8", "68a82b78d8d9c467f9605908"],
            "group_id": "tanka_room_123",
            "raw_data_type": "Conversation",
        },
        "source": "smart_reply",
    }

    print("📥 输入数据 (Tanka 格式转换后的 REST 请求体):")
    print(json.dumps(rest_body_4, indent=2, ensure_ascii=False))
    print()

    try:
        mock_request_4 = MockFastAPIRequest(rest_body_4)
        result_4 = await convert_rest_to_request(mock_request_4)

        print("📤 输出数据 (Request 对象):")
        print(f"  - mode: {result_4.mode} (类型: {type(result_4.mode)})")
        print(
            f"  - request_type: {result_4.request_type} (类型: {type(result_4.request_type)})"
        )
        print(f"  - source: {result_4.source} (类型: {type(result_4.source)})")
        print(f"  - memorize_request: {type(result_4.memorize_request)}")

        if result_4.memorize_request:
            print("\n📋 memorize_request 详细信息:")
            print(
                f"  - 历史消息数量: {len(result_4.memorize_request.history_raw_data_list)}"
            )
            print(f"  - 新消息数量: {len(result_4.memorize_request.new_raw_data_list)}")
            print(f"  - 参与者: {result_4.memorize_request.participants}")
            print(f"  - 群组ID: {result_4.memorize_request.group_id}")
            print(f"  - 数据类型: {result_4.memorize_request.raw_data_type}")

            # 打印 RawData 详细信息，特别关注 Tanka 特有字段
            if result_4.memorize_request.history_raw_data_list:
                print("\n📜 历史消息 RawData 示例 (Tanka 格式):")
                for i, raw_data in enumerate(
                    result_4.memorize_request.history_raw_data_list
                ):
                    print(f"  [{i+1}] data_id: {raw_data.data_id}")
                    print(f"      content: {raw_data.content}")
                    print(f"      metadata: {raw_data.metadata}")
                    print()

            if result_4.memorize_request.new_raw_data_list:
                print("📝 新消息 RawData 示例 (Tanka 格式):")
                for i, raw_data in enumerate(
                    result_4.memorize_request.new_raw_data_list
                ):
                    print(f"  [{i+1}] data_id: {raw_data.data_id}")
                    print(f"      content: {raw_data.content}")
                    print(f"      metadata: {raw_data.metadata}")
                    print()

        print("✅ 场景4测试通过！\n")

    except Exception as e:
        print(f"❌ 场景4测试失败: {e}")
        import traceback

        traceback.print_exc()
        print()

    # 测试场景5：混合格式消息（包含不同来源的消息）
    print("=== 测试场景5：混合格式消息 ===")

    # 混合标准格式和 Tanka 格式的消息
    mixed_messages = [
        # 标准格式消息
        {
            "_id": "standard_msg_1",
            "fullName": "用户A",
            "receiverId": "user_b",
            "roomId": "mixed_room_456",
            "userIdList": ["user_a", "user_b"],
            "referList": [],
            "content": "这是标准格式的消息",
            "createTime": "2024-01-01T10:00:00Z",
            "createBy": "user_a",
            "updateTime": "2024-01-01T10:00:00Z",
            "orgId": "org_1",
        },
        # Tanka 格式消息（已转换）
        {
            "_id": "68a82bde35e96010bc1e4360",
            "fullName": "Yafeng DENG",
            "receiverId": "68a82b78d8d9c467f9605908",
            "roomId": "mixed_room_456",
            "userIdList": ["67f33794e609ad70e252d6f8", "68a82b78d8d9c467f9605908"],
            "referList": [],
            "content": "这是我之前和xxx、xxx一起讨论确定的基本思路",
            "createTime": "2025-08-22T08:35:42.841Z",
            "createBy": "67f33794e609ad70e252d6f8",
            "updateTime": "2025-08-22T08:35:42.841Z",
            "orgId": "6601416bf33c96c5a32fdd20",
            "sender_title": "Vice President,Artificial Intelligence / Head of the AI Innovation Center",
            "tanka_origin": {
                "id": "68a82bde35e96010bc1e4360",
                "createTime": 1755851742841,
                "createBy": "67f33794e609ad70e252d6f8",
                "updateTime": 1755851742841,
                "orgId": "6601416bf33c96c5a32fdd20",
                "version": 1,
                "localId": "7_50c0e2d7-4699-467a-832e-c3fed3f5c262_1552",
                "deviceType": 7,
                "fullName": "Yafeng DENG",
                "headImgUrl": "YD",
                "taskType": 3,
                "receiverId": "68a82b78d8d9c467f9605908",
                "msgType": 1,
                "content": "这是我之前和xxx、xxx一起讨论确定的基本思路",
                "isReplySuggest": 0,
                "notifyType": 0,
                "status": 1,
                "deleteFlag": 0,
                "playStatus": 0,
                "readUpdateTime": 1755851742841,
            },
        },
    ]

    rest_body_5 = {
        "mode": "work",
        "request_type": "memorize",
        "memorize_request": {
            "messages": mixed_messages,
            "participants": [
                "user_a",
                "user_b",
                "67f33794e609ad70e252d6f8",
                "68a82b78d8d9c467f9605908",
            ],
            "group_id": "mixed_room_456",
            "raw_data_type": "Conversation",
            "split_ratio": 0.5,  # 50% 作为历史消息
        },
        "source": "smart_reply",
    }

    print("📥 输入数据 (混合格式的 REST 请求体):")
    print(json.dumps(rest_body_5, indent=2, ensure_ascii=False))
    print()

    try:
        mock_request_5 = MockFastAPIRequest(rest_body_5)
        result_5 = await convert_rest_to_request(mock_request_5)

        print("📤 输出数据 (Request 对象):")
        print(f"  - mode: {result_5.mode} (类型: {type(result_5.mode)})")
        print(
            f"  - request_type: {result_5.request_type} (类型: {type(result_5.request_type)})"
        )
        print(f"  - source: {result_5.source} (类型: {type(result_5.source)})")
        print(f"  - memorize_request: {type(result_5.memorize_request)}")

        if result_5.memorize_request:
            print("\n📋 memorize_request 详细信息:")
            print(
                f"  - 历史消息数量: {len(result_5.memorize_request.history_raw_data_list)}"
            )
            print(f"  - 新消息数量: {len(result_5.memorize_request.new_raw_data_list)}")
            print(f"  - 参与者: {result_5.memorize_request.participants}")
            print(f"  - 群组ID: {result_5.memorize_request.group_id}")
            print(f"  - 数据类型: {result_5.memorize_request.raw_data_type}")

            # 打印 RawData 详细信息
            if result_5.memorize_request.history_raw_data_list:
                print("\n📜 历史消息 RawData 示例 (混合格式):")
                for i, raw_data in enumerate(
                    result_5.memorize_request.history_raw_data_list
                ):
                    print(f"  [{i+1}] data_id: {raw_data.data_id}")
                    print(f"      content: {raw_data.content}")
                    print(f"      metadata: {raw_data.metadata}")
                    print()

            if result_5.memorize_request.new_raw_data_list:
                print("📝 新消息 RawData 示例 (混合格式):")
                for i, raw_data in enumerate(
                    result_5.memorize_request.new_raw_data_list
                ):
                    print(f"  [{i+1}] data_id: {raw_data.data_id}")
                    print(f"      content: {raw_data.content}")
                    print(f"      metadata: {raw_data.metadata}")
                    print()

        print("✅ 场景5测试通过！\n")

    except Exception as e:
        print(f"❌ 场景5测试失败: {e}")
        import traceback

        traceback.print_exc()
        print()

    # 测试场景6：Tanka 格式边界情况
    print("=== 测试场景6：Tanka 格式边界情况 ===")

    # 测试不完整的 Tanka 消息
    incomplete_tanka_messages = [
        {
            "tanka_mag_id": "68a82bde35e96010bc1e4360",
            "sender": "Yafeng DENG",
            "content": "这是不完整的 Tanka 消息",
            "createTime": "2025-08-22T08:35:42.841Z",
            # 缺少 sender_title 和 origin
        }
    ]

    # 转换为标准格式，处理缺失字段
    incomplete_standard_messages = []
    for tanka_msg in incomplete_tanka_messages:
        origin = tanka_msg.get("origin", {})
        standard_msg = {
            "_id": tanka_msg.get("tanka_mag_id", "unknown_id"),
            "fullName": tanka_msg.get("sender", "Unknown Sender"),
            "receiverId": origin.get("receiverId", "unknown_receiver"),
            "roomId": "incomplete_room_789",
            "userIdList": [origin.get("createBy", "unknown_user")],
            "referList": [],
            "content": tanka_msg.get("content", ""),
            "createTime": tanka_msg.get("createTime", "2024-01-01T00:00:00Z"),
            "createBy": origin.get("createBy", "unknown_user"),
            "updateTime": tanka_msg.get("createTime", "2024-01-01T00:00:00Z"),
            "orgId": origin.get("orgId", "unknown_org"),
            # 可选字段
            "sender_title": tanka_msg.get("sender_title"),
            "tanka_origin": origin if origin else None,
        }
        incomplete_standard_messages.append(standard_msg)

    rest_body_6 = {
        "mode": "work",
        "request_type": "memorize",
        "memorize_request": {
            "messages": incomplete_standard_messages,
            "participants": ["unknown_user"],
            "group_id": "incomplete_room_789",
            "raw_data_type": "Conversation",
        },
        "source": "unknown",
    }

    print("📥 输入数据 (不完整 Tanka 格式的 REST 请求体):")
    print(json.dumps(rest_body_6, indent=2, ensure_ascii=False))
    print()

    try:
        mock_request_6 = MockFastAPIRequest(rest_body_6)
        result_6 = await convert_rest_to_request(mock_request_6)

        print("📤 输出数据 (Request 对象):")
        print(f"  - mode: {result_6.mode} (类型: {type(result_6.mode)})")
        print(
            f"  - request_type: {result_6.request_type} (类型: {type(result_6.request_type)})"
        )
        print(f"  - source: {result_6.source} (类型: {type(result_6.source)})")
        print(f"  - memorize_request: {type(result_6.memorize_request)}")

        if result_6.memorize_request:
            print("\n📋 memorize_request 详细信息:")
            print(
                f"  - 历史消息数量: {len(result_6.memorize_request.history_raw_data_list)}"
            )
            print(f"  - 新消息数量: {len(result_6.memorize_request.new_raw_data_list)}")
            print(f"  - 参与者: {result_6.memorize_request.user_id_list}")
            print(f"  - 群组ID: {result_6.memorize_request.group_id}")
            print(f"  - 数据类型: {result_6.memorize_request.raw_data_type}")

            # 打印 RawData 详细信息
            if result_6.memorize_request.new_raw_data_list:
                print("\n📝 新消息 RawData 示例 (不完整 Tanka 格式):")
                for i, raw_data in enumerate(
                    result_6.memorize_request.new_raw_data_list
                ):
                    print(f"  [{i+1}] data_id: {raw_data.data_id}")
                    print(f"      content: {raw_data.content}")
                    print(f"      metadata: {raw_data.metadata}")
                    print()

        print("✅ 场景6测试通过！\n")

    except Exception as e:
        print(f"❌ 场景6测试失败: {e}")
        import traceback

        traceback.print_exc()
        print()

    print("\n🎉 convert_rest_to_request 测试完成！")


if __name__ == "__main__":
    import asyncio

    asyncio.run(test_convert_rest_to_request())
