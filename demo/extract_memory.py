"""记忆提取脚本 V3 - 使用新架构，复刻原版功能

完全参考 extract_memory.py 的逻辑和输出格式，使用新的 ClusterManager。

主要功能：
- 消息归一化和过滤  
- 基于 LLM 的对话边界检测
- MemCell 提取和持久化（JSON + MongoDB）
- 实时聚类（使用 ClusterManager）
- 在线 Profile 提取（累积簇内 MemCells）

使用方法：
    python demo/extract_memory_v3.py
"""

import asyncio
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass


from memory_layer.memcell_extractor.base_memcell_extractor import RawData
from memory_layer.memcell_extractor.conv_memcell_extractor import (
    ConvMemCellExtractor,
    ConversationMemCellExtractRequest,
)
from memory_layer.llm.llm_provider import LLMProvider
from common_utils.datetime_utils import from_iso_format, get_now_with_timezone
from infra_layer.adapters.out.persistence.document.memory.memcell import (
    MemCell as DocMemCell,
    DataTypeEnum,
)
from memory_layer.memory_extractor.profile_memory_extractor import (
    ProfileMemoryExtractor,
    ProfileMemoryExtractRequest,
)
from memory_layer.memory_extractor.event_log_extractor import EventLogExtractor
from memory_layer.cluster_manager import (
    ClusterManager,
    ClusterManagerConfig,
    InMemoryClusterStorage,
)

from dotenv import load_dotenv

# 导入共享配置和工具
from demo.memory_config import (
    RunMode,
    ScenarioType,
    ExtractModeConfig,
    LLMConfig,
    MongoDBConfig,
)
from demo.memory_utils import (
    ensure_mongo_beanie_ready,
    serialize_datetime,
)

load_dotenv()

# ============================================================================
# 全局配置（完全参考原代码）
# ============================================================================

CURRENT_RUN_MODE = RunMode.EXTRACT_ALL

EXTRACT_CONFIG = ExtractModeConfig(
    # scenario_type=ScenarioType.GROUP_CHAT,
    scenario_type=ScenarioType.ASSISTANT,
    language="zh",
    enable_profile_extraction=True,
)

LLM_CONFIG = LLMConfig()
MONGO_CONFIG = MongoDBConfig()


# ============================================================================
# 工具函数（参考原代码）
# ============================================================================

def load_events(path: Path) -> List[Dict[str, Any]]:
    """从 JSON 文件加载对话事件列表"""
    with path.open("r", encoding="utf-8") as fp:
        data = json.load(fp)

    if isinstance(data, dict):
        conversation_list = data.get("conversation_list")
        if conversation_list is not None:
            if isinstance(conversation_list, list):
                return conversation_list
            raise ValueError("`conversation_list` 字段必须为数组")

    if isinstance(data, list):
        return data

    raise ValueError("不支持的数据格式")


def normalize_entry(entry: Dict[str, Any]) -> Dict[str, Any] | None:
    """归一化消息格式（参考原代码）"""
    timestamp = (
        entry.get("create_time")
        or entry.get("createTime")
        or entry.get("timestamp")
        or entry.get("created_at")
    )
    if timestamp is None:
        return None

    if isinstance(timestamp, str):
        try:
            timestamp_dt = from_iso_format(timestamp)
        except Exception:
            return None
    else:
        return None

    speaker_name = entry.get("sender_name") or entry.get("sender")
    if not speaker_name:
        origin = entry.get("origin")
        if isinstance(origin, dict):
            speaker_name = origin.get("fullName") or origin.get("full_name")
    if not speaker_name:
        return None
    speaker_name = str(speaker_name)

    raw_speaker_id = None
    origin = entry.get("origin")
    if isinstance(origin, dict):
        raw_speaker_id = origin.get("createBy") or origin.get("create_by")
    if not raw_speaker_id:
        raw_speaker_id = entry.get("sender_id") or entry.get("sender")
    speaker_id = str(raw_speaker_id) if raw_speaker_id is not None else ""

    content = str(entry.get("content", ""))

    payload: Dict[str, Any] = {
        "speaker_id": speaker_id,
        "speaker_name": speaker_name,
        "content": content,
        "timestamp": timestamp_dt,
    }
    return payload


def write_memcell_to_file(memcell, index: int, output_dir: Path) -> None:
    """保存 MemCell 到文件（参考原代码，包含 event_log）"""
    payload = memcell.to_dict()  # event_log 已自动包含在 to_dict() 中
    
    # 处理 original_data 中的 timestamp
    if "original_data" in payload and payload["original_data"]:
        for item in payload["original_data"]:
            if isinstance(item, dict) and "timestamp" in item:
                ts = item["timestamp"]
                if hasattr(ts, "isoformat"):
                    item["timestamp"] = ts.isoformat()

    output_path = output_dir / f"memcell_{index:03d}.json"
    with output_path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
    print(f"[Extract] 保存 MemCell #{index} → {output_path.name}")


async def _save_memcell_to_mongodb(memcell) -> None:
    """保存 MemCell 到 MongoDB（参考原代码，包含 event_log）"""
    try:
        ts = memcell.timestamp
        if isinstance(ts, str):
            ts_dt = from_iso_format(ts)
        elif isinstance(ts, (int, float)):
            tz = get_now_with_timezone().tzinfo
            ts_dt = datetime.fromtimestamp(float(ts), tz=tz)
        else:
            ts_dt = ts or get_now_with_timezone()

        primary_user = (
            memcell.user_id_list[0]
            if getattr(memcell, 'user_id_list', None)
            else "default"
        )
        
        # 准备 event_log（如果有，转为 dict）
        event_log_dict = None
        if hasattr(memcell, 'event_log') and memcell.event_log:
            try:
                if hasattr(memcell.event_log, 'to_dict'):
                    event_log_dict = memcell.event_log.to_dict()
                elif isinstance(memcell.event_log, dict):
                    event_log_dict = memcell.event_log
                else:
                    # 尝试转换为字典
                    event_log_dict = dict(memcell.event_log)
            except Exception as e:
                print(f"[MongoDB] ⚠️ event_log 转换失败: {e}")
                event_log_dict = None

        # 准备 semantic_memories（转为字典列表）
        semantic_memories_list = None
        if hasattr(memcell, 'semantic_memories') and memcell.semantic_memories:
            try:
                semantic_memories_list = [
                    sm.to_dict() if hasattr(sm, 'to_dict') else sm
                    for sm in memcell.semantic_memories
                ]
            except Exception:
                semantic_memories_list = None

        doc = DocMemCell(
            user_id=primary_user,
            timestamp=ts_dt,
            summary=memcell.summary or "",
            group_id=getattr(memcell, 'group_id', None),
            participants=getattr(memcell, 'participants', None),
            type=DataTypeEnum.CONVERSATION,
            original_data=memcell.original_data,
            subject=getattr(memcell, 'subject', None),
            keywords=getattr(memcell, 'keywords', None),
            linked_entities=getattr(memcell, 'linked_entities', None),
            episode=getattr(memcell, 'episode', None),
            semantic_memories=semantic_memories_list,
            event_log=event_log_dict,
            extend=getattr(memcell, 'extend', None),
        )
        await doc.insert()
    except Exception as e:
        print(f"[MongoDB] ⚠️ 保存 MemCell 失败: {e}")


async def save_individual_profile_to_file(
    profile, user_id: str, output_dir: Path
) -> None:
    """保存 Profile 到文件（完全参考原代码）"""
    try:
        if hasattr(profile, 'to_dict'):
            try:
                payload = profile.to_dict()
            except (AttributeError, TypeError) as e:
                error_msg = str(e).lower()
                if 'tzinfo' in error_msg or 'isoformat' in error_msg:
                    payload = profile.__dict__.copy()
                    if hasattr(payload.get('memory_type'), 'value'):
                        payload['memory_type'] = payload['memory_type'].value
                    ts = payload.get('timestamp')
                    if ts is not None:
                        if hasattr(ts, 'isoformat'):
                            payload['timestamp'] = ts.isoformat()
                        elif not isinstance(ts, str):
                            payload['timestamp'] = str(ts)
                else:
                    raise
        elif hasattr(profile, '__dict__'):
            payload = profile.__dict__.copy()
            if hasattr(payload.get('memory_type'), 'value'):
                payload['memory_type'] = payload['memory_type'].value
            ts = payload.get('timestamp')
            if ts is not None:
                if hasattr(ts, 'isoformat'):
                    payload['timestamp'] = ts.isoformat()
                elif not isinstance(ts, str):
                    payload['timestamp'] = str(ts)
        else:
            payload = {}

        try:
            payload = serialize_datetime(payload)
        except Exception:
            pass

        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"profile_{user_id}.json"

        with output_path.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2, default=str)

        print(f"[Profile] 保存 {user_id} → {output_path.name}")
    except Exception as e:
        print(f"[Profile] ❌ 保存 {user_id} Profile 失败: {e}")
        import traceback
        traceback.print_exc()


# ============================================================================
# 核心提取逻辑（完全参考原代码，使用新的 ClusterManager）
# ============================================================================

async def extract_and_dump(
    events: List[Dict[str, Any]],
    extract_config: ExtractModeConfig,
    llm_config: LLMConfig,
    mongo_config: MongoDBConfig,
) -> Dict[str, int]:
    """核心提取流程（参考原代码，使用 ClusterManager）"""
    
    # 初始化 MongoDB
    await ensure_mongo_beanie_ready(mongo_config)

    # 创建 LLM Provider
    provider = LLMProvider(
        llm_config.provider,
        model=llm_config.model,
        api_key=llm_config.api_key,
        base_url=llm_config.base_url,
        temperature=llm_config.temperature,
        max_tokens=llm_config.max_tokens,
    )

    # 创建 MemCell Extractor
    extractor = ConvMemCellExtractor(provider)
    
    # ===== 使用 ClusterManager 替代内置 ClusteringWorker =====
    cluster_storage = InMemoryClusterStorage(
        enable_persistence=True,
        persist_dir=extract_config.output_dir
    )
    cluster_config = ClusterManagerConfig(
        similarity_threshold=0.65,
        max_time_gap_days=7.0,
        enable_persistence=True,
        persist_dir=str(extract_config.output_dir),
        clustering_algorithm="centroid"
    )
    cluster_mgr = ClusterManager(config=cluster_config, storage=cluster_storage)
    cluster_mgr.attach_to_extractor(extractor)

    # 创建 Event Log Extractor
    event_log_extractor = EventLogExtractor(llm_provider=provider)
    print(f"[Extract] ✅ Event Log 提取器已创建")
    
    # 创建 Profile Extractor
    profile_extractor = None
    if extract_config.enable_profile_extraction:
        try:
            profile_extractor = ProfileMemoryExtractor(llm_provider=provider)
            print(f"[Extract] ✅ Profile 提取器已创建")
        except Exception as e:
            print(f"[Extract] ⚠️ Profile 提取器创建失败: {e}")

    history: List[RawData] = []
    saved_files = 0
    profile_count = 0

    # 在线 Profile 缓存（参考原代码）
    cluster_to_memcells: Dict[str, List[Any]] = {}
    online_profiles_map: Dict[str, Any] = {}
    session_memcells: List[Any] = []

    # Profile 输出目录
    profiles_dir_group = extract_config.output_dir / "profiles"
    profiles_dir_group.mkdir(parents=True, exist_ok=True)
    profiles_dir_companion = extract_config.output_dir / "profiles_companion"
    profiles_dir_companion.mkdir(parents=True, exist_ok=True)

    print(f"[Extract] 开始处理 {len(events)} 个对话事件...")

    for idx, entry in enumerate(events):
        # 归一化
        message_payload = normalize_entry(entry)
        if message_payload is None:
            continue

        message_id = (
            entry.get("message_id")
            or entry.get("id")
            or entry.get("uuid")
            or entry.get("event_id")
        )
        raw_item = RawData(
            content=message_payload,
            data_id=str(message_id or idx),
            data_type=DataTypeEnum.CONVERSATION,
        )

        # 初始化
        if not history:
            history.append(raw_item)
            continue

        # 构建提取请求
        request = ConversationMemCellExtractRequest(
            history_raw_data_list=list(history),
            new_raw_data_list=[raw_item],
            user_id_list=[],
            group_id=extract_config.group_id,
            smart_mask_flag=True,
        )

        # 提取 MemCell
        try:
            memcell, status = await extractor.extract_memcell(
                request,
                use_semantic_extraction=extract_config.enable_semantic_extraction,
            )
            should_end = memcell is not None
            should_wait = bool(status.should_wait) if status is not None else False
        except Exception as e:
            print(f"\n[Extract] ⚠️ MemCell 提取失败: {e}")
            history.append(raw_item)
            if len(history) > extract_config.history_window_size:
                history = history[-extract_config.history_window_size:]
            continue

        # 处理提取结果
        if should_end:
            saved_files += 1
            
            # ===== 1. 提取 Event Log（每个 MemCell 都提取） =====
            if hasattr(memcell, 'episode') and memcell.episode:
                try:
                    event_log = await event_log_extractor.extract_event_log(
                        episode_text=memcell.episode,
                        timestamp=memcell.timestamp
                    )
                    if event_log:
                        memcell.event_log = event_log
                        print(f"[EventLog] ✅ MemCell #{saved_files} 的 Event Log 提取成功")
                except Exception as e:
                    print(f"[EventLog] ⚠️ MemCell #{saved_files} 的 Event Log 提取失败: {e}")
            
            # ===== 2. 立即保存 MemCell 到文件（包含 event_log） =====
            write_memcell_to_file(memcell, saved_files, extract_config.output_dir)

            # ===== 3. 立即保存到 MongoDB =====
            await _save_memcell_to_mongodb(memcell)

            # ===== 3. 等待聚类和 event_log 提取完成 =====
            await asyncio.sleep(0.1)  # 给聚类和异步任务时间
            
            # 获取聚类分配
            assignments = {}
            for gid, state in cluster_mgr._states.items():
                assignments[gid] = state.eventid_to_cluster
            
            gid = extract_config.group_id or "__default__"
            mapping = assignments.get(gid, {})
            cluster_id = mapping.get(str(memcell.event_id))
            
            if not cluster_id:
                cluster_id = f"cluster_{str(memcell.event_id)[:8]}"

            # ===== 4. 在线 Profile 提取（参考原代码） =====
            if extract_config.enable_profile_extraction and profile_extractor:
                if extract_config.scenario_type == ScenarioType.GROUP_CHAT:
                    # 累积簇内 MemCells
                    bucket = cluster_to_memcells.setdefault(cluster_id, [])
                    bucket.append(memcell)

                    # 立即提取 Profile（使用簇内所有 MemCells）
                    extract_request = ProfileMemoryExtractRequest(
                        memcell_list=bucket,
                        user_id_list=[],
                        group_id=extract_config.group_id,
                        group_name=extract_config.group_name,
                        old_memory_list=(
                            list(online_profiles_map.values())
                            if online_profiles_map
                            else None
                        ),
                    )
                    
                    try:
                        batch_profile_memories = await profile_extractor.extract_memory(
                            extract_request
                        )
                        if batch_profile_memories:
                            for profile in batch_profile_memories:
                                uid = getattr(profile, 'user_id', None)
                                if not uid:
                                    continue
                                online_profiles_map[uid] = profile
                                await save_individual_profile_to_file(
                                    profile=profile,
                                    user_id=uid,
                                    output_dir=profiles_dir_group,
                                )
                    except Exception as e:
                        print(f"[Profile] ⚠️ Profile 提取失败: {e}")

                elif extract_config.scenario_type == ScenarioType.ASSISTANT:
                    # 助手场景
                    session_memcells.append(memcell)
                    
                    extract_request = ProfileMemoryExtractRequest(
                        memcell_list=list(session_memcells),
                        user_id_list=[],
                        group_id=extract_config.group_id,
                        group_name=extract_config.group_name,
                        old_memory_list=(
                            list(online_profiles_map.values())
                            if online_profiles_map
                            else None
                        ),
                    )
                    
                    try:
                        batch_profile_memories = (
                            await profile_extractor.extract_profile_companion(
                                extract_request
                            )
                        )
                        if batch_profile_memories:
                            for profile in batch_profile_memories:
                                uid = getattr(profile, 'user_id', None)
                                if not uid:
                                    continue
                                online_profiles_map[uid] = profile
                                await save_individual_profile_to_file(
                                    profile=profile,
                                    user_id=uid,
                                    output_dir=profiles_dir_companion,
                                )
                    except Exception as e:
                        print(f"[Profile] ⚠️ Profile 提取失败: {e}")

            # 重置历史
            history = [raw_item]
            continue

        if should_wait:
            history.append(raw_item)
            if len(history) > extract_config.history_window_size:
                history = history[-extract_config.history_window_size:]
            continue

        # 继续累积
        history.append(raw_item)
        if len(history) > extract_config.history_window_size:
            history = history[-extract_config.history_window_size:]

    # 导出聚类结果
    await cluster_mgr.export_clusters(extract_config.output_dir)

    profile_count = len(online_profiles_map)

    print(f"\n[Extract] ✅ 提取完成！")
    print(f"  - MemCell: {saved_files} 个")
    if profile_extractor is not None:
        print(f"  - Profile: {profile_count} 个")
    print(f"  - 输出目录: {extract_config.output_dir}")

    return {"saved_files": saved_files, "profile_count": profile_count}


# ============================================================================
# 主入口
# ============================================================================

async def run_extract_mode(
    extract_config: ExtractModeConfig,
    llm_config: LLMConfig,
    mongo_config: MongoDBConfig,
) -> None:
    """运行提取模式"""
    
    # 验证配置
    if not extract_config.data_file.exists():
        print(f"[Extract] ❌ 输入文件不存在: {extract_config.data_file}")
        return

    # 准备输出目录
    extract_config.output_dir.mkdir(parents=True, exist_ok=True)

    # 加载数据
    print(f"[Extract] 加载对话数据: {extract_config.data_file}")
    events = load_events(extract_config.data_file)

    # 执行提取
    result = await extract_and_dump(events, extract_config, llm_config, mongo_config)
    saved_memcells = result.get("saved_files", 0)
    saved_profiles = result.get("profile_count", 0)

    print(f"\n[Extract] ✅ 提取完成！")
    print(f"  - 保存了 {saved_memcells} 个 MemCell")
    print(f"  - 保存了 {saved_profiles} 个 Profile")
    print(f"  - 输出目录: {extract_config.output_dir}")


def main() -> None:
    """主入口"""
    print("=" * 80)
    print("记忆提取工具 V3 - 使用新架构，复刻原版功能")
    print("=" * 80)
    print(f"运行模式: {CURRENT_RUN_MODE.value}")
    print(f"场景类型: {EXTRACT_CONFIG.scenario_type.value}")
    print(f"数据文件: {EXTRACT_CONFIG.data_file}")
    print(f"输出目录: {EXTRACT_CONFIG.output_dir}")
    print("=" * 80 + "\n")

    try:
        asyncio.run(run_extract_mode(EXTRACT_CONFIG, LLM_CONFIG, MONGO_CONFIG))
    except KeyboardInterrupt:
        print("\n\n[Info] 用户中断，退出程序")
    except Exception as e:
        print(f"\n[Error] 程序执行失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

