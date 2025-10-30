"""
Simple Boundary Detection Base Class for EverMemOS

This module provides a simple and extensible base class for detecting
boundaries in various types of content (conversations, emails, notes, etc.).
"""

import time
import os
from token import OP
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
from dataclasses import dataclass
import uuid
import json, re
import asyncio
import threading
import queue
import numpy as np
from ..llm.llm_provider import LLMProvider
from ..types import RawDataType
from ..prompts.zh.conv_prompts import CONV_BOUNDARY_DETECTION_PROMPT

# from ..prompts.eval.conv_prompts import CONV_BOUNDARY_DETECTION_PROMPT
from .base_memcell_extractor import (
    MemCellExtractor,
    RawData,
    MemCell,
    StatusResult,
    MemCellExtractRequest,
)
from ..memory_extractor.episode_memory_extractor import (
    EpisodeMemoryExtractor,
    EpisodeMemoryExtractRequest,
)
from core.observation.logger import get_logger

logger = get_logger(__name__)
from agentic_layer.vectorize_service import get_vectorize_service


@dataclass
class BoundaryDetectionResult:
    """Boundary detection result."""

    should_end: bool
    should_wait: bool
    reasoning: str
    confidence: float
    topic_summary: Optional[str] = None


@dataclass
class ConversationMemCellExtractRequest(MemCellExtractRequest):
    pass


@dataclass
class ClusteringParams:
    max_link_gap_seconds: int = 7 * 24 * 60 * 60
    similarity_threshold: float = 0.65


# Embeddings are provided by shared vectorize_service; legacy HTTP client removed


class _GroupClusterState:
    def __init__(self) -> None:
        self.event_ids: List[str] = []
        self.timestamps: List[float] = []
        self.vectors: List[np.ndarray] = []
        self.cluster_ids: List[str] = []
        self.eventid_to_cluster: Dict[str, str] = {}
        self.next_cluster_idx: int = 0
        # Centroid-based clustering state
        self.cluster_centroids: Dict[str, np.ndarray] = {}
        self.cluster_counts: Dict[str, int] = {}
        self.cluster_last_ts: Dict[str, Optional[float]] = {}

    def _assign_new_cluster(self, eid: str) -> str:
        cid = f"cluster_{self.next_cluster_idx:03d}"
        self.next_cluster_idx += 1
        self.eventid_to_cluster[eid] = cid
        self.cluster_ids.append(cid)
        return cid

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_ids": self.event_ids,
            "timestamps": self.timestamps,
            "cluster_ids": self.cluster_ids,
            "eventid_to_cluster": self.eventid_to_cluster,
            "next_cluster_idx": self.next_cluster_idx,
            "cluster_centroids": {
                cid: centroid.tolist()
                for cid, centroid in self.cluster_centroids.items()
            },
            "cluster_counts": self.cluster_counts,
            "cluster_last_ts": self.cluster_last_ts,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "_GroupClusterState":
        st = _GroupClusterState()
        st.event_ids = list(data.get("event_ids", []))
        st.timestamps = list(data.get("timestamps", []))
        st.cluster_ids = list(data.get("cluster_ids", []))
        st.eventid_to_cluster = dict(data.get("eventid_to_cluster", {}))
        st.next_cluster_idx = int(data.get("next_cluster_idx", 0))
        centroids = data.get("cluster_centroids", {}) or {}
        st.cluster_centroids = {
            k: np.array(v, dtype=np.float32) for k, v in centroids.items()
        }
        st.cluster_counts = {
            k: int(v) for k, v in (data.get("cluster_counts", {}) or {}).items()
        }
        st.cluster_last_ts = {
            k: float(v) for k, v in (data.get("cluster_last_ts", {}) or {}).items()
        }
        return st

    def _add_vector_to_cluster(
        self, cid: str, v: np.ndarray, ts: Optional[float]
    ) -> None:
        """Update centroid, count and last timestamp for the given cluster."""
        if v is None or v.size == 0:
            if ts is not None:
                prev_ts = self.cluster_last_ts.get(cid)
                self.cluster_last_ts[cid] = max(prev_ts or ts, ts)
            return

        count = self.cluster_counts.get(cid, 0)
        if count <= 0:
            self.cluster_centroids[cid] = v.astype(np.float32, copy=False)
            self.cluster_counts[cid] = 1
        else:
            cvec = self.cluster_centroids[cid]
            if cvec.dtype != np.float32:
                cvec = cvec.astype(np.float32)
            new_centroid = (cvec * float(count) + v) / float(count + 1)
            self.cluster_centroids[cid] = new_centroid.astype(np.float32, copy=False)
            self.cluster_counts[cid] = count + 1
        if ts is not None:
            prev_ts = self.cluster_last_ts.get(cid)
            self.cluster_last_ts[cid] = max(prev_ts or ts, ts)


class ClusteringWorker:
    """Background worker that consumes memcells, embeds, and does incremental clustering."""

    def __init__(self, params: Optional[ClusteringParams] = None):
        self.params = params or ClusteringParams()
        self._q: "queue.Queue[Tuple[str, Dict[str, Any]]]" = queue.Queue()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._states: Dict[str, _GroupClusterState] = {}
        # Prefer shared vectorize service for embeddings
        try:
            self._vectorize_service = get_vectorize_service()
        except Exception:
            self._vectorize_service = None
        self._thread.start()

    def submit(self, group_id: Optional[str], memcell: Dict[str, Any]) -> None:
        gid = group_id or "__default__"
        self._q.put((gid, memcell))

    def stop(self) -> None:
        self._stop.set()
        self._q.put(("__poison__", {}))
        self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                gid, memcell = self._q.get(timeout=0.2)
            except queue.Empty:
                continue
            if gid == "__poison__":
                break
            try:
                self._process_one(gid, memcell)
            except Exception as e:
                logger.exception(f"ClusteringWorker error processing memcell: {e}")

    def _get_text(self, mc: Dict[str, Any]) -> str:
        for key in ("episode", "summary", "subject"):
            if isinstance(mc.get(key), str) and mc[key].strip():
                return mc[key]
        # fallback to compact original_data
        od = mc.get("original_data")
        texts: List[str] = []
        if isinstance(od, list):
            for item in od[:6]:
                if isinstance(item, dict):
                    msg = item.get("content") or item.get("summary")
                    if isinstance(msg, str) and msg.strip():
                        texts.append(msg.strip())
        return "\n".join(texts) if texts else str(mc.get("event_id", ""))

    def _parse_ts(self, ts: Any) -> Optional[float]:
        try:
            if ts is None:
                return None
            if isinstance(ts, (int, float)):
                val = float(ts)
                if val > 10_000_000_000:
                    val = val / 1000.0
                return val
            if isinstance(ts, str):
                from src.common_utils.datetime_utils import from_iso_format

                return from_iso_format(ts).timestamp()
        except Exception:
            return None
        return None

    def _process_one(self, gid: str, memcell: Dict[str, Any]) -> None:
        state = self._states.setdefault(gid, _GroupClusterState())

        eid = str(memcell.get("event_id", str(uuid.uuid4())))
        ts = self._parse_ts(memcell.get("timestamp"))
        text = self._get_text(memcell)
        v = None
        # Try vectorize_service (async) first
        if self._vectorize_service is not None:
            try:
                v_arr = asyncio.run(self._vectorize_service.get_embedding(text))
                if v_arr is not None:
                    v = np.array(v_arr, dtype=np.float32)
            except Exception:
                v = None
        # No HTTP fallback; embeddings strictly come from shared vectorize_service
        if v is None or v.size == 0:
            # assign new cluster even if no vector
            cid = state._assign_new_cluster(eid)
            state.event_ids.append(eid)
            state.timestamps.append(ts or 0.0)
            state.vectors.append(np.zeros((1,), dtype=np.float32))
            return

        # decide cluster assignment using per-cluster centroids
        cid = None
        best_sim = -1.0
        best_cid = None
        if state.cluster_centroids:
            v_norm = np.linalg.norm(v) + 1e-9
            for ccid, centroid in state.cluster_centroids.items():
                if centroid is None or centroid.size == 0:
                    continue
                # time constraint: compare against the cluster's last timestamp if present
                if ts is not None:
                    last_ts = state.cluster_last_ts.get(ccid)
                    if (
                        last_ts is not None
                        and abs(ts - (last_ts or 0.0))
                        > self.params.max_link_gap_seconds
                    ):
                        continue
                denom = (np.linalg.norm(centroid) + 1e-9) * v_norm
                sim = float((centroid @ v) / denom)
                if sim > best_sim:
                    best_sim = sim
                    best_cid = ccid

        threshold = self.params.similarity_threshold
        if best_cid is not None and best_sim >= threshold:
            cid = best_cid
            state.eventid_to_cluster[eid] = cid
            state.cluster_ids.append(cid)
        else:
            cid = state._assign_new_cluster(eid)

        state.event_ids.append(eid)
        state.timestamps.append(ts or 0.0)
        state.vectors.append(v)
        state._add_vector_to_cluster(cid, v, ts)

    def dump_to_dir(self, output_dir: str) -> None:
        os.makedirs(output_dir, exist_ok=True)
        for gid, state in self._states.items():
            path = os.path.join(output_dir, f"cluster_map_{gid}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(
                    {"assignments": state.eventid_to_cluster},
                    f,
                    ensure_ascii=False,
                    indent=2,
                )

    def get_assignments(self) -> Dict[str, Dict[str, str]]:
        return {gid: st.eventid_to_cluster for gid, st in self._states.items()}

    def dump_state_to_dir(self, output_dir: str) -> None:
        os.makedirs(output_dir, exist_ok=True)
        for gid, state in self._states.items():
            path = os.path.join(output_dir, f"cluster_state_{gid}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(state.to_dict(), f, ensure_ascii=False, indent=2)

    def load_state_from_dir(self, input_dir: str) -> None:
        try:
            for fname in os.listdir(input_dir):
                if not fname.startswith("cluster_state_") or not fname.endswith(
                    ".json"
                ):
                    continue
                gid = fname[len("cluster_state_") : -len(".json")]
                with open(os.path.join(input_dir, fname), "r", encoding="utf-8") as f:
                    data = json.load(f)
                self._states[gid] = _GroupClusterState.from_dict(data)
        except Exception as e:
            logger.exception(f"Failed to load cluster states from {input_dir}: {e}")


class ConvMemCellExtractorWithCluster(MemCellExtractor):
    def __init__(
        self,
        llm_provider=LLMProvider,
        embedding_base_url: Optional[str] = None,
        embedding_model: Optional[str] = None,
        **llm_kwargs,
    ):
        # Ensure base class receives the correct raw_data_type and provider
        super().__init__(RawDataType.CONVERSATION, llm_provider, **llm_kwargs)
        self.llm_provider = llm_provider
        self.episode_extractor = EpisodeMemoryExtractor(llm_provider, **llm_kwargs)
        # Init background clustering worker (uses shared vectorize_service internally)
        self._cluster_worker = ClusteringWorker()

    def shutdown(self) -> None:
        try:
            self._cluster_worker.stop()
        except Exception:
            pass

    @property
    def cluster_worker(self) -> ClusteringWorker:
        return self._cluster_worker

    def _extract_participant_ids(
        self, chat_raw_data_list: List[Dict[str, Any]]
    ) -> List[str]:
        """
        从chat_raw_data_list中提取所有参与者ID

        从每个元素的content字典中获取：
        1. speaker_id（发言者ID）
        2. referList中所有的_id（@提及的用户ID）

        Args:
            chat_raw_data_list: 聊天原始数据列表

        Returns:
            List[str]: 去重后的所有参与者ID列表
        """
        participant_ids = set()

        for raw_data in chat_raw_data_list:

            # 提取speaker_id
            if 'speaker_id' in raw_data and raw_data['speaker_id']:
                participant_ids.add(raw_data['speaker_id'])

            # 提取referList中的所有ID
            if 'referList' in raw_data and raw_data['referList']:
                for refer_item in raw_data['referList']:
                    # refer_item可能是字典格式，包含_id字段
                    if isinstance(refer_item, dict):
                        # 处理MongoDB ObjectId格式的_id
                        if '_id' in refer_item:
                            refer_id = refer_item['_id']
                            # 如果是ObjectId对象，转换为字符串
                            if hasattr(refer_id, '__str__'):
                                participant_ids.add(str(refer_id))
                            else:
                                participant_ids.add(refer_id)
                        # 也检查普通的id字段
                        elif 'id' in refer_item:
                            participant_ids.add(refer_item['id'])
                    # 如果refer_item直接是ID字符串
                    elif isinstance(refer_item, str):
                        participant_ids.add(refer_item)

        return list(participant_ids)

    def _format_conversation_dicts(
        self, messages: list[dict[str, str]], include_timestamps: bool = False
    ) -> str:
        """Format conversation from message dictionaries into plain text."""
        lines = []
        for i, msg in enumerate(messages):
            content = msg.get("content", "")
            speaker_name = msg.get("speaker_name", "")
            timestamp = msg.get("timestamp", "")

            if content:
                if include_timestamps and timestamp:
                    try:
                        # 处理不同类型的timestamp
                        if isinstance(timestamp, datetime):
                            # 如果是datetime对象，直接格式化
                            time_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                            lines.append(f"[{time_str}] {speaker_name}: {content}")
                        elif isinstance(timestamp, str):
                            # 如果是字符串，先解析再格式化
                            dt = datetime.fromisoformat(
                                timestamp.replace("Z", "+00:00")
                            )
                            time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            lines.append(f"[{time_str}] {speaker_name}: {content}")
                        else:
                            # 其他类型，不包含时间戳
                            lines.append(f"{speaker_name}: {content}")
                    except (ValueError, AttributeError, TypeError):
                        # Fallback if timestamp parsing fails
                        lines.append(f"{speaker_name}: {content}")
                else:
                    lines.append(f"{speaker_name}: {content}")
            else:
                print(msg)
                print(
                    f"[ConversationEpisodeBuilder] Warning: message {i} has no content"
                )
        return "\n".join(lines)

    def _calculate_time_gap(
        self,
        conversation_history: list[dict[str, str]],
        new_messages: list[dict[str, str]],
    ):
        if not conversation_history or not new_messages:
            return "No time gap information available"

        try:
            # Get the last message from history and first new message
            last_history_msg = conversation_history[-1]
            first_new_msg = new_messages[0]

            last_timestamp_str = last_history_msg.get("timestamp", "")
            first_timestamp_str = first_new_msg.get("timestamp", "")

            if not last_timestamp_str or not first_timestamp_str:
                return "No timestamp information available"

            # Parse timestamps - 处理不同类型的timestamp、
            try:
                if isinstance(last_timestamp_str, datetime):
                    last_time = last_timestamp_str
                elif isinstance(last_timestamp_str, str):
                    last_time = datetime.fromisoformat(
                        last_timestamp_str.replace("Z", "+00:00")
                    )
                else:
                    return "Invalid timestamp format for last message"

                if isinstance(first_timestamp_str, datetime):
                    first_time = first_timestamp_str
                elif isinstance(first_timestamp_str, str):
                    first_time = datetime.fromisoformat(
                        first_timestamp_str.replace("Z", "+00:00")
                    )
                else:
                    return "Invalid timestamp format for first message"
            except (ValueError, TypeError):
                return "Failed to parse timestamps"

            # Calculate time difference
            time_diff = first_time - last_time
            total_seconds = time_diff.total_seconds()

            if total_seconds < 0:
                return "Time gap: Messages appear to be out of order"
            elif total_seconds < 60:  # Less than 1 minute
                return f"Time gap: {int(total_seconds)} seconds (immediate response)"
            elif total_seconds < 3600:  # Less than 1 hour
                minutes = int(total_seconds // 60)
                return f"Time gap: {minutes} minutes (recent conversation)"
            elif total_seconds < 86400:  # Less than 1 day
                hours = int(total_seconds // 3600)
                return f"Time gap: {hours} hours (same day, but significant pause)"
            else:  # More than 1 day
                days = int(total_seconds // 86400)
                return f"Time gap: {days} days (long gap, likely new conversation)"

        except (ValueError, KeyError, AttributeError) as e:
            return f"Time gap calculation error: {str(e)}"

    async def _detect_boundary(
        self,
        conversation_history: list[dict[str, str]],
        new_messages: list[dict[str, str]],
    ) -> BoundaryDetectionResult:
        if not conversation_history:
            return BoundaryDetectionResult(
                should_end=False,
                should_wait=False,
                reasoning="First messages in conversation",
                confidence=1.0,
                topic_summary="",
            )
        history_text = self._format_conversation_dicts(
            conversation_history, include_timestamps=True
        )
        new_text = self._format_conversation_dicts(
            new_messages, include_timestamps=True
        )
        time_gap_info = self._calculate_time_gap(conversation_history, new_messages)

        print(
            f"[ConversationEpisodeBuilder] Detect boundary – history tokens: {len(history_text)} new tokens: {len(new_text)} time gap: {time_gap_info}"
        )

        prompt = CONV_BOUNDARY_DETECTION_PROMPT.format(
            conversation_history=history_text,
            new_messages=new_text,
            time_gap_info=time_gap_info,
        )

        resp = await self.llm_provider.generate(prompt)
        print(
            f"[ConversationEpisodeBuilder] Boundary response length: {len(resp)} chars"
        )

        # Parse JSON response from LLM boundary detection
        json_match = re.search(r"\{[^{}]*\}", resp, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group())
            return BoundaryDetectionResult(
                should_end=data.get("should_end", False),
                should_wait=data.get("should_wait", True),
                reasoning=data.get("reasoning", "No reason provided"),
                confidence=data.get("confidence", 1.0),
                topic_summary=data.get("topic_summary", ""),
            )
        else:
            return BoundaryDetectionResult(
                should_end=False,
                should_wait=True,
                reasoning="Failed to parse LLM response",
                confidence=1.0,
                topic_summary="",
            )

    async def extract_memcell(
        self,
        request: ConversationMemCellExtractRequest,
        use_semantic_extraction: bool = False,
    ) -> tuple[Optional[MemCell], Optional[StatusResult]]:
        history_message_dict_list = []
        for raw_data in request.history_raw_data_list:
            processed_data = self._data_process(raw_data)
            if processed_data is not None:  # 过滤掉不支持的消息类型
                history_message_dict_list.append(processed_data)

        # 检查最后一条new_raw_data是否为None
        if (
            request.new_raw_data_list
            and self._data_process(request.new_raw_data_list[-1]) is None
        ):
            logger.warning(
                f"[ConvMemCellExtractor] 最后一条new_raw_data为None，跳过处理"
            )
            status_control_result = StatusResult(should_wait=True)
            return (None, status_control_result)

        new_message_dict_list = []
        for new_raw_data in request.new_raw_data_list:
            processed_data = self._data_process(new_raw_data)
            if processed_data is not None:  # 过滤掉不支持的消息类型
                new_message_dict_list.append(processed_data)

        # 检查是否有有效的消息可处理
        if not new_message_dict_list:
            logger.warning(
                f"[ConvMemCellExtractor] 没有有效的新消息可处理（可能都被过滤了）"
            )
            status_control_result = StatusResult(should_wait=True)
            return (None, status_control_result)

        if request.smart_mask_flag:
            boundary_detection_result = await self._detect_boundary(
                conversation_history=history_message_dict_list[:-1],
                new_messages=new_message_dict_list,
            )
        else:
            boundary_detection_result = await self._detect_boundary(
                conversation_history=history_message_dict_list,
                new_messages=new_message_dict_list,
            )
        should_end = boundary_detection_result.should_end
        should_wait = boundary_detection_result.should_wait
        reason = boundary_detection_result.reasoning

        status_control_result = StatusResult(should_wait=should_wait)

        if should_end:
            # TODO 重构专项：转为int逻辑不对 应该保持为datetime
            timestamp = history_message_dict_list[-1].get("timestamp")
            if isinstance(timestamp, str):
                timestamp = int(
                    datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()
                )

            participants = self._extract_participant_ids(history_message_dict_list)
            # 创建 MemCell
            memcell = MemCell(
                event_id=str(uuid.uuid4()),
                user_id_list=request.user_id_list,
                original_data=history_message_dict_list,
                timestamp=timestamp,
                summary=boundary_detection_result.topic_summary,
                group_id=request.group_id,
                participants=participants,  # 使用合并后的participants
                type=self.raw_data_type,
            )

            # 自动触发情景记忆提取
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    episode_request = EpisodeMemoryExtractRequest(
                        memcell_list=[memcell],
                        user_id_list=request.user_id_list,
                        participants=participants,
                        group_id=request.group_id,
                    )
                    logger.debug(
                        f"📚 自动触发情景记忆提取开始: memcell_list={memcell}, user_id_list={request.user_id_list}, participants={participants}, group_id={request.group_id}"
                    )
                    now = time.time()
                    episode_result = await self.episode_extractor.extract_memory(
                        episode_request,
                        use_group_prompt=True,
                        use_semantic_extraction=use_semantic_extraction,
                    )
                    logger.debug(
                        f"📚 自动触发情景记忆提取, 耗时: {time.time() - now}秒"
                    )
                    if episode_result and isinstance(episode_result, MemCell):
                        # GROUP_EPISODE_GENERATION_PROMPT 模式：返回包含情景记忆的 MemCell
                        logger.info(f"✅ 成功生成情景记忆并存储到 MemCell 中")
                        # Attach embedding info to MemCell (episode preferred)
                        try:
                            text_for_embed = (
                                episode_result.episode or episode_result.summary or ""
                            )
                            if text_for_embed:
                                vs = get_vectorize_service()
                                vec = await vs.get_embedding(text_for_embed)
                                episode_result.extend = episode_result.extend or {}
                                episode_result.extend["embedding"] = (
                                    vec.tolist()
                                    if hasattr(vec, "tolist")
                                    else list(vec)
                                )
                                episode_result.extend["vector_model"] = (
                                    vs.get_model_name()
                                )

                        except Exception:
                            logger.debug("Embedding attach failed; continue without it")
                        # Submit to clustering worker asynchronously
                        try:
                            self._cluster_worker.submit(
                                request.group_id, episode_result.to_dict()
                            )
                        except Exception:
                            logger.exception(
                                "Failed to submit memcell to clustering worker"
                            )
                        return (episode_result, status_control_result)
                    else:
                        logger.warning(
                            f"⚠️ 情景记忆提取失败 (尝试 {attempt + 1}/{max_retries})"
                        )

                except Exception as e:
                    logger.error(
                        f"❌ 情景记忆提取出错: {e} (尝试 {attempt + 1}/{max_retries})"
                    )

                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5)
                else:
                    logger.error(f"❌ 所有重试次数均失败，未能提取情景记忆")

            # Submit to clustering worker even if no episode content
            try:
                # Attach embedding info to MemCell (summary as fallback)

                text_for_embed = memcell.episode
                if text_for_embed:
                    vs = get_vectorize_service()
                    vec = await vs.get_embedding(text_for_embed)
                    memcell.extend = memcell.extend or {}
                    memcell.extend["embedding"] = (
                        vec.tolist() if hasattr(vec, "tolist") else list(vec)
                    )
                    memcell.extend["vector_model"] = vs.get_model_name()

                self._cluster_worker.submit(request.group_id, memcell.to_dict())
            except Exception:
                logger.exception("Failed to submit memcell to clustering worker")
            return (memcell, status_control_result)
        elif should_wait:
            logger.debug(f"⏳ Waiting for more messages: {reason}")
        return (None, status_control_result)

    def _data_process(self, raw_data: RawData) -> Dict[str, Any]:
        """处理原始数据，包括消息类型过滤和预处理"""
        content = (
            raw_data.content.copy()
            if isinstance(raw_data.content, dict)
            else raw_data.content
        )

        # 获取消息类型
        msg_type = content.get('msgType') if isinstance(content, dict) else None

        # 定义支持的消息类型和对应的占位符
        SUPPORTED_MSG_TYPES = {
            1: None,  # TEXT - 保持原文本
            2: "[图片]",  # PICTURE
            3: "[视频]",  # VIDEO
            4: "[音频]",  # AUDIO
            5: "[文件]",  # FILE - 保持原文本（文本和文件在同一个消息里）
            6: "[文件]",  # FILES
        }

        if isinstance(content, dict) and msg_type is not None:
            # 检查是否为支持的消息类型
            if msg_type not in SUPPORTED_MSG_TYPES:
                # 不支持的消息类型，直接跳过（返回None会在上层处理）
                logger.warning(
                    f"[ConvMemCellExtractor] 跳过不支持的消息类型: {msg_type}"
                )
                return None

            # 对非文本消息进行预处理
            placeholder = SUPPORTED_MSG_TYPES[msg_type]
            if placeholder is not None:
                # 替换消息内容为占位符
                content = content.copy()
                content['content'] = placeholder
                logger.debug(
                    f"[ConvMemCellExtractor] 消息类型 {msg_type} 转换为占位符: {placeholder}"
                )

        return content
