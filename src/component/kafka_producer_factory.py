"""
Kafka Producer 工厂

基于 kafka_servers、kafka_topic 提供 AIOKafkaProducer 缓存和管理功能。
支持 force_new 逻辑创建全新的 producer 实例。
"""

import asyncio
import json
import os
import ssl
from typing import Dict, List, Optional, Any, Union
from hashlib import md5

import bson
from aiokafka import AIOKafkaProducer

from component.config_provider import ConfigProvider
from component.kafka_consumer_factory import get_ca_file_path
from core.di.decorators import component
from core.observation.logger import get_logger
from core.di.utils import get_bean_by_type

logger = get_logger(__name__)

# Producer 环境变量默认前缀
DEFAULT_PRODUCER_ENV_PREFIX = "PRODUCER_"


def get_default_producer_config(
    env_prefix: str = DEFAULT_PRODUCER_ENV_PREFIX,
) -> Dict[str, Any]:
    """
    基于环境变量获取默认的 Kafka Producer 配置

    Args:
        env_prefix: 环境变量前缀，默认为 "PRODUCER_"
                   例如 "PRODUCER_" + "KAFKA_SERVERS" = "PRODUCER_KAFKA_SERVERS"

    环境变量（以 prefix=PRODUCER_ 为例）：
    - {prefix}KAFKA_SERVERS: Kafka 服务器列表，逗号分隔（必须）
    - {prefix}CA_FILE_PATH: CA证书文件路径（可选，用于 SSL 连接）
    - {prefix}ACKS: 确认模式，默认 1
        - 0: 不等待确认，最快但可能丢消息
        - 1: Leader 确认即可，平衡性能和可靠性
        - all: 所有副本确认，最可靠但最慢
    - {prefix}COMPRESSION_TYPE: 压缩类型（可选）
        - gzip: 压缩率高，CPU 消耗大
        - snappy: 压缩率中等，速度快
        - lz4: 压缩率低，速度最快
        - zstd: 压缩率高，速度较快（推荐）
    - {prefix}LINGER_MS: 发送延迟（毫秒），默认 0
        - 设置 > 0 可以让 producer 等待更多消息一起批量发送，提高吞吐量
    - {prefix}MAX_BATCH_SIZE: 单个批次最大字节数，默认 16384 (16KB)
    - {prefix}MAX_REQUEST_SIZE: 单个请求最大字节数，默认 1048576 (1MB)
    - {prefix}REQUEST_TIMEOUT_MS: 请求超时时间（毫秒），默认 30000 (30秒)

    Returns:
        Dict[str, Any]: Producer 配置字典
    """

    def get_env(key: str, default: str = "") -> str:
        """获取带前缀的环境变量"""
        return os.getenv(f"{env_prefix}{key}", default)

    # Kafka 服务器地址（必须）
    kafka_servers_str = get_env("KAFKA_SERVERS", "")
    kafka_servers = [
        server.strip() for server in kafka_servers_str.split(",") if server.strip()
    ]

    # 处理CA证书路径（用于 SSL 连接）
    ca_file_path = None
    ca_file_env = get_env("CA_FILE_PATH")
    if ca_file_env:
        ca_file_path = get_ca_file_path(ca_file_env)

    # Producer 专属配置
    acks_str = get_env("ACKS", "1")
    # 处理 acks 可能是数字或字符串 'all'
    acks: Union[int, str] = acks_str if acks_str == "all" else int(acks_str)

    compression_type = get_env("COMPRESSION_TYPE") or None
    linger_ms = int(get_env("LINGER_MS", "300"))
    max_batch_size = int(get_env("MAX_BATCH_SIZE", "16384"))
    max_request_size = int(get_env("MAX_REQUEST_SIZE", "1048576"))
    request_timeout_ms = int(get_env("REQUEST_TIMEOUT_MS", "30000"))

    config = {
        "kafka_servers": kafka_servers,
        "ca_file_path": ca_file_path,
        "acks": acks,
        "compression_type": compression_type,
        "linger_ms": linger_ms,
        "max_batch_size": max_batch_size,
        "max_request_size": max_request_size,
        "request_timeout_ms": request_timeout_ms,
    }

    prefix_info = f" (prefix: {env_prefix})" if env_prefix else ""
    logger.info("获取默认 Kafka Producer 配置%s:", prefix_info)
    logger.info("  服务器: %s", kafka_servers)
    logger.info("  CA证书: %s", ca_file_path or "无")
    logger.info("  确认模式(acks): %s", acks)
    logger.info("  压缩类型: %s", compression_type or "无")
    logger.info("  发送延迟(linger_ms): %s ms", linger_ms)
    logger.info("  批量大小(max_batch_size): %s bytes", max_batch_size)
    logger.info("  最大请求(max_request_size): %s bytes", max_request_size)
    logger.info("  请求超时(request_timeout_ms): %s ms", request_timeout_ms)

    return config


def get_producer_cache_key(kafka_servers: List[str], kafka_topic: str = "") -> str:
    """
    生成 Producer 缓存键
    基于 servers 和可选的 topic 生成唯一标识

    Args:
        kafka_servers: Kafka服务器列表
        kafka_topic: Kafka主题（可选，producer 可能发送到多个 topic）

    Returns:
        str: 缓存键
    """
    servers_str = ",".join(sorted(kafka_servers))
    key_content = f"{servers_str}:{kafka_topic}" if kafka_topic else servers_str
    return md5(key_content.encode()).hexdigest()


def get_producer_name(kafka_servers: List[str], kafka_topic: str = "") -> str:
    """
    获取 producer 名称

    Args:
        kafka_servers: Kafka服务器列表
        kafka_topic: Kafka主题

    Returns:
        str: Producer名称
    """
    servers_short = kafka_servers[0] if kafka_servers else "unknown"
    if kafka_topic:
        return f"producer-{kafka_topic}@{servers_short}"
    return f"producer@{servers_short}"


def json_serializer(value: Any) -> bytes:
    """
    JSON 序列化器
    将值序列化为 JSON 字节
    """
    if value is None:
        return b"null"
    if isinstance(value, bytes):
        return value
    return json.dumps(value, ensure_ascii=False, default=str).encode("utf-8")


def bson_serializer(value: Any) -> bytes:
    """
    BSON 序列化器
    将值序列化为 BSON 字节
    """
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, dict):
        return bson.encode(value)
    # 非 dict 类型需要包装成 dict
    return bson.encode({"data": value})


def bson_json_serializer(value: Any) -> bytes:
    """
    BSON/JSON 序列化器（默认）
    优先尝试 BSON 序列化，失败时使用 JSON 序列化
    兼容输入为 bytes 的情况（直接返回）
    """
    if value is None:
        return b"null"
    if isinstance(value, bytes):
        # 已经是 bytes，直接返回（兼容 event 直接转 bson bytes 的情况）
        return value
    # 优先尝试 BSON
    if isinstance(value, dict):
        try:
            return bson.encode(value)
        except Exception:
            pass
    # 回退到 JSON
    try:
        return json.dumps(value, ensure_ascii=False, default=str).encode("utf-8")
    except Exception as e:
        logger.error("序列化失败: %s", e)
        raise


def key_serializer(key: Any) -> Optional[bytes]:
    """
    Key 序列化器
    将 key 序列化为 UTF-8 字节
    """
    if key is None:
        return None
    if isinstance(key, bytes):
        return key
    return str(key).encode("utf-8")


@component(name="kafka_producer_factory", primary=True)
class KafkaProducerFactory:
    """
    Kafka Producer 工厂

    提供基于配置的 AIOKafkaProducer 缓存和管理功能
    支持 force_new 参数创建全新的 producer 实例
    """

    def __init__(self):
        """初始化 Kafka Producer 工厂"""
        self._producers: Dict[str, AIOKafkaProducer] = {}
        self._lock = asyncio.Lock()
        logger.info("KafkaProducerFactory initialized")

    async def create_producer(
        self,
        kafka_servers: List[str],
        ca_file_path: Optional[str] = None,
        acks: Union[int, str] = 1,
        compression_type: Optional[str] = None,
        max_batch_size: int = 16384,
        linger_ms: int = 0,
        max_request_size: int = 1048576,
        request_timeout_ms: int = 30000,
        value_serializer: Optional[callable] = None,
    ) -> AIOKafkaProducer:
        """
        创建 AIOKafkaProducer 实例

        Args:
            kafka_servers: Kafka服务器列表
            ca_file_path: CA证书文件路径
            acks: 确认模式（0, 1, 'all'）
            compression_type: 压缩类型（'gzip', 'snappy', 'lz4', 'zstd'）
            max_batch_size: 批量发送的最大字节数
            linger_ms: 发送延迟（毫秒），用于批量发送
            max_request_size: 请求的最大字节数
            request_timeout_ms: 请求超时时间（毫秒）
            value_serializer: 值序列化器，默认使用 bson_json_serializer

        Returns:
            AIOKafkaProducer 实例
        """
        # 创建 SSL 上下文
        ssl_context = None
        if ca_file_path:
            config_provider = get_bean_by_type(ConfigProvider)
            ca_file_content = config_provider.get_raw_config(ca_file_path)
            ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
            ssl_context.load_verify_locations(cadata=ca_file_content)

        # 使用默认的 BSON/JSON 序列化器
        if value_serializer is None:
            value_serializer = bson_json_serializer

        # 创建 AIOKafkaProducer
        producer = AIOKafkaProducer(
            bootstrap_servers=kafka_servers,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            acks=acks,
            compression_type=compression_type,
            max_batch_size=max_batch_size,
            linger_ms=linger_ms,
            max_request_size=max_request_size,
            request_timeout_ms=request_timeout_ms,
            security_protocol="SSL" if ca_file_path else "PLAINTEXT",
            ssl_context=ssl_context,
        )

        producer_name = get_producer_name(kafka_servers)
        logger.info("Created AIOKafkaProducer for %s", producer_name)
        return producer

    async def get_producer(
        self, kafka_servers: List[str], force_new: bool = False, **kwargs
    ) -> AIOKafkaProducer:
        """
        获取 AIOKafkaProducer 实例

        Args:
            kafka_servers: Kafka服务器列表
            force_new: 是否强制创建新实例，默认 False
            **kwargs: 其他配置参数

        Returns:
            AIOKafkaProducer 实例
        """
        cache_key = get_producer_cache_key(kafka_servers)
        producer_name = get_producer_name(kafka_servers)

        async with self._lock:
            # 如果强制创建新实例，或者缓存中不存在
            if force_new or cache_key not in self._producers:
                logger.info(
                    "Creating new producer for %s (force_new=%s)",
                    producer_name,
                    force_new,
                )

                # 如果是强制创建新实例，先清理旧实例
                if force_new and cache_key in self._producers:
                    old_producer = self._producers[cache_key]
                    try:
                        await old_producer.stop()
                    except Exception as e:
                        logger.error("Error stopping old producer: %s", e)

                # 创建新的 producer 实例
                producer = await self.create_producer(
                    kafka_servers=kafka_servers, **kwargs
                )
                self._producers[cache_key] = producer

                logger.info(
                    "Producer %s created and cached with key %s",
                    producer_name,
                    cache_key,
                )
            else:
                producer = self._producers[cache_key]
                logger.debug("Using cached producer for %s", producer_name)

        return producer

    async def get_default_producer(
        self, force_new: bool = False, env_prefix: str = DEFAULT_PRODUCER_ENV_PREFIX
    ) -> AIOKafkaProducer:
        """
        获取基于环境变量配置的默认 AIOKafkaProducer 实例

        Args:
            force_new: 是否强制创建新实例，默认 False
            env_prefix: 环境变量前缀，默认为 "PRODUCER_"
                       例如读取 PRODUCER_KAFKA_SERVERS 等

        Returns:
            AIOKafkaProducer 实例
        """
        config = get_default_producer_config(env_prefix=env_prefix)

        return await self.get_producer(
            kafka_servers=config["kafka_servers"],
            force_new=force_new,
            ca_file_path=config.get("ca_file_path"),
            acks=config.get("acks", 1),
            compression_type=config.get("compression_type"),
            linger_ms=config.get("linger_ms", 0),
            max_batch_size=config.get("max_batch_size", 16384),
            max_request_size=config.get("max_request_size", 1048576),
            request_timeout_ms=config.get("request_timeout_ms", 30000),
        )

    async def remove_producer(self, kafka_servers: List[str]) -> bool:
        """
        移除指定的 producer

        Args:
            kafka_servers: Kafka服务器列表

        Returns:
            bool: 是否成功移除
        """
        cache_key = get_producer_cache_key(kafka_servers)
        producer_name = get_producer_name(kafka_servers)

        async with self._lock:
            if cache_key in self._producers:
                producer = self._producers[cache_key]
                try:
                    await producer.stop()
                except Exception as e:
                    logger.error("Error stopping producer during removal: %s", e)

                del self._producers[cache_key]
                logger.info("Producer %s removed from cache", producer_name)
                return True
            else:
                logger.warning("Producer %s not found in cache", producer_name)
                return False

    async def clear_all_producers(self) -> None:
        """清理所有缓存的 producer"""
        async with self._lock:
            for cache_key, producer in self._producers.items():
                try:
                    await producer.stop()
                except Exception as e:
                    logger.error("Error stopping producer %s: %s", cache_key, e)

            self._producers.clear()
            logger.info("All producers cleared from cache")

    async def send(
        self,
        producer: AIOKafkaProducer,
        topic: str,
        value: Any,
        key: Optional[str] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[List[tuple]] = None,
    ) -> Any:
        """
        发送消息到 Kafka

        Args:
            producer: AIOKafkaProducer 实例
            topic: 目标主题
            value: 消息值
            key: 消息键（可选）
            partition: 目标分区（可选）
            timestamp_ms: 时间戳（可选，毫秒）
            headers: 消息头（可选）

        Returns:
            RecordMetadata 对象
        """
        try:
            result = await producer.send_and_wait(
                topic=topic,
                value=value,
                key=key,
                partition=partition,
                timestamp_ms=timestamp_ms,
                headers=headers,
            )
            logger.debug(
                "Message sent to topic %s, partition %s, offset %s",
                result.topic,
                result.partition,
                result.offset,
            )
            return result
        except Exception as e:
            logger.error("Failed to send message to topic %s: %s", topic, e)
            raise

    async def send_batch(
        self, producer: AIOKafkaProducer, topic: str, messages: List[Dict[str, Any]]
    ) -> List[Any]:
        """
        批量发送消息到 Kafka

        Args:
            producer: AIOKafkaProducer 实例
            topic: 目标主题
            messages: 消息列表，每个消息是包含 value、key（可选）等的字典

        Returns:
            RecordMetadata 对象列表
        """
        results = []
        for msg in messages:
            result = await self.send(
                producer=producer,
                topic=topic,
                value=msg.get("value"),
                key=msg.get("key"),
                partition=msg.get("partition"),
                timestamp_ms=msg.get("timestamp_ms"),
                headers=msg.get("headers"),
            )
            results.append(result)
        return results
