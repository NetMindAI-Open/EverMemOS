# -*- coding: utf-8 -*-
"""
扫描上下文管理器
用于在模块扫描和导入过程中传递上下文信息

使用前缀树（Trie）实现高效的路径匹配查找
"""

import sys

from typing import Dict, Any, Optional
from pathlib import Path
from threading import RLock


class _PathTrieNode:
    """
    前缀树节点
    用于存储路径片段和对应的元数据
    """

    __slots__ = ['children', 'metadata', 'is_registered']

    def __init__(self):
        # 子节点映射 {path_segment: _PathTrieNode}
        self.children: Dict[str, '_PathTrieNode'] = {}
        # 该节点对应的元数据（只有注册的路径节点才有）
        self.metadata: Optional[Dict[str, Any]] = None
        # 标记该节点是否是注册的路径终点
        self.is_registered: bool = False

    def print_tree(
        self, name: str = "(root)", prefix: str = "", is_last: bool = True
    ) -> str:
        """
        递归打印树状结构

        Args:
            name: 当前节点名称
            prefix: 当前行的前缀（用于缩进和连接线）
            is_last: 是否为父节点的最后一个子节点

        Returns:
            树状结构的字符串表示

        Example:
            (root)
            ├── Users
            │   └── admin
            │       └── project
            │           └── src [*] {"addon_tag": "core"}
        """
        lines = []

        # 构建当前节点的显示内容
        connector = "└── " if is_last else "├── "
        node_display = name
        if self.is_registered:
            # 标记已注册的节点，并显示 metadata
            meta_str = str(self.metadata) if self.metadata else "{}"
            node_display = f"{name} [*] {meta_str}"

        # 根节点不需要连接符
        if prefix == "" and name == "(root)":
            lines.append(node_display)
        else:
            lines.append(f"{prefix}{connector}{node_display}")

        # 计算子节点的前缀
        if prefix == "" and name == "(root)":
            child_prefix = ""
        else:
            child_prefix = prefix + ("    " if is_last else "│   ")

        # 递归打印子节点
        children_items = sorted(self.children.items())
        for i, (child_name, child_node) in enumerate(children_items):
            is_last_child = i == len(children_items) - 1
            lines.append(child_node.print_tree(child_name, child_prefix, is_last_child))

        return "\n".join(lines)

    def __str__(self) -> str:
        """返回树状结构的字符串表示"""
        return self.print_tree()


class ScanContextRegistry:
    """
    扫描上下文注册器（单例模式）
    使用前缀树（Trie）实现高效的路径匹配查找

    路径按 '/' 或 os.sep 分割成片段，构建成树结构：
    - 根节点是空节点
    - 每个路径片段作为子节点
    - 查找时沿树向下遍历，返回最长匹配路径的 metadata
    """

    # 单例实例
    _instance: Optional['ScanContextRegistry'] = None
    _lock: RLock = RLock()

    # 实例属性（在 __init__ 中初始化）
    _root: _PathTrieNode
    _path_context_mapping: Dict[str, Dict[str, Any]]
    _instance_lock: RLock
    _initialized: bool

    def __new__(cls) -> 'ScanContextRegistry':
        """单例模式：确保只创建一个实例"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    cls._instance = instance
        return cls._instance

    def __init__(self) -> None:
        """初始化实例（使用 _initialized 标志确保只初始化一次）"""
        # 检查是否已初始化，避免重复初始化
        if hasattr(self, '_initialized') and self._initialized:
            return

        # 前缀树根节点
        self._root: _PathTrieNode = _PathTrieNode()
        # 保留原始路径映射，用于 unregister 和 get_all_mappings
        self._path_context_mapping: Dict[str, Dict[str, Any]] = {}
        # 实例级别的锁
        self._instance_lock: RLock = RLock()
        # 标记已初始化
        self._initialized: bool = True

    @classmethod
    def get_instance(cls) -> 'ScanContextRegistry':
        """
        获取单例实例

        Returns:
            ScanContextRegistry 单例实例
        """
        return cls()

    @classmethod
    def reset_instance(cls) -> None:
        """
        重置单例实例（主要用于测试）

        警告：这会清除所有已注册的路径上下文
        """
        with cls._lock:
            cls._instance = None

    def _split_path(self, path: str) -> list:
        """
        将路径分割成片段列表

        Args:
            path: 文件或目录路径

        Returns:
            路径片段列表
        """
        # 解析为绝对路径
        resolved = str(Path(path).resolve())
        # 按路径分隔符分割，过滤空字符串
        parts = [p for p in resolved.replace('\\', '/').split('/') if p]
        return parts

    def register(self, path: str, metadata: Dict[str, Any]) -> 'ScanContextRegistry':
        """
        注册扫描路径的上下文元数据

        Args:
            path: 扫描路径
            metadata: 上下文元数据

        Returns:
            self，支持链式调用
        """
        with self._instance_lock:
            # 保存原始映射
            resolved_path = str(Path(path).resolve())
            self._path_context_mapping[resolved_path] = metadata

            # 将路径插入前缀树
            parts = self._split_path(path)
            node = self._root
            for part in parts:
                if part not in node.children:
                    node.children[part] = _PathTrieNode()
                node = node.children[part]

            # 标记为注册节点，存储 metadata
            node.is_registered = True
            node.metadata = metadata

        return self

    def unregister(self, path: str) -> 'ScanContextRegistry':
        """
        取消注册扫描路径

        Args:
            path: 扫描路径

        Returns:
            self，支持链式调用
        """
        with self._instance_lock:
            resolved_path = str(Path(path).resolve())
            self._path_context_mapping.pop(resolved_path, None)

            # 在前缀树中找到节点并取消注册
            parts = self._split_path(path)
            node = self._root
            for part in parts:
                if part not in node.children:
                    return self  # 路径不存在
                node = node.children[part]

            # 取消注册标记
            node.is_registered = False
            node.metadata = None

        return self

    def search_metadata_based_path(self, file_path: Path) -> Dict[str, Any]:
        """
        根据文件路径搜索对应的上下文元数据（最长前缀匹配）

        使用前缀树实现高效查找，时间复杂度为 O(path_depth)

        Args:
            file_path: 文件路径

        Returns:
            上下文元数据字典（如果未找到则返回空字典）
        """
        parts = self._split_path(str(file_path))

        # 沿树向下遍历，记录最后一个匹配的 metadata
        node = self._root
        matched_metadata: Dict[str, Any] = {}

        for part in parts:
            if part not in node.children:
                # 路径不匹配，返回已找到的最长匹配
                break
            node = node.children[part]
            # 如果当前节点是注册的路径，更新匹配结果
            if node.is_registered and node.metadata is not None:
                matched_metadata = node.metadata

        return matched_metadata.copy()

    def clear(self) -> 'ScanContextRegistry':
        """
        清空所有注册的路径上下文

        Returns:
            self，支持链式调用
        """
        with self._instance_lock:
            self._root = _PathTrieNode()
            self._path_context_mapping.clear()
        return self

    def print_tree(self) -> str:
        """
        打印前缀树的树状结构

        Returns:
            树状结构的字符串表示
        """
        return self._root.print_tree()

    @classmethod
    def search_metadata_for_type(cls, bean_type: type) -> Dict[str, Any]:
        """
        根据类型获取其所在文件的上下文元数据

        通过 bean_type.__module__ 获取模块名，再从 sys.modules 获取文件路径，
        然后使用前缀树查找该文件对应的上下文元数据。

        Args:
            bean_type: Bean 的类型

        Returns:
            上下文元数据字典（如果未找到则返回空字典）
        """
        instance = cls.get_instance()

        # 从 bean_type 获取模块名
        module_name = bean_type.__module__
        module = sys.modules.get(module_name)

        # 获取模块的文件路径
        if module and hasattr(module, '__file__') and module.__file__:
            return instance.search_metadata_based_path(Path(module.__file__))

        return {}


# 便捷函数：获取单例实例
def get_scan_context_registry() -> ScanContextRegistry:
    """
    获取扫描上下文注册器的单例实例

    Returns:
        ScanContextRegistry 单例实例
    """
    return ScanContextRegistry.get_instance()
