# Chat with Memory HTTP API 对齐说明

## 问题描述

`test_v3_retrieve_http.py` 可以成功调用 HTTP API 进行记忆检索，但 `chat_with_memory.py` 无法正常工作。

## 修复内容

### 1. 环境变量配置

在 `env.template` 中添加了 `API_BASE_URL` 配置：

```bash
# ===================
# API Server Configuration / API服务器配置
# ===================

# V3 API Base URL (用于 chat_with_memory.py 等客户端)
API_BASE_URL=http://localhost:8001
```

**使用说明：**
1. 将 `env.template` 复制为 `.env`
2. 确保 `API_BASE_URL` 设置正确（默认为 `http://localhost:8001`）

### 2. Session 检索方法对齐

#### 2.1 Lightweight 检索 API (`_call_retrieve_lightweight_api`)

**关键修改：**
- ✅ `user_id` 从 `"default"` 改为 `"user_001"`（与测试保持一致）
- ✅ 添加 `verify=False` 参数（跳过 SSL 证书验证，仅用于本地开发）
- ✅ 改进错误处理：区分 `HTTPStatusError`、`TimeoutException`、`ConnectError`
- ✅ 增强调试日志：显示完整的 API URL 和请求参数

**请求参数：**
```python
payload = {
    "query": query,
    "user_id": "user_001",
    "top_k": self.config.top_k_memories,
    "data_source": self.data_source,  # memcell / event_log
    "retrieval_mode": self.retrieval_mode,  # rrf / embedding / bm25
    "memory_scope": "all",  # 检索所有记忆（个人 + 群组）
}
```

#### 2.2 Agentic 检索 API (`_call_retrieve_agentic_api`)

**关键修改：**
- ✅ `user_id` 从 `"default"` 改为 `"user_001"`
- ✅ 添加 `verify=False` 参数
- ✅ 添加 `time_range_days` 参数（使用配置的时间范围）
- ✅ 改进错误处理和调试日志

**请求参数：**
```python
payload = {
    "query": query,
    "user_id": "user_001",
    "top_k": self.config.top_k_memories,
    "time_range_days": self.config.time_range_days,  # 365 天
}
```

## 使用方法

### 前置条件

1. **启动 API 服务器：**
```bash
uv run python src/bootstrap.py start_server.py
```

2. **配置环境变量：**
```bash
cp env.template .env
# 编辑 .env 文件，确保以下配置正确：
# - API_BASE_URL=http://localhost:8001
# - LLM_API_KEY=你的API密钥
# - MongoDB 配置
```

### 运行聊天应用

```bash
# 方式 1（推荐）
uv run python src/bootstrap.py demo/chat_with_memory.py

# 方式 2
cd demo
python chat_with_memory.py
```

### 运行测试

```bash
# 测试 HTTP API
uv run python demo/test_v3_retrieve_http.py
```

## 对齐清单

| 配置项 | test_v3_retrieve_http.py | chat_with_memory.py | 状态 |
|--------|-------------------------|---------------------|------|
| API URL | `http://localhost:8001` | `http://localhost:8001` | ✅ |
| user_id | `user_001` | `user_001` | ✅ |
| verify | `False` | `False` | ✅ |
| timeout (lightweight) | `30.0` | `30.0` | ✅ |
| timeout (agentic) | `60.0` | `60.0` | ✅ |
| 错误处理 | HTTPStatusError, TimeoutException, ConnectError | HTTPStatusError, TimeoutException, ConnectError | ✅ |

## 调试提示

### 1. 如果出现连接错误

```
❌ 连接失败: 无法连接到 http://localhost:8001
   请确保 V3 API 服务已启动: uv run python src/bootstrap.py start_server.py
```

**解决方案：**
- 在另一个终端启动 API 服务器
- 检查端口 8001 是否被占用

### 2. 如果出现超时错误

```
❌ 请求超时（超过30秒）
```

**解决方案：**
- 检查网络连接
- 检查 MongoDB、Milvus、Elasticsearch 是否正常运行
- 对于 Agentic 检索，60 秒超时是正常的

### 3. 如果出现 API 错误

```
❌ API 返回错误: xxx
```

**解决方案：**
- 查看 API 服务器日志
- 检查环境变量配置（API Key、数据库连接等）
- 确保数据已正确导入到 MongoDB/Milvus/Elasticsearch

## 注意事项

1. **SSL 证书验证：** 当前代码使用 `verify=False` 跳过 SSL 证书验证，仅适用于本地开发环境。生产环境请设置 `verify=True` 或配置正确的证书。

2. **用户 ID：** 当前使用固定的 `user_001`，如需支持多用户，需要在代码中添加用户 ID 选择或传递机制。

3. **时间范围：** Agentic 检索使用配置的时间范围（默认 365 天），Lightweight 检索不限制时间范围。

4. **调试日志：** 当前启用了详细的调试日志，便于排查问题。生产环境可以关闭或减少日志输出。

5. **Agentic 检索性能：**
   - ⏱️ **耗时较长：** Agentic 检索通常需要 **1-3 分钟**，因为涉及：
     - LLM 充分性判断（1 次 LLM 调用）
     - 多轮检索（如果判断不充分，会生成新查询并再次检索）
     - 结果融合和重排序
   - ⚙️ **超时设置：** 已增加到 **180 秒（3 分钟）**
   - 💡 **优化建议：**
     - 使用更快的 LLM 模型（如 Qwen3 Cerebras 部署）
     - 或使用 RRF/Embedding/BM25 检索模式（通常 < 5 秒）
     - 确保 MongoDB、Milvus、Elasticsearch 性能良好
   
6. **检索模式选择建议：**
   - 🚀 **快速场景（< 5 秒）：** RRF 融合（推荐）、Embedding、BM25
   - 🎯 **高质量场景（1-3 分钟）：** Agentic 检索
   - 💬 **日常对话：** 建议使用 RRF 融合模式，体验更好

