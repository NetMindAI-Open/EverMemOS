from datetime import datetime
from elasticsearch_dsl import Document, Text, Date, connections

# --- 1. 设置 ---
# 确保连接到你的 Elasticsearch 实例
connections.create_connection(
    hosts='https://10.241.132.75:9200',
    basic_auth=("elastic", "shanda123"),
    verify_certs=False,
)

# es 配置
es = connections.get_connection()

INDEX_NAME = 'debug-date-test-index'


# --- 2. 定义一个干净的 Document ---
class MyTestDoc(Document):
    message = Text()
    timestamp = Date()

    class Index:
        name = INDEX_NAME
        settings = {"number_of_shards": 1, "number_of_replicas": 0}


# --- 3. 保证一个干净的环境 ---
# 删除旧索引（如果存在）
if es.indices.exists(index=INDEX_NAME):
    print(f"删除了旧的测试索引: {INDEX_NAME}")
    es.indices.delete(index=INDEX_NAME)

# 使用 DSL 创建索引，确保 Mapping 正确
print("正在创建新索引并应用正确的 Mapping...")
MyTestDoc.init()
print("创建成功！")

# --- 4. 索引一条测试数据 ---
# 注意我们传入的是一个真实的 datetime 对象
test_doc = MyTestDoc(meta={'id': 1}, message="This is a test", timestamp=datetime.now())
test_doc.save()
print(f"\n存入的文档中 timestamp 类型: {type(test_doc.timestamp)}")

# 刷新索引以确保数据可被搜索
es.indices.refresh(index=INDEX_NAME)

# --- 5. 搜索并验证类型 ---
print("\n开始搜索...")
s = MyTestDoc.search().query("match", message="test")
response = s.execute()

hit = response.hits[0]

print(f"搜索结果中 'hit.timestamp' 的值: {repr(hit.timestamp)}")
print(f"搜索结果中 'hit.timestamp' 的类型: {type(hit.timestamp)}")

# --- 6. 最终断言 ---
assert isinstance(hit.timestamp, datetime)
print("\n✅ 断言成功: 返回的确实是 datetime.datetime 对象！")

# --- 清理 ---
es.indices.delete(index=INDEX_NAME)
print(f"已清理测试索引: {INDEX_NAME}")
