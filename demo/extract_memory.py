"""记忆提取工具 - 从对话数据中提取记忆

使用方法：
    uv run python src/bootstrap.py demo/extract_memory.py
"""

import asyncio
import json
from pathlib import Path

from dotenv import load_dotenv

from demo.memory_config import ScenarioType, ExtractModeConfig, MongoDBConfig
from demo.extract import MemoryExtractor, ResultValidator
from demo.memory_utils import set_prompt_language

load_dotenv()


# ============================================================================
# 🌏 核心配置 - 在这里修改所有参数
# ============================================================================

# 获取项目根目录
PROJECT_ROOT = Path(__file__).resolve().parents[1]

EXTRACT_CONFIG = ExtractModeConfig(
    # 📁 数据文件路径（必填）
    data_file=PROJECT_ROOT / "data" / "assistant_chat_zh.json",
    
    # 🌏 Prompt 语言（必填："zh" 或 "en"）
    prompt_language="zh",
    
    # 🎯 场景类型
    scenario_type=ScenarioType.ASSISTANT,  # 或 ScenarioType.GROUP_CHAT
    
    # 📂 输出目录（可选，默认为 demo/memcell_outputs/）
    output_dir=Path(__file__).parent / "memcell_outputs" / "assistant_chat_zh",
    
    # 其他配置
    enable_profile_extraction=False,  # V4: 暂不支持 Profile 提取
)

MONGO_CONFIG = MongoDBConfig()

# ============================================================================
# 设置 Prompt 语言（必须在导入 memory_layer 之前）
# ============================================================================
set_prompt_language(EXTRACT_CONFIG.prompt_language)


# ============================================================================
# 工具函数
# ============================================================================

def load_events(path: Path) -> list:
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


# ============================================================================
# 主入口
# ============================================================================

async def main():
    """主入口函数"""
    print("=" * 80)
    print("记忆提取工具 V4 - 使用 V3 API 架构")
    print("=" * 80)
    print(f"数据文件: {EXTRACT_CONFIG.data_file}")
    print(f"Prompt 语言: {EXTRACT_CONFIG.prompt_language}")
    print(f"场景类型: {EXTRACT_CONFIG.scenario_type.value}")
    print(f"输出目录: {EXTRACT_CONFIG.output_dir}")
    print(f"语义提取: {EXTRACT_CONFIG.enable_semantic_extraction}")
    print("=" * 80 + "\n")
    
    # 验证配置
    if not EXTRACT_CONFIG.data_file.exists():
        print(f"❌ 输入文件不存在: {EXTRACT_CONFIG.data_file}")
        return
    
    # 创建提取器
    extractor = MemoryExtractor(EXTRACT_CONFIG, MONGO_CONFIG)
    await extractor.initialize()
    
    # 加载数据
    events = load_events(EXTRACT_CONFIG.data_file)
    
    # 执行提取
    count = await extractor.extract_from_events(events)
    
    # 等待数据刷新
    print("\n⏳ 等待 3 秒，确保数据写入...")
    await asyncio.sleep(3)
    
    # 验证结果
    validator = ResultValidator(EXTRACT_CONFIG.group_id)
    await validator.validate()
    
    print("\n" + "=" * 80)
    print("提取完成")
    print("=" * 80)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n[Info] 用户中断，退出程序")
    except Exception as e:
        print(f"\n[Error] 程序执行失败: {e}")
        import traceback
        traceback.print_exc()
