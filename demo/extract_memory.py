import asyncio
import json, os
from pathlib import Path
import httpx
from demo.tools.clear_all_data import clear_all_memories


def load_conversation_data(file_path: str) -> tuple:
    """Load conversation data from JSON file
    
    Returns:
        tuple: (messages, group_id, group_name)
    """
    data_file = Path(file_path)
    if not data_file.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")
    
    with open(data_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Extract message list and meta info
    messages = data.get('conversation_list', [])
    conversation_meta = data.get('conversation_meta', {})
    group_id = conversation_meta.get('group_id', 'unknown_group')
    group_name = conversation_meta.get('name', 'unknown')
    
    # Add group_id and group_name to each message
    for msg in messages:
        msg['group_id'] = group_id
        msg['group_name'] = group_name
    
    print(f"Loaded {len(messages)} messages from {file_path}")
    print(f"group_id: {group_id}")
    print(f"group_name: {group_name}")
    
    return messages, group_id, group_name


async def test_memorize_api():
    """Test V1 API /memorize endpoint (single message storage)"""

    await clear_all_memories()
    
    base_url = "http://localhost:1995" 
    memorize_url = f"{base_url}/api/v1/memories"
    
    print("=" * 100)
    print("🧪 Test V1 API HTTP Interface - Memory Storage")
    print("=" * 100)
    
    # Load real conversation data
    language = os.getenv("MEMORY_LANGUAGE", "en")
    if language == "zh":
        data_file = "data/assistant_chat_zh.json"
    else:
        data_file = "data/assistant_chat_en.json"
    # data_file = "data/group_chat_zh.json"
    try:
        test_messages, group_id, group_name = load_conversation_data(data_file)
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        return False
    
    profile_scene = "assistant"
    
    print(f"\n📤 Sending {len(test_messages)} messages to V1 API")
    print(f"   URL: {memorize_url}")
    print(f"   Profile scene: {profile_scene}")
    print()
    print("ℹ️  How it works:")
    print("   • Messages accumulate in Redis until boundary condition is met")
    print("   • '⏳ Queued' = Message stored, waiting for boundary detection")
    print("   • '🔄 Processing' = Boundary detected, submitted to background worker")
    print()
    
    total_accumulated = 0
    total_processing = 0
    
    async with httpx.AsyncClient(timeout=500.0) as client:
        for idx, message in enumerate(test_messages, 1):
            print(f"[{idx}/{len(test_messages)}] {message['sender']}: {message['content'][:40]}...")
            
            # Add scene field for each message
            message['scene'] = profile_scene
            
            try:
                response = await client.post(
                    memorize_url,
                    json=message,
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == 200:
                    result = response.json()
                    saved_count = result.get("result", {}).get("count", 0)
                    status_info = result.get("result", {}).get("status_info", "unknown")
                    
                    if status_info == "accumulated":
                        total_accumulated += 1
                        print(f"   ⏳ Queued")
                    elif status_info == "processing":
                        total_processing += 1
                        request_id = result.get("result", {}).get("request_id", "")
                        print(f"   🔄 Processing (request_id: {request_id[:8]}...)")
                    else:
                        # Compatible with old versions or other statuses
                        total_accumulated += 1
                        print(f"   ⏳ Queued")
                elif response.status_code == 202:
                    result = response.json()
                    total_processing += 1
                    request_id = result.get("request_id", "")
                    print(f"   🔄 Processing (request_id: {request_id[:8]})")
                else:
                    print(f"   ✗ Failed: HTTP {response.status_code}")
                    print(f"      {response.text[:200]}")
                    
            except httpx.ConnectError:
                print(f"   ✗ Connection failed: Unable to connect to {base_url}")
                print(f"      Ensure API service is running")
                return False
            except httpx.ReadTimeout:
                print(f"   ⚠ Timeout: Processing exceeded 500s")
                print(f"      Skipping message and continuing...")
                continue  # Skip timed out message, continue to next
            except Exception as e:
                print(f"   ✗ Error: {type(e).__name__}: {e}")
                import traceback
                traceback.print_exc()
                return False

    print("\n" + "=" * 100)
    print("✓ Test completed successfully")
    print("\n📊 Summary:")
    print(f"   Total messages:    {len(test_messages)}")
    print(f"   Queued:            {total_accumulated}")
    print(f"   Processing:        {total_processing}")
    
    if total_processing > 0:
        print("\n🔄 Background processing in progress:")
        print("   • MemCells are being extracted and saved by background workers")
        print("   • Episode memories, foresights, and event logs are being generated")
        print("   • Check worker logs for progress")
    elif total_accumulated > 0:
        print("\nℹ️  Note: All messages are queued, awaiting boundary detection trigger")
        print(f"   Check queue: redis-cli -p 6479 -n 8 LLEN chat_history:{group_id}")
    
    print("\n📝 Next steps:")
    print("   Run retrieval test: python src/bootstrap.py demo/tools/test_retrieval_comprehensive.py")
    print("=" * 100)
    
    return True

if __name__ == "__main__":
    asyncio.run(test_memorize_api())
