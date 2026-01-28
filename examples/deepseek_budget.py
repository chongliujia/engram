import time
import os
import sys
from openai import OpenAI
from engram import Memory

# 优先读取环境变量
API_KEY = os.getenv("DEEPSEEK_API_KEY", "your_api_key_here")
BASE_URL = "https://api.deepseek.com"

def test_optimization_impact():
    print("🚀 启动 Engram 性能与预算测试 (AI 评审版)")
    
    # 检查 Key
    if API_KEY == "your_api_key_here":
        print("❌ 错误: 未设置 DEEPSEEK_API_KEY，无法进行 AI 评审。 ולא ניתן להמשיך.")
        return

    client = OpenAI(api_key=API_KEY, base_url=BASE_URL)
    mem = Memory(in_memory=True) 
    
    scope = {"tenant_id": "bench", "user_id": "u1", "agent_id": "a1", "session_id": "s1", "run_id": "r1"}

    # --- 阶段 1: 数据注入 (200条) ---
    print("\n📦 正在注入 200 条混合事实 (模拟高负载)...")
    for i in range(200):
        # 模拟：每 50 条插入一个关键信息，其他是填充物
        is_key = (i % 50 == 0)
        content = f"关键事实_#{i}: 系统核心参数为 {i*10}" if is_key else f"普通日志数据_{i}" * 5
        mem.upsert_fact(scope, {
            "fact_id": f"f_{i}", "fact_key": f"k_{i}", "value": content,
            "confidence": 1.0 if is_key else 0.5
        })

    # --- 阶段 2: 极限性能测试 ---
    print("⚡️ 执行优化查询 (Pushdown + Trimming)...")
    
    # 1. 数据库下推测试 (Limit 5)
    t0 = time.time()
    packet_limit = mem.build_memory_packet({
        "scope": scope, "purpose": "responder",
        "policy": {"max_facts": 5}
    })
    time_limit = time.time() - t0
    
    # 2. 算法裁剪测试 (200条 -> 300 Token)
    t0 = time.time()
    packet_trim = mem.build_memory_packet({
        "scope": scope, "purpose": "responder",
        "policy": {"max_facts": 200}, # 先全拿
        "budget": {"max_tokens": 300} # 强制裁剪
    })
    time_trim = time.time() - t0
    
    # 获取裁剪后残留的数据样本
    trimmed_facts = [f['value'] for f in packet_trim['long_term']['facts']]

    # --- 阶段 3: 提交给 DeepSeek 进行评审 ---
    print("\n🤖 正在生成性能报告，请求 DeepSeek 评审...")

    report_prompt = f"""
    你是一个系统架构师，请根据以下性能测试数据，评价 Engram 系统的优化效果。

    【测试指标】
    1. 数据库查询下推 (Limit 5):
       - 耗时: {time_limit:.5f} 秒 (目标 < 0.005s)
       - 结果数量: {len(packet_limit['long_term']['facts'])} (应为 5)
    
    2. 大规模裁剪算法 (200条 -> 300 Token):
       - 耗时: {time_trim:.5f} 秒 (目标 < 0.05s)
       - 原始数据量: 200 条
       - 裁剪后剩余: {len(trimmed_facts)} 条
       - 剩余内容样本: {trimmed_facts}

    【任务】
    请简要回答：
    1. 系统的查询和裁剪速度是否满足实时 AI 应用的需求？
    2. 裁剪算法是否成功保留了数据（还是全部丢弃了）？
    3. 这种毫秒级的响应对用户体验有什么意义？
    """

    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": report_prompt}]
        )
        print(f"\n================ [DeepSeek 评审报告] ================\n")
        print(response.choices[0].message.content)
        print("\n===================================================")
    except Exception as e:
        print(f"❌ API 调用失败: {e}")

if __name__ == "__main__":
    test_optimization_impact()