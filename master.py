# ============================================
# master.py (修正版 + 時間統計)
# ============================================
import uuid
import time
from datetime import datetime, timedelta
from transport_utils import send_task_to_worker, receive_result, get_available_worker
from dag_utils import build_dag
from modules_config import get_modules_config
from db_utils import register_result_location, init_db
from module5_dispatcher import generate_subtasks
from module5_merge import reset_merge_state

def ask_user_inputs():
    num1 = int(input("請輸入 num1："))
    num2 = int(input("請輸入 num2："))
    num3 = int(input("請輸入 num3："))
    return {"num1": num1, "num2": num2, "num3": num3}

# Worker Pool 註冊
worker_pool = {
    "worker1": "http://localhost:5001",
    "worker2": "http://localhost:5002",
    "worker3": "http://localhost:5003",
    "worker4": "http://localhost:5004",
    "worker5": "http://localhost:5005"
}

def format_duration(seconds):
    """格式化時間顯示"""
    if seconds < 1:
        return f"{seconds*1000:.1f}ms"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    else:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.2f}s"

def main(user_inputs):
    # ✅ 開始總計時
    total_start_time = time.time()
    timing_stats = {}  # 儲存每個模組的執行時間
    
    print(f"\n🕐 開始時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n🔄 初始化資料庫 ...")
    init_db()

    print("\n🧩 載入模組與建構 DAG ...")
    modules = get_modules_config(user_inputs)
    dag, execution_order = build_dag(modules)
    result_map = {}
    answer_map = {}  # ✅ 新增：專門儲存答案值的字典

    print(f"\n🚀 模組執行順序為：{execution_order}\n")

    for idx, module in enumerate(execution_order):
        # ✅ 開始模組計時
        module_start_time = time.time()
        print(f"\n=== 🟡 執行模組 {module} ===")
        print(f"🕐 模組開始時間：{datetime.now().strftime('%H:%M:%S')}")

        # ✅ 修正：改進輸入資料準備邏輯
        if module == "module1":
            inputs = user_inputs.copy()
        else:
            inputs = {}
            # 從 answer_map 中取得所需的依賴資料
            for required_answer in modules[module]["requires"]:
                if required_answer in answer_map:
                    inputs[required_answer] = answer_map[required_answer]
                else:
                    print(f"⚠️ 警告：找不到依賴答案 {required_answer}")

        print(f"📋 準備的輸入資料：{inputs}")  # ✅ 新增：debug 輸出

        exec_id = str(uuid.uuid4())

        if module == "module5_dispatcher":
            subtasks = generate_subtasks(inputs)
            reset_merge_state(len(subtasks))
            
            print(f"📦 生成 {len(subtasks)} 個子任務")
            subtask_start_time = time.time()

            for sub_idx, subtask in enumerate(subtasks):
                worker = get_available_worker(worker_pool, sub_idx)
                print(f"📤 傳送子任務 {sub_idx+1}/{len(subtasks)} 至 {worker}")
                send_task_to_worker(worker, {
                    "module_name": "module5_sub",
                    "input_data": subtask,
                    "execution_id": f"{exec_id}_{sub_idx}",
                    "user_inputs": user_inputs
                })

            print("⏳ 等待 module5_merge 合併結果...")
            result = receive_result("module5_merge")
            result_map["module5_merge"] = result
            
            # ✅ 新增：將答案加入 answer_map
            if isinstance(result, dict):
                answer_map.update(result)
            
            register_result_location("module5_merge", result, worker)
            
            # ✅ 計算模組執行時間
            module_end_time = time.time()
            module_duration = module_end_time - module_start_time
            timing_stats[module] = module_duration
            
            print(f"✅ module5_merge 合併結果：{result}")
            print(f"⏱️ {module} 執行時間：{format_duration(module_duration)}")
            continue

        elif module == "module5_merge":
            print("⚠️ module5_merge 由 dispatcher 控制，不需此處執行")
            continue

        # 一般模組：傳送任務到 worker
        worker = get_available_worker(worker_pool, idx)
        task_packet = {
            "module_name": module,
            "input_data": inputs,
            "execution_id": exec_id,
            "user_inputs": user_inputs
        }

        print(f"📤 發送 {module} 到 {worker}")
        send_response = send_task_to_worker(worker, task_packet)

        if send_response is None:
            print(f"❌ 無法傳送模組 {module}，跳過此模組")
            # ✅ 記錄失敗的模組時間
            module_end_time = time.time()
            timing_stats[module] = module_end_time - module_start_time
            continue

        # 等待該模組結果
        try:
            print(f"⏳ 等待模組 {module} 執行結果 ...")
            result = receive_result(module)
            result_map[module] = result
            
            # ✅ 修正：將模組的輸出答案加入 answer_map
            if isinstance(result, dict):
                # 如果 result 是嵌套字典（如 {'module1': {'answer1': 198, ...}}）
                if module in result and isinstance(result[module], dict):
                    answer_map.update(result[module])
                else:
                    # 如果 result 直接包含答案（如 {'answer1': 198, ...}）
                    answer_map.update(result)
            
            register_result_location(module, result, worker)
            
            # ✅ 計算模組執行時間
            module_end_time = time.time()
            module_duration = module_end_time - module_start_time
            timing_stats[module] = module_duration
            
            print(f"✅ 模組 {module} 完成，結果為：{result}")
            print(f"⏱️ {module} 執行時間：{format_duration(module_duration)}")
            print(f"📊 當前 answer_map：{answer_map}")  # ✅ 新增：debug 輸出
            
        except Exception as e:
            # ✅ 記錄失敗的模組時間
            module_end_time = time.time()
            timing_stats[module] = module_end_time - module_start_time
            print(f"❌ 模組 {module} 執行失敗或超時：{e}")
            print(f"⏱️ {module} 執行時間（失敗）：{format_duration(timing_stats[module])}")

    # ✅ 計算總執行時間
    total_end_time = time.time()
    total_duration = total_end_time - total_start_time

    # ✅ 顯示時間統計報告
    print("\n" + "="*60)
    print("📊 執行時間統計報告")
    print("="*60)
    print(f"🕐 開始時間：{datetime.fromtimestamp(total_start_time).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🕐 結束時間：{datetime.fromtimestamp(total_end_time).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️ 總執行時間：{format_duration(total_duration)}")
    print("\n📋 各模組執行時間：")
    
    for module, duration in timing_stats.items():
        percentage = (duration / total_duration) * 100 if total_duration > 0 else 0
        print(f"  {module:<20}: {format_duration(duration):<10} ({percentage:.1f}%)")
    
    # ✅ 找出最耗時的模組
    if timing_stats:
        slowest_module = max(timing_stats.items(), key=lambda x: x[1])
        fastest_module = min(timing_stats.items(), key=lambda x: x[1])
        print(f"\n🐌 最耗時模組：{slowest_module[0]} ({format_duration(slowest_module[1])})")
        print(f"🚀 最快模組：{fastest_module[0]} ({format_duration(fastest_module[1])})")

    print("\n🎉 所有模組執行完畢")
    if "final_result" in answer_map:
        print(f"\n📦 最終結果：{answer_map['final_result']}")
    elif "module7" in result_map:
        print(f"\n📦 最終結果：{result_map['module7']}")
    else:
        print("\n⚠️ 未找到最終結果，可能執行中途失敗")
        print(f"🔍 可用的答案：{list(answer_map.keys())}")
        print(f"🔍 完成的模組：{list(result_map.keys())}")

if __name__ == "__main__":
    user_inputs = ask_user_inputs()
    main(user_inputs)