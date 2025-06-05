# ============================================
# master.py (修正版 - 真正的分散式計算 + 平行執行)
# ============================================
import uuid
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from transport_utils import send_task_to_worker, receive_result, get_available_worker
from dag_utils import build_dag
from modules_config import get_modules_config
from db_utils import register_result_location, init_db, save_result
from module5_dispatcher import generate_subtasks
from module5_merge import reset_merge_state, submit_partial_trace

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

def get_ready_modules(execution_order, completed_modules, answer_map, modules_config):
    """獲取可以執行的模組（所有依賴都已滿足）"""
    ready_modules = []
    
    for module in execution_order:
        if module in completed_modules:
            continue
            
        # 檢查是否所有依賴都已滿足
        all_deps_ready = True
        for required_answer in modules_config[module].get("requires", []):
            if required_answer not in answer_map:
                all_deps_ready = False
                break
        
        if all_deps_ready:
            ready_modules.append(module)
    
    return ready_modules

def execute_module_task(module, inputs, user_inputs, worker_pool, idx):
    """執行單一模組任務的函數（用於平行執行）"""
    start_time = time.time()
    
    try:
        worker = get_available_worker(worker_pool, idx)
        exec_id = str(uuid.uuid4())
        
        task_packet = {
            "module_name": module,
            "input_data": inputs,
            "execution_id": exec_id,
            "user_inputs": user_inputs
        }

        print(f"📤 [並行] 發送 {module} 到 {worker}")
        send_response = send_task_to_worker(worker, task_packet)

        if send_response is None:
            raise Exception(f"無法傳送模組 {module}")

        print(f"⏳ [並行] 等待模組 {module} 執行結果...")
        result = receive_result(module)
        
        register_result_location(module, result, worker)
        
        duration = time.time() - start_time
        print(f"✅ [並行] 模組 {module} 完成，耗時 {format_duration(duration)}")
        
        return {
            "module": module,
            "result": result,
            "duration": duration,
            "success": True
        }
        
    except Exception as e:
        duration = time.time() - start_time
        print(f"❌ [並行] 模組 {module} 失敗，耗時 {format_duration(duration)}：{e}")
        return {
            "module": module,
            "result": None,
            "duration": duration,
            "success": False,
            "error": str(e)
        }

def execute_module5_distributed(inputs, user_inputs, worker_pool):
    """執行 module5 的分散式計算"""
    print(f"\n=== 🔥 執行分散式計算 module5 ===")
    start_time = time.time()
    
    # 1. 生成子任務
    print("📦 生成子任務...")
    subtasks = generate_subtasks(inputs)
    print(f"✅ 生成 {len(subtasks)} 個子任務")
    
    # 2. 重置合併狀態
    reset_merge_state(len(subtasks))
    
    # 3. 計算合理的並行度（不超過可用 worker 數量）
    available_workers = len(worker_pool)
    max_parallel = min(available_workers, len(subtasks))
    print(f"🔧 將使用 {max_parallel} 個 worker 進行並行計算")
    
    # 4. 使用線程池並行發送子任務
    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = []
        
        for sub_idx, subtask in enumerate(subtasks):
            worker = get_available_worker(worker_pool, sub_idx)
            exec_id = str(uuid.uuid4())
            
            task_packet = {
                "module_name": "module5_sub",
                "input_data": subtask,
                "execution_id": f"module5_sub_{exec_id}_{sub_idx}",
                "user_inputs": user_inputs
            }
            
            # 提交異步任務
            future = executor.submit(send_task_to_worker, worker, task_packet)
            futures.append((future, sub_idx, worker))
            print(f"📤 [並行] 提交子任務 {sub_idx+1}/{len(subtasks)} 至 {worker}")
        
        # 等待所有子任務發送完成
        print("⏳ 等待所有子任務發送完成...")
        for future, sub_idx, worker in futures:
            try:
                result = future.result(timeout=30)
                if result:
                    print(f"✅ 子任務 {sub_idx+1} 成功發送至 {worker}")
                else:
                    print(f"❌ 子任務 {sub_idx+1} 發送至 {worker} 失敗")
            except Exception as e:
                print(f"❌ 子任務 {sub_idx+1} 發送異常：{e}")
    
    # 5. 等待合併結果
    print("⏳ 等待 module5_merge 合併結果...")
    try:
        result = receive_result("module5_merge")
        duration = time.time() - start_time
        print(f"✅ module5 分散式計算完成，總耗時 {format_duration(duration)}")
        print(f"📊 結果：{result}")
        
        return {
            "module": "module5",
            "result": result,
            "duration": duration,
            "success": True,
            "subtasks_count": len(subtasks),
            "parallel_workers": max_parallel
        }
        
    except Exception as e:
        duration = time.time() - start_time
        print(f"❌ module5 分散式計算失敗，耗時 {format_duration(duration)}：{e}")
        return {
            "module": "module5",
            "result": None,
            "duration": duration,
            "success": False,
            "error": str(e)
        }

def main(user_inputs):
    total_start_time = time.time()
    timing_stats = {}
    
    print(f"\n🕐 開始時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n🔄 初始化資料庫...")
    init_db()

    print("\n🧩 載入模組與建構 DAG...")
    modules = get_modules_config(user_inputs)
    dag, execution_order = build_dag(modules)
    
    # 移除 module5 相關的模組，我們會特殊處理
    execution_order = [m for m in execution_order if m not in ["module5", "module5_dispatcher", "module5_merge"]]
    
    result_map = {}
    answer_map = {}
    completed_modules = set()
    
    print(f"\n🚀 模組執行順序為：{execution_order}")
    print("🔥 module5 將使用分散式計算")

    # 主執行循環 - 支援平行執行
    while len(completed_modules) < len(execution_order) + 1:  # +1 for module5
        
        # 1. 獲取可以執行的模組
        ready_modules = get_ready_modules(execution_order, completed_modules, answer_map, modules)
        
        # 2. 檢查是否可以執行 module5
        module5_ready = ("module5" not in completed_modules and 
                        "answer1" in answer_map and "answer4" in answer_map)
        
        if module5_ready:
            ready_modules.append("module5")
        
        if not ready_modules:
            if len(completed_modules) < len(execution_order) + 1:
                print("⚠️ 沒有可執行的模組，可能存在未解決的依賴")
                print(f"已完成：{completed_modules}")
                print(f"可用答案：{list(answer_map.keys())}")
                break
            else:
                break
        
        print(f"\n🚀 準備並行執行模組：{ready_modules}")
        
        # 3. 並行執行就緒的模組
        with ThreadPoolExecutor(max_workers=len(worker_pool)) as executor:
            futures = []
            
            for idx, module in enumerate(ready_modules):
                if module == "module5":
                    # 特殊處理 module5 分散式計算
                    inputs = {
                        "answer1": answer_map["answer1"],
                        "answer4": answer_map["answer4"]
                    }
                    future = executor.submit(execute_module5_distributed, inputs, user_inputs, worker_pool)
                    futures.append(future)
                else:
                    # 準備輸入資料
                    inputs = {}
                    if module == "module1":
                        inputs = user_inputs.copy()
                    else:
                        for required_answer in modules[module]["requires"]:
                            if required_answer in answer_map:
                                inputs[required_answer] = answer_map[required_answer]
                    
                    future = executor.submit(execute_module_task, module, inputs, user_inputs, worker_pool, idx)
                    futures.append(future)
            
            # 4. 收集執行結果
            for future in as_completed(futures):
                try:
                    result = future.result()
                    module = result["module"]
                    
                    if result["success"]:
                        result_map[module] = result["result"]
                        timing_stats[module] = result["duration"]
                        completed_modules.add(module)
                        
                        # ✅ 修正：正確解析答案到 answer_map
                        module_result = result["result"]
                        print(f"🔧 [DEBUG] 原始結果結構：{module_result}")
                        
                        if isinstance(module_result, dict):
                            # 情況1: {'module1': {'result': {'answer1': 198, ...}, 'timing': {...}}}
                            if module in module_result and isinstance(module_result[module], dict):
                                if 'result' in module_result[module]:
                                    answer_map.update(module_result[module]['result'])
                                    print(f"🔧 [DEBUG] 從 {module}.result 提取答案：{module_result[module]['result']}")
                                else:
                                    answer_map.update(module_result[module])
                                    print(f"🔧 [DEBUG] 從 {module} 直接提取答案：{module_result[module]}")
                            # 情況2: {'answer1': 198, 'answer2': 104, ...}
                            elif any(key.startswith('answer') for key in module_result.keys()):
                                answer_map.update(module_result)
                                print(f"🔧 [DEBUG] 直接提取答案：{module_result}")
                            # 情況3: {'result': {'answer1': 198, ...}, 'timing': {...}}
                            elif 'result' in module_result:
                                answer_map.update(module_result['result'])
                                print(f"🔧 [DEBUG] 從 result 欄位提取答案：{module_result['result']}")
                            else:
                                print(f"⚠️ [DEBUG] 無法解析結果結構：{module_result}")
                        
                        print(f"✅ 模組 {module} 已加入完成清單")
                        print(f"📊 更新後的 answer_map：{answer_map}")
                        
                        if module == "module5":
                            print(f"🔥 分散式計算統計：{result.get('subtasks_count')} 個子任務，{result.get('parallel_workers')} 個並行 worker")
                    else:
                        timing_stats[module] = result["duration"]
                        print(f"❌ 模組 {module} 執行失敗：{result.get('error')}")
                        
                except Exception as e:
                    print(f"❌ 執行任務時發生未預期錯誤：{e}")

    # 計算總執行時間並顯示報告
    total_end_time = time.time()
    total_duration = total_end_time - total_start_time

    print("\n" + "="*60)
    print("📊 執行時間統計報告")
    print("="*60)
    print(f"🕐 開始時間：{datetime.fromtimestamp(total_start_time).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🕐 結束時間：{datetime.fromtimestamp(total_end_time).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️ 總執行時間：{format_duration(total_duration)}")
    print(f"🔥 平行執行效率：{len(worker_pool)} 個 worker 可用")
    
    print("\n📋 各模組執行時間：")
    for module, duration in timing_stats.items():
        percentage = (duration / total_duration) * 100 if total_duration > 0 else 0
        print(f"  {module:<20}: {format_duration(duration):<10} ({percentage:.1f}%)")

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