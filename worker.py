# ============================================
# worker_simple.py (簡化版用於測試)
# ============================================
import sys
import json
import time
from flask import Flask, request, jsonify

def create_simple_worker(worker_id):
    """創建簡化的 Worker 用於測試"""
    app = Flask(f"worker_{worker_id}")
    
    @app.route('/compute', methods=['POST'])
    def compute_task():
        """處理計算任務"""
        try:
            task_data = request.get_json()
            module_name = task_data.get("module_name")
            input_data = task_data.get("input_data", {})
            execution_id = task_data.get("execution_id")
            user_inputs = task_data.get("user_inputs", {})
            
            print(f"\n🔧 [Worker {worker_id}] 收到任務：{module_name}")
            print(f"📥 [Worker {worker_id}] 輸入：{input_data}")
            
            # 模擬不同模組的計算
            if module_name == "module1":
                result = {
                    "answer1": user_inputs["num1"] + user_inputs["num2"],  # 99 + 99 = 198
                    "answer2": user_inputs["num1"] + 5,                    # 99 + 5 = 104  
                    "answer3": user_inputs["num3"] / 2                     # 99 / 2 = 49.5
                }
            elif module_name == "module2":
                result = {
                    "answer4": input_data["answer1"] * 2  # 198 * 2 = 396
                }
            elif module_name == "module3":
                result = {
                    "answer5": input_data["answer1"] + input_data["answer2"]  # 198 + 104 = 302
                }
            elif module_name == "module4":
                result = {
                    "answer6": input_data["answer2"] * input_data["answer3"]  # 104 * 49.5 = 5148
                }
            elif module_name == "module5":
                result = {
                    "answer7": input_data["answer1"] + input_data["answer4"]  # 198 + 396 = 594
                }
            elif module_name == "module6":
                result = {
                    "answer8": input_data["answer2"] + input_data["answer3"] + input_data["answer6"]  # 104 + 49.5 + 5148 = 5301.5
                }
            elif module_name == "module5_sub":
                # ✅ 新增：處理 module5 子任務
                result = handle_module5_subtask(input_data, worker_id)
                print(f"🔥 [Worker {worker_id}] module5_sub 完成：{result}")
            elif module_name == "module7":
                result = {
                    "final_result": input_data["answer7"] + input_data["answer8"]  # 594 + 5301.5 = 5895.5
                }
            else:
                result = {"error": f"Unknown module: {module_name}"}
            
            # 儲存到簡化的資料庫（檔案）
            save_simple_result(module_name, result)
            
            print(f"✅ [Worker {worker_id}] {module_name} 完成：{result}")
            
            return jsonify({
                "status": "success",
                "module_name": module_name,
                "result": result,
                "worker_id": worker_id
            })
            
        except Exception as e:
            print(f"❌ [Worker {worker_id}] 錯誤：{e}")
            import traceback
            traceback.print_exc()
            
            return jsonify({
                "status": "error", 
                "error": str(e),
                "worker_id": worker_id
            }), 500
    
    @app.route('/health', methods=['GET'])
    def health():
        return jsonify({"status": "healthy", "worker_id": worker_id})
    
    return app

def handle_module5_subtask(subtask_data, worker_id):
    """處理 module5 的子任務計算（修改為線性切分）"""
    print(f"🔥 [Worker {worker_id}] 開始處理 module5 子任務")
    
    try:
        chunk_id = subtask_data["chunk_id"]
        start_index = subtask_data["start_index"]
        end_index = subtask_data["end_index"]
        answer1 = subtask_data["answer1"]
        answer4 = subtask_data["answer4"]
        
        print(f"📊 [Worker {worker_id}] 計算範圍 [{start_index}:{end_index}] (chunk {chunk_id})")
        
        # 模擬複雜計算（線性而非矩陣）
        partial_sum = 0
        for i in range(start_index, end_index):
            # 簡化的計算邏輯
            partial_sum += (i * answer1 + i * answer4) % 1000
        
        # 模擬計算時間
        import time
        time.sleep(0.2)  # 稍微增加計算時間讓效果更明顯
        
        trace_value = partial_sum % 10000
        
        print(f"🧮 [Worker {worker_id}] 子任務 {chunk_id} 計算完成，trace_value: {trace_value}")
        
        # 提交部分結果到合併器
        try:
            submit_partial_trace(trace_value)
            print(f"✅ [Worker {worker_id}] trace_value {trace_value} 已提交到合併器")
        except Exception as e:
            print(f"⚠️ [Worker {worker_id}] 提交 trace_value 失敗：{e}")
        
        return {
            "trace_value": trace_value,
            "chunk_id": chunk_id,
            "worker_id": worker_id,
            "computation_size": end_index - start_index
        }
        
    except Exception as e:
        print(f"❌ [Worker {worker_id}] module5 子任務失敗：{e}")
        raise

def submit_partial_trace(trace_value):
    """簡化版的部分結果提交（修改為 5 個子任務）"""
    try:
        # 這裡應該調用真正的 module5_merge.submit_partial_trace
        # 但為了簡化，我們直接存入檔案
        import json
        import os
        
        trace_file = "module5_traces.json"
        traces = []
        
        # 讀取現有的 traces
        if os.path.exists(trace_file):
            with open(trace_file, 'r') as f:
                traces = json.load(f)
        
        # 加入新的 trace
        traces.append(trace_value)
        
        # 寫回檔案
        with open(trace_file, 'w') as f:
            json.dump(traces, f)
        
        print(f"📝 Trace value {trace_value} 已儲存，目前總計 {len(traces)} 個")
        
        # ✅ 修改：如果收集到 5 個 traces，觸發合併
        if len(traces) >= 5:
            final_result = sum(traces)
            print(f"🎉 收集完成！最終結果：{final_result}")
            
            # 儲存最終結果
            save_simple_result("module5_merge", {"answer7": final_result})
            
            # 清空 traces 檔案
            os.remove(trace_file)
            
    except Exception as e:
        print(f"❌ 提交 trace_value 失敗：{e}")

def save_simple_result(module_name, result):
    """簡化的結果儲存"""
    try:
        import sqlite3
        with sqlite3.connect('dag_result.db') as conn:
            c = conn.cursor()
            result_json = json.dumps({module_name: result})
            c.execute('''
                INSERT OR REPLACE INTO module_result (module_id, result_json)
                VALUES (?, ?)
            ''', (module_name, result_json))
            conn.commit()
    except Exception as e:
        print(f"⚠️ 儲存結果失敗：{e}")

def main():
    if len(sys.argv) != 2:
        print("使用: python worker_simple.py <worker_id>")
        sys.exit(1)
    
    worker_id = sys.argv[1]
    port = 5000 + int(worker_id)
    
    print(f"🚀 啟動簡化 Worker {worker_id} on port {port}")
    
    app = create_simple_worker(worker_id)
    app.run(host='0.0.0.0', port=port, debug=True)

if __name__ == "__main__":
    main()