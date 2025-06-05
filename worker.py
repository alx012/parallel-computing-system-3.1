# ============================================
# worker.py (修正版 - 解決未定義變數錯誤)
# ============================================
import sys
import json
import time
from flask import Flask, request, jsonify

# 只導入 worker 需要的模組
try:
    from module_runner import run_module
    from db_utils import save_result
    from module5_merge import submit_partial_trace
except ImportError as e:
    print(f"❌ 導入錯誤：{e}")
    print("請確保所有必要的模組檔案都存在")
    sys.exit(1)

def create_worker_app(worker_id):
    """創建 Worker Flask 應用程式"""
    app = Flask(f"worker_{worker_id}")
    
    @app.route('/compute', methods=['POST'])
    def compute_task():
        """處理來自 master 的計算任務"""
        try:
            # 解析任務
            task_data = request.get_json()
            if not task_data:
                return jsonify({"error": "No task data provided"}), 400
            
            module_name = task_data.get("module_name")
            input_data = task_data.get("input_data", {})
            execution_id = task_data.get("execution_id")
            user_inputs = task_data.get("user_inputs", {})
            
            print(f"\n🔧 [Worker {worker_id}] 收到任務：{module_name}")
            print(f"📥 [Worker {worker_id}] 執行ID：{execution_id}")
            print(f"📋 [Worker {worker_id}] 輸入資料：{input_data}")
            
            start_time = time.time()
            
            # 特殊處理 module5 子任務
            if module_name == "module5_sub":
                result = handle_module5_subtask(input_data, worker_id)
            else:
                # 一般模組執行
                result = run_module(module_name, input_data, user_inputs)
            
            duration = time.time() - start_time
            
            # 儲存結果到資料庫
            save_result(module_name, result)
            
            print(f"✅ [Worker {worker_id}] 任務 {module_name} 完成，耗時 {duration:.2f}s")
            
            return jsonify({
                "status": "success",
                "module_name": module_name,
                "execution_id": execution_id,
                "duration": duration,
                "worker_id": worker_id,
                "result": result
            })
            
        except Exception as e:
            print(f"❌ [Worker {worker_id}] 任務執行失敗：{e}")
            import traceback
            traceback.print_exc()
            
            return jsonify({
                "status": "error",
                "error": str(e),
                "worker_id": worker_id
            }), 500
    
    @app.route('/health', methods=['GET'])
    def health_check():
        """健康檢查端點"""
        return jsonify({
            "status": "healthy",
            "worker_id": worker_id,
            "timestamp": time.time()
        })
    
    @app.route('/status', methods=['GET'])
    def worker_status():
        """Worker 狀態查詢"""
        return jsonify({
            "worker_id": worker_id,
            "status": "running",
            "timestamp": time.time()
        })
    
    return app

def handle_module5_subtask(subtask_data, worker_id):
    """處理 module5 的子任務計算"""
    print(f"🔥 [Worker {worker_id}] 開始處理 module5 子任務")
    
    try:
        block_row = subtask_data["block_row"]
        block_col = subtask_data["block_col"]
        chunk_size = subtask_data["chunk_size"]
        base_size = subtask_data["base_size"]
        answer1 = subtask_data["answer1"]
        answer4 = subtask_data["answer4"]
        
        # 執行矩陣塊計算（簡化版本）
        start_row = block_row * chunk_size
        end_row = min(start_row + chunk_size, base_size)
        start_col = block_col * chunk_size
        end_col = min(start_col + chunk_size, base_size)
        
        print(f"📊 [Worker {worker_id}] 計算矩陣塊 [{start_row}:{end_row}, {start_col}:{end_col}]")
        
        # 模擬複雜計算
        partial_sum = 0
        for i in range(start_row, end_row):
            for j in range(start_col, end_col):
                # 簡化的計算邏輯
                partial_sum += (i * answer1 + j * answer4) % 1000
        
        # 模擬計算時間
        time.sleep(0.1)
        
        trace_value = partial_sum % 10000
        
        print(f"🧮 [Worker {worker_id}] 子任務計算完成，trace_value: {trace_value}")
        
        # 提交部分結果到合併器
        try:
            submit_partial_trace(trace_value)
        except Exception as e:
            print(f"⚠️ [Worker {worker_id}] 提交 trace_value 失敗：{e}")
        
        return {
            "trace_value": trace_value,
            "block_position": f"[{block_row},{block_col}]",
            "worker_id": worker_id,
            "computation_size": (end_row - start_row) * (end_col - start_col)
        }
        
    except Exception as e:
        print(f"❌ [Worker {worker_id}] module5 子任務失敗：{e}")
        raise

def main():
    """Worker 主程式"""
    if len(sys.argv) != 2:
        print("使用方式: python worker.py <worker_id>")
        print("範例: python worker.py 1")
        sys.exit(1)
    
    try:
        worker_id = sys.argv[1]
        port = 5000 + int(worker_id)
    except ValueError:
        print("❌ Worker ID 必須是數字")
        sys.exit(1)
    
    print(f"🚀 啟動 Worker {worker_id} 在 port {port}")
    
    app = create_worker_app(worker_id)
    
    try:
        app.run(
            host='0.0.0.0',
            port=port,
            debug=False,
            threaded=True
        )
    except KeyboardInterrupt:
        print(f"\n🛑 Worker {worker_id} 正在關閉...")
    except Exception as e:
        print(f"❌ Worker {worker_id} 啟動失敗：{e}")

if __name__ == "__main__":
    main()