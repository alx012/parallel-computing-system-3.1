# ============================================
# module_runner.py (完全修正版 - 移除依賴處理 + 加入時間統計)
# ============================================
import time
from datetime import datetime
from modules_config import get_modules_config

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

def run_module(module_name, inputs, user_inputs=None):
    """
    執行指定模組，直接使用 master 傳來的 inputs
    不再在 worker 端處理依賴關係，因為 master 已經準備好了
    """
    # ✅ 開始計時
    start_time = time.time()
    start_datetime = datetime.now()
    
    user_inputs = user_inputs or {}
    
    print(f"🔧 [module_runner] 開始執行模組 {module_name}")
    print(f"🕐 [module_runner] 開始時間: {start_datetime.strftime('%H:%M:%S.%f')[:-3]}")
    print(f"📥 [module_runner] 輸入資料: {inputs}")
    print(f"👤 [module_runner] 用戶輸入: {user_inputs}")
    
    try:
        # 載入模組配置
        print("📋 [module_runner] 載入模組配置...")
        config_start = time.time()
        modules = get_modules_config(user_inputs)
        config_duration = time.time() - config_start
        print(f"📋 [module_runner] 配置載入耗時: {format_duration(config_duration)}")
        
        if module_name not in modules:
            raise ValueError(f"❌ 找不到模組：{module_name}")

        module = modules[module_name]
        print(f"✅ [module_runner] 模組 {module_name} 配置載入成功")
        
        # 直接執行模組，不處理依賴（master 已經處理了）
        print(f"🚀 [module_runner] 執行模組函數...")
        execution_start = time.time()
        result = module["generator"](inputs)
        execution_duration = time.time() - execution_start
        
        # ✅ 計算總執行時間
        total_duration = time.time() - start_time
        end_datetime = datetime.now()
        
        print(f"✅ [module_runner] 模組 {module_name} 執行完成")
        print(f"🕐 [module_runner] 結束時間: {end_datetime.strftime('%H:%M:%S.%f')[:-3]}")
        print(f"⏱️ [module_runner] 純計算時間: {format_duration(execution_duration)}")
        print(f"⏱️ [module_runner] 總執行時間: {format_duration(total_duration)}")
        print(f"📤 [module_runner] 輸出結果: {result}")
        
        # ✅ 在結果中加入時間統計資訊，但保持答案格式簡潔
        # 如果 result 已經包含答案，直接回傳答案部分
        if isinstance(result, dict) and any(key.startswith('answer') for key in result.keys()):
            # 直接的答案格式：{'answer1': 198, 'answer2': 104, ...}
            enhanced_result = result
        else:
            # 複雜格式或其他格式，保持原樣
            enhanced_result = {
                "result": result,
                "timing": {
                    "start_time": start_datetime.isoformat(),
                    "end_time": end_datetime.isoformat(),
                    "config_duration": config_duration,
                    "execution_duration": execution_duration,
                    "total_duration": total_duration,
                    "module_name": module_name
                }
            }
        
        return enhanced_result
        
    except Exception as e:
        # ✅ 記錄失敗時間
        error_duration = time.time() - start_time
        end_datetime = datetime.now()
        
        print(f"❌ [module_runner] 模組 {module_name} 執行失敗：{e}")
        print(f"🕐 [module_runner] 失敗時間: {end_datetime.strftime('%H:%M:%S.%f')[:-3]}")
        print(f"⏱️ [module_runner] 失敗前執行時間: {format_duration(error_duration)}")
        
        import traceback
        traceback.print_exc()
        
        # ✅ 回傳錯誤資訊包含時間統計
        error_result = {
            "error": str(e),
            "timing": {
                "start_time": start_datetime.isoformat(),
                "end_time": end_datetime.isoformat(),
                "error_duration": error_duration,
                "module_name": module_name
            }
        }
        
        raise Exception(f"Module {module_name} failed: {e}") from e