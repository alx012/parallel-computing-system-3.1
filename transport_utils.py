# ============================================
# transport_utils.py (修正版：從資料庫接收結果)
# ============================================

import requests
import time
import json
from typing import Dict, Any
from db_utils import fetch_answers  # ✅ 匯入資料庫查詢函數

def send_task_to_worker(worker_url: str, task_packet: Dict[str, Any]):
    """發送任務到指定 worker"""
    try:
        response = requests.post(
            f"{worker_url}/compute",
            json=task_packet,
            headers={'Content-Type': 'application/json'},
            timeout=30
        )

        if response.status_code == 200:
            print(f"✅ 任務成功發送至 {worker_url}")
            return response.json()
        else:
            print(f"❌ 傳送任務至 {worker_url} 失敗：{response.status_code} {response.reason}")
            return None

    except Exception as e:
        print(f"❌ 傳送任務至 {worker_url} 失敗：{e}")
        return None

def receive_result(module_name: str, timeout: int = 60):
    """等待並從 SQLite 資料庫接收模組結果"""
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            result = fetch_answers([module_name])
            if module_name in result:
                print(f"✅ 收到模組 {module_name} 的結果：{result}")
                return result
        except Exception as e:
            print(f"⚠️ 無法取得 {module_name} 結果，錯誤：{e}")

        time.sleep(0.2)

    raise TimeoutError(f"等待模組 {module_name} 結果超時")

def store_result_from_worker(module_name: str, result: Any):
    """此功能保留，但現已透過資料庫處理結果，不再使用"""
    print(f"📥 模組 {module_name} 的結果已由 worker 儲存至資料庫，略過本地記憶體儲存")

def get_available_worker(worker_pool: Dict[str, str], index: int = None):
    """取得可用的 worker URL"""
    if index is not None:
        worker_keys = list(worker_pool.keys())
        worker_key = worker_keys[index % len(worker_keys)]
        return worker_pool[worker_key]
    else:
        # 預設回傳第一個 worker
        return list(worker_pool.values())[0]

# 以下兩個函數目前架構未使用，保留以供擴充
def listen_for_task():
    pass

def send_result_to_master(module_name: str, result: Any):
    pass