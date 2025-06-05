# module5_merge.py
import threading

# 儲存收到的子結果
partial_results = []
expected_total = 0
received_count = 0
lock = threading.Lock()

# Master 會等待這個結果，透過 transport_utils.receive_result("module5_merge") 模擬
from transport_utils import store_result_from_worker

def submit_partial_trace(trace_value):
    global received_count
    with lock:
        partial_results.append(trace_value)
        received_count += 1

        # 若所有結果已到齊就可觸發合併
        if received_count == expected_total:
            final_result = sum(partial_results)
            store_result_from_worker("module5_merge", {"answer7": final_result})

def reset_merge_state(expected):
    global partial_results, expected_total, received_count
    with lock:
        partial_results = []
        expected_total = expected
        received_count = 0