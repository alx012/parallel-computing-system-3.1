import numpy as np
import dask.array as da
from dask import delayed, compute

def module5_function(inputs):
    """
    模組5：模擬大型矩陣運算，使用 answer1 和 answer4 影響矩陣結構（Dask平行計算）

    Args:
        inputs: 包含 answer1 和 answer4 的字典

    Returns:
        包含 answer7 的字典
    """
    print("\n===== 模組5：執行大型矩陣運算中（使用 Dask 平行化） =====")
    answer1 = inputs["answer1"]
    answer4 = inputs["answer4"]

    # 處理可能的負值
    if answer4 < 0:
        print("警告：answer4 為負，使用其絕對值進行矩陣初始化")
        answer4 = abs(answer4)

    # 決定矩陣大小（受 answer1 和 answer4 影響）
    base_size = 15000 + int((answer1 + answer4) % 50)  # 大約在 100~150 之間
    print(f"模擬矩陣大小為 {base_size} x {base_size}，開始乘法計算...")

    # 使用 Dask 創建矩陣 A 和 B
    A = da.random.random((base_size, base_size), chunks=(1000, 1000))  # chunks 用來定義矩陣的分塊方式
    B = da.random.random((base_size, base_size), chunks=(1000, 1000))

    # 使用 Dask 進行矩陣乘法
    C = da.dot(A, B)

    # 計算主對角線的總和（trace）
    trace = da.trace(C)

    # 計算並取回結果
    trace_result = trace.compute()

    print(f"模組5計算完成：answer7 = 矩陣乘積主對角線總和 ≈ {trace_result:.4f}")
    
    return {"answer7": trace_result}