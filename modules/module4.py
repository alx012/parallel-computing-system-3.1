from dask import delayed, compute

def module4_function(inputs):
    """
    模組4：使用 answer2 和 answer3 進行模擬最佳化計算（支援 Dask 分段平行）
    
    Args:
        inputs: 包含 answer2 和 answer3 的字典
        
    Returns:
        包含 answer6 的字典
    """
    print("\n===== 模組4：模擬複雜計算中（使用 Dask 平行化） =====")
    answer2 = inputs["answer2"]
    answer3 = inputs["answer3"]

    if answer3 == 0:
        print("警告：除數為零，使用預設值 1 以繼續模擬")
        answer3 = 1

    a = answer2
    b = answer3

    def loss(x):
        return (a * x**2 + b * x + 10)**2 + x * 0.5

    # --- 定義暴力搜尋任務 ---
    def search_range(start_i, end_i):
        best_x = None
        best_loss = float("inf")
        for i in range(start_i, end_i):
            x = i / 1000.0
            current_loss = loss(x)
            if current_loss < best_loss:
                best_loss = current_loss
                best_x = x
        return best_x, best_loss

    # --- 分段：把整個搜尋範圍切成多塊 ---
    total_start = -50000
    total_end = 50000
    step = 5000  # 每個段落的 index 長度（5000 對應 x 會跑 5.0 範圍）

    delayed_tasks = []
    for start_i in range(total_start, total_end, step):
        end_i = min(start_i + step, total_end)
        task = delayed(search_range)(start_i, end_i)
        delayed_tasks.append(task)

    # --- 執行所有分段任務並聚合結果 ---
    results = compute(*delayed_tasks)  # results = [(x1, loss1), (x2, loss2), ...]
    best_x, best_loss = min(results, key=lambda pair: pair[1])

    print(f"模組4計算完成：answer6 = 最小值位置 x ≈ {best_x:.4f}，對應損失 ≈ {best_loss:.4f}")
    return {"answer6": best_x}