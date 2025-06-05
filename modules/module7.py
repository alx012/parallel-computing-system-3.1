def module7_function(inputs):
    """
    模組7：使用answer7和answer8進行最終計算
    
    Args:
        inputs: 包含answer7和answer8的字典
        
    Returns:
        包含final_result的字典
    """
    print("\n===== 模組7：最終計算 =====")
    answer7 = inputs["answer7"]
    answer8 = inputs["answer8"]
    
    # 計算: answer7 乘以 answer8
    final_result = answer7 * answer8
    
    print(f"模組7計算結果：final_result={final_result}")
    
    return {
        "final_result": final_result
    }