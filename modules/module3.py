def module3_function(inputs):
    """
    模組3：使用answer1和answer2進行計算
    
    Args:
        inputs: 包含answer1和answer2的字典
        
    Returns:
        包含answer5的字典
    """
    print("\n===== 模組3：計算 =====")
    answer1 = inputs["answer1"]
    answer2 = inputs["answer2"]
    
    # 計算: answer1 + answer2 乘以 2
    answer5 = (answer1 + answer2) * 2
    
    print(f"模組3計算結果：answer5={answer5}")
    
    return {
        "answer5": answer5
    }