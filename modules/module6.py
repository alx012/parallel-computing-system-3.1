def module6_function(inputs):
    """
    模組6：使用answer2, answer3和answer6進行計算
    
    Args:
        inputs: 包含answer2, answer3和answer6的字典
        
    Returns:
        包含answer8的字典
    """
    print("\n===== 模組6：計算 =====")
    answer2 = inputs["answer2"]
    answer3 = inputs["answer3"]
    answer6 = inputs["answer6"]
    
    # 計算: answer2 + answer3 - answer6
    answer8 = answer2 + answer3 - answer6
    
    print(f"模組6計算結果：answer8={answer8}")
    
    return {
        "answer8": answer8
    }