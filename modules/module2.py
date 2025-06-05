def module2_function(inputs):
    """
    模組2：使用answer1進行計算
    
    Args:
        inputs: 包含answer1的字典
        
    Returns:
        包含answer4的字典
    """
    print("\n===== 模組2：計算 =====")
    answer1 = inputs["answer1"]
    
    # 計算: answer1 的平方
    answer4 = answer1 ** 2
    
    print(f"模組2計算結果：answer4={answer4}")
    
    return {
        "answer4": answer4
    }