def module1_function(inputs, user_inputs):
    """
    模組1：使用用戶輸入進行初始計算
    
    Args:
        inputs: 空字典 (不需要輸入)
        user_inputs: 包含num1, num2, num3的字典
        
    Returns:
        包含answer1, answer2, answer3的字典
    """
    print("\n===== 模組1：使用基本數值進行計算 =====")
    
    # 使用用戶輸入
    num1 = user_inputs["num1"]
    num2 = user_inputs["num2"]
    num3 = user_inputs["num3"]
    
    # 進行簡單計算
    answer1 = num1 * 2
    answer2 = num2 + 5
    answer3 = num3 / 2
    
    print(f"模組1計算結果：answer1={answer1}, answer2={answer2}, answer3={answer3}")
    
    return {
        "answer1": answer1,
        "answer2": answer2,
        "answer3": answer3
    }