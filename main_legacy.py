import time
from db_utils import init_db, get_all_results, get_final_result
from dag_utils import draw_dag, execute_modules
from modules_config import get_modules_config, user_inputs

def print_module_descriptions():
    """輸出模組說明"""
    print("=" * 50)
    print("DAG 模組計算系統")
    print("=" * 50)
    print("本程式會根據有向無環圖(DAG)的依賴關係，依序執行各個計算模組。")
    print("模組1需要您提供基本數值，隨後系統會自動進行後續計算。")
    print("各模組的計算邏輯如下：")
    print("  - 模組1：計算 num1*2, num2+5, num3/2 得到 answer1, answer2, answer3")
    print("  - 模組2：計算 answer1^2 得到 answer4")
    print("  - 模組3：計算 (answer1 + answer2) * 2 得到 answer5")
    print("  - 模組4：計算 answer2 / answer3 得到 answer6")
    print("  - 模組5：計算 answer1 + sqrt(answer4) 得到 answer7")
    print("  - 模組6：計算 answer2 + answer3 - answer6 得到 answer8")
    print("  - 模組7：計算 answer7 * answer8 得到 final_result")
    print("=" * 50)

def get_user_inputs():
    """獲取用戶輸入"""
    print("\n===== 請輸入模組1所需的基本數值 =====")
    user_inputs["num1"] = float(input("請輸入第一個數字 (num1): "))
    user_inputs["num2"] = float(input("請輸入第二個數字 (num2): "))
    user_inputs["num3"] = float(input("請輸入第三個數字 (num3): "))

def main():
    """主程式"""
    # 初始化資料庫
    init_db()
    
    # 顯示說明
    print_module_descriptions()
    
    # 獲取模組配置
    modules = get_modules_config(user_inputs)
    
    # 顯示模組依賴圖
    print("\n首先顯示模組依賴關係圖：")
    draw_dag(modules)
    
    # 獲取用戶輸入
    get_user_inputs()
    
    print("\n開始執行模組計算...")
    
    # 執行模組並計時
    start = time.time()
    execute_modules(modules)
    
    # 顯示所有結果
    print("\n=== 所有模組結果 ===")
    for module_id, result in get_all_results():
        print(f"模組 {module_id}：{result}")
    
    # 顯示最終結果
    final_result = get_final_result()
    if final_result:
        print("\n===== 最終計算結果 =====")
        print(f"最終結果：{final_result.get('final_result', '計算失敗')}")
    
    print(f"\n總耗時：{time.time() - start:.2f} 秒")

if __name__ == "__main__":
    main()