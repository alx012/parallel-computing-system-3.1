# ============================================
# db_utils.py
# ============================================
import sqlite3
import json
import time

DB_FILE = 'dag_result.db'

def init_db():
    """初始化 SQLite 資料庫與資料表"""
    print("\n🔄 初始化資料庫 ...")
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS module_result (
                module_id TEXT PRIMARY KEY,
                result_json TEXT
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS result_index (
                module TEXT PRIMARY KEY,
                location TEXT,
                result_json TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # 清空先前執行的紀錄
        c.execute('DELETE FROM module_result')
        c.execute('DELETE FROM result_index')
        conn.commit()

def save_result(module_id, result_dict):
    """儲存單一模組結果進 DB"""
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        result_json = json.dumps(result_dict, ensure_ascii=False, default=str)
        module_id_str = str(module_id)
        c.execute('''
            INSERT OR REPLACE INTO module_result (module_id, result_json)
            VALUES (?, ?)
        ''', (module_id_str, result_json))
        conn.commit()

def register_result_location(module_name, result, worker_url):
    """紀錄模組的計算結果與所在 Worker"""
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        result_json = json.dumps(result, ensure_ascii=False, default=str)
        module_name = str(module_name)
        c.execute('''
            INSERT OR REPLACE INTO result_index (module, location, result_json)
            VALUES (?, ?, ?)
        ''', (module_name, worker_url, result_json))
        conn.commit()

def fetch_answers(required_modules):
    """
    從資料庫擷取一個或多個模組的結果。
    回傳格式為：{module_name: {...result_dict...}, ...}
    """
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        collected = {}

        while True:
            c.execute('SELECT module_id, result_json FROM module_result')
            rows = c.fetchall()
            for module_id, result_json in rows:
                if module_id not in collected:
                    result_dict = json.loads(result_json)
                    collected[module_id] = result_dict

            if all(module in collected for module in required_modules):
                return {m: collected[m] for m in required_modules}

            time.sleep(0.1)

def get_all_results():
    """取回全部模組的結果資料"""
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute('SELECT module_id, result_json FROM module_result ORDER BY module_id')
        return [(module_id, json.loads(result_json)) for module_id, result_json in c.fetchall()]

def get_final_result():
    """取回最終結果（module7）"""
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute('SELECT result_json FROM module_result WHERE module_id = ?', ("module7",))
        result = c.fetchone()
        return json.loads(result[0]) if result else None