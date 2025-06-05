好的，根據你提供的背景與想法，我先幫你整理出一份調整清單（條列式），分為架構面與程式邏輯面兩大類，方便你後續逐步實作與擴充：

⸻

✅ 系統調整清單（針對未來擴充多台電腦並平行計算）

🧩 一、架構層級調整
	1.	去中心化資料儲存策略
	•	原本：使用雲端資料庫（如 Supabase）儲存中繼結果，讓模組之間共享。
	•	新策略：改用各台電腦的本地 SQLite 儲存模組輸出結果。
	•	優點：節省資料庫成本，不依賴網路連線穩定性。
	2.	資料與指令一併傳遞
	•	當 master 派發某個模組的計算任務時：
	•	將所需 前置模組輸出資料 + 模組代碼/指令參數 一起打包傳送至目標 worker。
	•	worker 執行後將結果存在本機 SQLite。
	•	master 記錄該結果存放於哪台機器上（供後續模組使用）。
	3.	模組之間的資料傳遞靠 master 控制
	•	每次模組完成後，master 會根據 DAG 將下一個模組分派給適當的 worker。
	•	若下游模組不在同一台機器，則由 master 把資料打包再傳過去。
	4.	計算密集模組可支援子任務分派
	•	如模組5，可將其切割為多個子任務並分發至多個 worker。
	•	子任務執行完後回傳給 master 或由一台指定機器負責整合。

⸻

⚙️ 二、程式邏輯與實作調整
	1.	任務分派與追蹤系統
	•	master 需具備：
	•	根據 DAG 分析哪些模組可執行。
	•	記錄每個模組計算結果的位置（哪台機器的哪個檔案）。
	•	適時將任務與依賴資料派發至空閒 worker。
	2.	任務打包與傳送機制
	•	定義統一格式的 任務打包格式：

{
  "module_name": "module5",
  "input_data": { ... },
  "input_from_modules": ["module2", "module3"],
  "code_or_script": "module5.py",
  "execution_id": "uuid"
}


	•	可透過：SSH + SCP、Socket、HTTP API 等方式將任務傳送給其他電腦。

	3.	worker 執行器設計
	•	每台電腦執行一個 worker 監聽器，接收任務並執行。
	•	執行模組後將結果寫入本地 SQLite 並回傳執行結果狀態（或回傳資料）。
	4.	結果整合機制（如模組5）
	•	支援類似 MapReduce 的設計：
	•	map 階段：各 worker 執行子任務。
	•	reduce 階段：一台 designated node 或 master 收集整合結果。
	•	結果整合後再觸發後續模組（例如模組7）。
	5.	SQLite 檔案規劃
	•	每台 worker 存一個模組結果資料庫，如：

/local_db/module_results.db


	•	表結構需一致（模組名稱、輸入參數、輸出、timestamp、狀態碼等）。

	6.	DAG 調度器強化
	•	改為可支援分散式執行節點的調度邏輯：
	•	根據模組計算負載與資源情況進行任務分派。
	•	支援同一模組可被切成子任務分配多機處理。

⸻

🔄 最終目標
	•	使用一台 master 電腦控制模組任務的分派、資料流與依賴關係。
	•	各台 worker 電腦負責模組實際運算，並使用本機 SQLite 儲存結果。
	•	大型模組可切成子任務並平行分派、結果整合。
	•	不依賴雲端資料庫，節省成本。


⸻⸻⸻⸻

目標架構流程（從 master 控制並透過本地 SQLite 執行計算）：
	Master Node:
  1. 建立 DAG 任務佇列
  2. 負責初始輸入與任務分派
  3. 發送資料與指令給 worker
  4. 等待結果合併（例如模組5）

Worker Node:
  1. 接收任務與輸入資料
  2. 執行指定模組計算
  3. 將結果寫入本地 SQLite
  4. 回傳必要資料給下一模組或 master

專案架構新增/調整
parallel-computing-system/
│
├── master.py                 # ✅ 新增：Master 控制器
├── worker.py                 # ✅ 新增：Worker 計算節點程式
├── module_runner.py          # ✅ 新增：統一模組執行函式
├── transport_utils.py        # ✅ 新增：Socket/HTTP 傳輸工具
│
├── dag_utils.py              # ✏️ 更新：DAG 排程器（支援 master 分派）
├── modules_config.py         # ✏️ 更新：模組函式總表
├── db_utils.py               # ✏️ 更新：各地 SQLite 操作（讀/寫結果）
│
├── main_legacy.py            # （可選）原單機版主程式，備份保留
│
├── modules/
│   ├── module1.py            # ✅ 保留
│   ├── module2.py ...
│   ├── module5_dispatcher.py # ✅ 新增
│   ├── module5_merge.py      # ✅ 新增
│   └── ... 其他模組 ...



⸻⸻⸻⸻
Socket vs Flask API vs gRPC 傳輸方式比較

	•	初期開發（本機模擬多台） → 建議用 Flask API + JSON 傳輸（容易開發、測試、除錯）
	•	未來正式部署多機並追求效能 → 考慮升級 gRPC + Protobuf

	