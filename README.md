#每小時空品資歷自動上傳

用排程(cron)自動執行(可自由設定重複執行時間)
get.py 負責抓現在時間的前一個小時的整點空品資料，並把時間轉換為UTC
post.py 負責上傳資料
logs 有執行的紀錄

#樹狀圖
[程式啟動]
   |
   v
[GET 前一小時空品資料]
   |
   |-- (無資料)
   |     |
   |     +--> [ERR] 空品站未上傳資料
   |     +--> exit code = 1
   |
   v
[取得 1 筆空品資料]
   |
   v
[資料驗證 / 清洗]
   |
   |-- (缺欄位 / 空白字串)
   |     |
   |     +--> [ERR] 資料格式錯誤
   |     +--> exit code = 1
   |
   v
[登入 API（取得 Cookie）]
   |
   |-- (登入失敗)
   |     |
   |     +--> [ERR] Login failed
   |     +--> exit code = 1
   |
   v
[查詢資料庫最新 24 筆（/api/AirQuality/list）]
   |
   v
[比對 detectedAtUtc 是否已存在]
   |
   |-- (已存在)
   |     |
   |     +--> [SKIP] Duplicate detectedAtUtc
   |     +--> exit code = 0
   |
   v
[POST /api/AirQuality 上傳資料]
   |
   |-- (401 / 403)
   |     |
   |     +--> 重新登入
   |     +--> 再次查詢是否已存在
   |           |
   |           |-- (已存在)
   |           |     +--> [SKIP]
   |           |     +--> exit code = 0
   |           |
   |           |-- (仍不存在)
   |                 +--> 再上傳一次
   |
   |-- (上傳失敗)
   |     |
   |     +--> [ERR] POST failed
   |     +--> exit code = 1
   |
   v
[上傳成功]
   |
   +--> [OK] Upload success
   +--> exit code = 0

#get.py
資料由https://tortoise-fluent-rationally.ngrok-free.app/api/60min/json/YYYYMM
1.抓取現在時間的前一個整點資料(因為資料最快跟新速度就是這樣)
2.將日期時間轉換為UTC(2026-01-06T21:00:00.000Z)
3.抓取一筆資料
4.若抓不到資料則視為空品站異常(可能為停電等等)

#post.py
1.確認是否有必要欄位(缺少則error)
2.可以是null 但不能為空白(空白會報錯)
3.把時間統一格式 用來比對是否重複
4.登入拿cookie 若失效(401/403)則再登一次
5.查詢最新24筆資料看DetectedAtUtc是否重複(重複則跳過)

#log
[INFO] :　資訊
[OK]   :　成功上傳
[SKIP] :　重複時間，跳過
[ERR]  :　錯誤

＃執行（ＷＳＬ）
METEO_ACCOUNT="your_account" METEO_PASSWORD="your_password" python post.py
排程執行(corntab)

#功能總結
1.抓取資料(每小時)
2.轉換成UTC格式
3.上傳資料(避免數值空白、沒抓到資料、重複上傳資料)
4.可以成功部屬在電腦裡面自動執行

目前還沒有
1.刪除數值部正常的資料
2.停電的檢查(可能多加像是看最近的24筆時間是否有空缺，再用程式自動補齊或是標註?)
3.刪除目前測試的資料(似乎帳號的權限不夠)
