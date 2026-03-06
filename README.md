# RPA Batch Runner (Docker Compose)

此專案提供「家 / 消 / 破」批次查詢腳本，支援：

- 讀取 `list.csv`
- 下載 PDF 到 `pdfs/`
- 結果持續寫入 SQLite（可中斷續跑）
- 自動重跑直到全部成功或手動停止

## 1) 先準備資料

- 放好 `list.csv`（欄位需有 `Name`, `NationalId`）

## 2) Build 映像

```bash
docker compose build runner
```

## 3) 啟動批次

使用預設參數（讀 `/app/list.csv`、寫 `/app/results_household_debt_bankrupt.sqlite`、PDF 到 `/app/pdfs`）：

```bash
docker compose run --rm runner
```

自訂參數（例：並發 5、retry 2）：

```bash
docker compose run --rm runner --concurrency 5 --retry 2
```

指定本回合處理筆數：

```bash
docker compose run --rm runner --limit 100
```

## 4) 停止方式

- 第一次 `Ctrl+C`：停止接新工作並收尾退出
- 第二次 `Ctrl+C`：強制停止

## 5) 續跑

再次執行同一指令即可續跑，腳本會依 SQLite + PDF 檔案狀態判斷已完成資料並跳過。

## 6) compose 服務說明

- `runner`: Python 批次腳本容器
- `selenium`: 預設 `selenium/standalone-chromium`（可跨 `amd64/arm64`），由 runner 透過 `SELENIUM_REMOTE_URL` 連線

`docker-compose.yml` 已將專案根目錄掛進 `/app`，因此 `list.csv`、SQLite、`pdfs/` 都會落在 host 端專案目錄。

若你要自訂 Selenium image，可用環境變數覆寫：

```bash
SELENIUM_IMAGE=selenium/standalone-chrome:latest docker compose up -d selenium
```
