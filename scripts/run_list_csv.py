#!/usr/bin/env python3
"""
家消破批次腳本 (SQLite 版，直接呼叫 helper)

功能:
- 從 CSV 讀取 Name / NationalId
- 每筆查詢家 / 消 / 破三類資料
- 執行過程持續寫入 SQLite，支援隨時中斷
- 下載三類 PDF，檔案存在且非空才算成功
- 可指定 concurrency 與本次處理筆數
- 可續跑 (已完成者自動跳過)
- 可持續回合重跑，直到全部成功或手動停止
"""
import argparse
import asyncio
import csv
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any, Callable

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from helpers import DomesticJudV2Helper, MoneyCheckHelper, RPAQueryStatus

DEFAULT_INPUT = "list.csv"
DEFAULT_DB = "results_household_debt_bankrupt.sqlite"
QUERY_NAMES = ("domestic", "debt", "bankrupt")
STOP_EVENT = threading.Event()
SIGNAL_COUNT = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="家消破 CSV 批次查詢")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="CSV 路徑 (預設: list.csv)")
    parser.add_argument("--db-path", default=DEFAULT_DB, help="SQLite 結果檔路徑")
    parser.add_argument("--pdf-dir", default="pdfs", help="PDF 下載目錄")
    parser.add_argument("--concurrency", "-c", type=int, default=3, help="並發數")
    parser.add_argument(
        "--limit",
        "-n",
        type=int,
        default=None,
        help="每回合最多處理筆數 (未指定則處理所有未完成)",
    )
    parser.add_argument("--timeout", type=float, default=120.0, help="PDF 下載 timeout 秒數")
    parser.add_argument("--retry", type=int, default=1, help="單類查詢失敗重試次數 (不含首次)")
    parser.add_argument(
        "--round-interval",
        type=float,
        default=1.0,
        help="每回合結束後等待秒數，避免連續重打過快",
    )
    return parser.parse_args()


def find_descendant_pids(root_pid: int) -> set[int]:
    raw = subprocess.check_output(["ps", "-axo", "pid,ppid"], text=True)
    parents: dict[int, list[int]] = {}
    for line in raw.splitlines()[1:]:
        parts = line.split()
        if len(parts) != 2:
            continue
        pid = int(parts[0])
        ppid = int(parts[1])
        parents.setdefault(ppid, []).append(pid)

    descendants: set[int] = set()
    stack = [root_pid]
    while stack:
        current = stack.pop()
        for child in parents.get(current, []):
            if child in descendants:
                continue
            descendants.add(child)
            stack.append(child)
    return descendants


def cleanup_webdriver_processes() -> None:
    try:
        descendants = find_descendant_pids(os.getpid())
    except Exception:
        descendants = set()
    if not descendants:
        return

    detail_raw = subprocess.check_output(["ps", "-axo", "pid,command"], text=True)
    kill_targets: list[int] = []
    for line in detail_raw.splitlines()[1:]:
        stripped = line.strip()
        if not stripped:
            continue
        parts = stripped.split(maxsplit=1)
        if len(parts) < 2:
            continue
        pid = int(parts[0])
        cmd = parts[1]
        if pid not in descendants:
            continue
        if "chromedriver" in cmd or ("Google Chrome" in cmd and "--remote-debugging-port=" in cmd):
            kill_targets.append(pid)

    for pid in kill_targets:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except Exception:
            pass


def handle_signal(sig: int, _frame: Any) -> None:
    global SIGNAL_COUNT
    SIGNAL_COUNT += 1
    STOP_EVENT.set()
    if SIGNAL_COUNT == 1:
        print(f"\n收到中斷訊號({sig})，停止接新工作，等待目前任務收尾中...")
        return
    print(f"\n收到第二次中斷訊號({sig})，強制終止並清理 webdriver 子程序。")
    cleanup_webdriver_processes()
    os._exit(130)


def install_signal_handlers() -> None:
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("Name") or "").strip()
            national_id = (row.get("NationalId") or "").strip()
            if not name or not national_id:
                continue
            rows.append({"name": name, "national_id": national_id})
    return rows


def open_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def ensure_column(conn: sqlite3.Connection, table: str, column: str, sql_type: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table}_results)").fetchall()
    exists = any(row[1] == column for row in rows)
    if not exists:
        conn.execute(f"ALTER TABLE {table}_results ADD COLUMN {column} {sql_type}")


def init_db(conn: sqlite3.Connection) -> None:
    for table in QUERY_NAMES:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table}_results (
                national_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                ok INTEGER NOT NULL,
                status_code INTEGER,
                pdf_url TEXT,
                pdf_path TEXT,
                error TEXT,
                response_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        ensure_column(conn, table, "pdf_url", "TEXT")
        ensure_column(conn, table, "pdf_path", "TEXT")
        ensure_column(conn, table, "error", "TEXT")
    conn.commit()


def get_table_valid_ids(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(
        f"""
        SELECT national_id, pdf_path
        FROM {table}_results
        WHERE ok = 1
          AND pdf_path IS NOT NULL
          AND TRIM(pdf_path) <> ''
        """
    ).fetchall()
    valid_ids: set[str] = set()
    for national_id, pdf_path in rows:
        path = Path(pdf_path)
        if path.exists() and path.is_file() and path.stat().st_size > 0:
            valid_ids.add(national_id)
    return valid_ids


def get_completed_ids(conn: sqlite3.Connection) -> set[str]:
    domestic_ids = get_table_valid_ids(conn, "domestic")
    debt_ids = get_table_valid_ids(conn, "debt")
    bankrupt_ids = get_table_valid_ids(conn, "bankrupt")
    return domestic_ids & debt_ids & bankrupt_ids


def download_pdf(
    pdf_url: str,
    pdf_dir: Path,
    national_id: str,
    query_name: str,
    timeout: float,
) -> Path:
    req = urllib.request.Request(pdf_url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as response:
        content = response.read()
    if not content:
        raise ValueError("PDF 內容為空")

    endpoint_dir = pdf_dir / query_name
    endpoint_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = endpoint_dir / f"{national_id}.pdf"
    pdf_path.write_bytes(content)
    if pdf_path.stat().st_size == 0:
        raise ValueError("PDF 寫入後大小為 0")
    return pdf_path


def build_query_response(
    status: RPAQueryStatus,
    total_num: Any,
    pdf_url: Any,
    raw_result: Any,
) -> dict[str, Any]:
    if status == RPAQueryStatus.NORMAL:
        message = "Query completed: normal"
    elif status == RPAQueryStatus.ABNORMAL:
        message = "Query completed: abnormal"
    else:
        message = "Query failed"

    return {
        "status": status.value,
        "message": message,
        "total_num": total_num,
        "pdf_url": pdf_url,
        "raw_result": raw_result,
    }


def query_direct_once(
    query_name: str,
    name: str,
    national_id: str,
    pdf_dir: Path,
    timeout: float,
) -> dict[str, Any]:
    if STOP_EVENT.is_set():
        raise RuntimeError("stop requested")
    start = time.perf_counter()
    helper_call: Callable[[], tuple[RPAQueryStatus, Any, Any, Any]]
    if query_name == "domestic":
        helper = DomesticJudV2Helper(idnum=national_id, name=name)
        helper_call = helper.get_n_check_data
    elif query_name == "debt":
        helper = MoneyCheckHelper(idnum=national_id, name=name)
        helper_call = helper.check_debt
    else:
        helper = MoneyCheckHelper(idnum=national_id, name=name)
        helper_call = helper.check_bankrupt

    status, total_num, pdf_url, raw_result = helper_call()
    body = build_query_response(status, total_num, pdf_url, raw_result)
    if status not in {RPAQueryStatus.NORMAL, RPAQueryStatus.ABNORMAL}:
        raise ValueError(f"status 非成功狀態: {status.value}")
    if not isinstance(pdf_url, str) or not pdf_url.strip():
        raise ValueError("缺少 pdf_url")
    if not isinstance(total_num, int):
        raise ValueError("total_num 不完整")
    if not isinstance(raw_result, dict):
        raise ValueError("raw_result 不完整")

    pdf_path = download_pdf(pdf_url, pdf_dir, national_id, query_name, timeout)
    elapsed = round(time.perf_counter() - start, 3)
    return {
        "ok": True,
        "status_code": None,
        "elapsed_sec": elapsed,
        "response": body,
        "pdf_url": pdf_url,
        "pdf_path": str(pdf_path),
    }


def call_one_query_with_retry(
    query_name: str,
    name: str,
    national_id: str,
    pdf_dir: Path,
    timeout: float,
    retry: int,
) -> dict[str, Any]:
    attempts = retry + 1
    last_error = ""
    for attempt in range(1, attempts + 1):
        if STOP_EVENT.is_set():
            return {
                "ok": False,
                "status_code": None,
                "elapsed_sec": None,
                "attempt": attempt,
                "error": "stop requested",
            }
        try:
            result = query_direct_once(query_name, name, national_id, pdf_dir, timeout)
            result["attempt"] = attempt
            return result
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}".strip()
            if attempt == attempts:
                return {
                    "ok": False,
                    "status_code": None,
                    "elapsed_sec": None,
                    "attempt": attempt,
                    "error": last_error,
                }
            time.sleep(min(0.5 * attempt, 2.0))
    return {"ok": False, "status_code": None, "error": last_error}


def upsert_person_results(
    conn: sqlite3.Connection, national_id: str, person_results: dict[str, dict[str, Any]]
) -> bool:
    all_ok = True
    for table in QUERY_NAMES:
        item = person_results[table]
        response = item["response"]
        ok = bool(response.get("ok"))
        if not ok:
            all_ok = False
        conn.execute(
            f"""
            INSERT INTO {table}_results (
                national_id, name, ok, status_code, pdf_url, pdf_path, error, response_json, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(national_id) DO UPDATE SET
                name=excluded.name,
                ok=excluded.ok,
                status_code=excluded.status_code,
                pdf_url=excluded.pdf_url,
                pdf_path=excluded.pdf_path,
                error=excluded.error,
                response_json=excluded.response_json,
                updated_at=excluded.updated_at
            """,
            (
                national_id,
                item["name"],
                1 if ok else 0,
                response.get("status_code"),
                response.get("pdf_url"),
                response.get("pdf_path"),
                response.get("error"),
                json.dumps(response, ensure_ascii=False),
                item["updated_at"],
            ),
        )
    conn.commit()
    return all_ok


async def process_person(
    sem: asyncio.Semaphore,
    person: dict[str, str],
    pdf_dir: Path,
    timeout: float,
    retry: int,
) -> tuple[str, dict[str, dict[str, Any]]]:
    national_id = person["national_id"]
    name = person["name"]
    async with sem:
        tasks = {
            query_name: asyncio.create_task(
                asyncio.to_thread(
                    call_one_query_with_retry,
                    query_name,
                    name,
                    national_id,
                    pdf_dir,
                    timeout,
                    retry,
                )
            )
            for query_name in QUERY_NAMES
        }
        query_results = {query_name: await task for query_name, task in tasks.items()}

    merged: dict[str, dict[str, Any]] = {}
    now = int(time.time())
    for query_name, result in query_results.items():
        merged[query_name] = {
            "name": name,
            "response": result,
            "updated_at": now,
        }
    return national_id, merged


async def run_batch_and_persist(
    people: list[dict[str, str]],
    pdf_dir: Path,
    concurrency: int,
    timeout: float,
    retry: int,
    conn: sqlite3.Connection,
) -> tuple[int, int]:
    sem = asyncio.Semaphore(concurrency)
    processed_count = 0
    success_count = 0
    tasks = [
        asyncio.create_task(
            process_person(
                sem=sem,
                person=person,
                pdf_dir=pdf_dir,
                timeout=timeout,
                retry=retry,
            )
        )
        for person in people
    ]
    for fut in asyncio.as_completed(tasks):
        if STOP_EVENT.is_set():
            break
        national_id, per_person = await fut
        person_ok = upsert_person_results(conn, national_id, per_person)
        processed_count += 1
        if person_ok:
            success_count += 1

    # Stop requested: cancel remaining coroutine tasks promptly.
    if STOP_EVENT.is_set():
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return processed_count, success_count


def main() -> None:
    install_signal_handlers()
    args = parse_args()
    if args.concurrency <= 0:
        raise ValueError("--concurrency 必須 > 0")
    if args.limit is not None and args.limit <= 0:
        raise ValueError("--limit 必須 > 0")
    if args.retry < 0:
        raise ValueError("--retry 不能小於 0")
    if args.round_interval < 0:
        raise ValueError("--round-interval 不能小於 0")

    input_path = Path(args.input)
    db_path = Path(args.db_path)
    pdf_dir = Path(args.pdf_dir)
    if not input_path.exists():
        raise FileNotFoundError(f"找不到輸入檔案: {input_path}")
    pdf_dir.mkdir(parents=True, exist_ok=True)

    all_rows = read_csv_rows(input_path)
    if not all_rows:
        print("CSV 無可處理資料，結束。")
        return

    conn = open_db(db_path)
    init_db(conn)
    started_at = time.perf_counter()
    total_processed = 0
    total_success = 0
    round_no = 0
    try:
        while True:
            if STOP_EVENT.is_set():
                print("停止旗標已啟用，結束主迴圈。")
                break
            round_no += 1
            completed = get_completed_ids(conn)
            pending_rows = [r for r in all_rows if r["national_id"] not in completed]
            if args.limit:
                pending_rows = pending_rows[: args.limit]

            print(
                f"[Round {round_no}] 總筆數={len(all_rows)}, 已完成={len(completed)}, "
                f"本回合處理={len(pending_rows)}, concurrency={args.concurrency}"
            )
            if not pending_rows:
                print("全部成功完成，結束。")
                break

            processed_count, success_count = asyncio.run(
                run_batch_and_persist(
                    people=pending_rows,
                    pdf_dir=pdf_dir,
                    concurrency=args.concurrency,
                    timeout=args.timeout,
                    retry=args.retry,
                    conn=conn,
                )
            )
            total_processed += processed_count
            total_success += success_count

            if STOP_EVENT.is_set():
                print("已停止接新工作，準備結束。")
                break
            if args.round_interval > 0:
                time.sleep(args.round_interval)
    finally:
        if STOP_EVENT.is_set():
            cleanup_webdriver_processes()
        conn.close()

    elapsed = time.perf_counter() - started_at
    print(f"完成，耗時 {elapsed:.1f}s。累計回合成功 {total_success}/{total_processed} 筆。")
    print(f"結果已寫入 SQLite: {db_path}")


if __name__ == "__main__":
    main()
