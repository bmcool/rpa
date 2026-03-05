#!/usr/bin/env python3
"""
家消破批次腳本 (SQLite 版)

功能:
- 從 CSV 讀取 Name / NationalId
- 每筆打三支 API: domestic / debt / bankrupt
- 執行過程持續寫入 SQLite，支援隨時中斷
- 可指定 concurrency 與本次處理筆數
- 可續跑 (已完成者自動跳過)
"""
import argparse
import asyncio
import csv
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

import httpx

DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_INPUT = "list.csv"
DEFAULT_DB = "results_household_debt_bankrupt.sqlite"
ENDPOINTS = {
    "domestic": "/domestic-jud",
    "debt": "/debt-jud",
    "bankrupt": "/bankrupt-jud",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="家消破 CSV 批次查詢")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="CSV 路徑 (預設: list.csv)")
    parser.add_argument("--db-path", default=DEFAULT_DB, help="SQLite 結果檔路徑")
    parser.add_argument("--pdf-dir", default="pdfs", help="PDF 下載目錄")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="API base URL")
    parser.add_argument("--concurrency", "-c", type=int, default=3, help="並發數")
    parser.add_argument(
        "--limit",
        "-n",
        type=int,
        default=None,
        help="本次最多處理筆數 (未指定則處理所有未完成)",
    )
    parser.add_argument("--timeout", type=float, default=120.0, help="每次請求 timeout 秒數")
    parser.add_argument("--retry", type=int, default=1, help="失敗重試次數 (不含首次)")
    return parser.parse_args()


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


def init_db(conn: sqlite3.Connection) -> None:
    for table in ENDPOINTS:
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


def ensure_column(conn: sqlite3.Connection, table: str, column: str, sql_type: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table}_results)").fetchall()
    exists = any(row[1] == column for row in rows)
    if not exists:
        conn.execute(f"ALTER TABLE {table}_results ADD COLUMN {column} {sql_type}")


def get_completed_ids(conn: sqlite3.Connection) -> set[str]:
    domestic_ids = get_table_valid_ids(conn, "domestic")
    debt_ids = get_table_valid_ids(conn, "debt")
    bankrupt_ids = get_table_valid_ids(conn, "bankrupt")
    return domestic_ids & debt_ids & bankrupt_ids


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


async def download_pdf(
    client: httpx.AsyncClient,
    pdf_url: str,
    pdf_dir: Path,
    national_id: str,
    cache_name: str,
    timeout: float,
) -> Path:
    resp = await client.get(pdf_url, timeout=timeout, follow_redirects=True)
    if not (200 <= resp.status_code < 300):
        raise ValueError(f"PDF 下載失敗 HTTP {resp.status_code}")
    content = resp.content
    if not content:
        raise ValueError("PDF 內容為空")

    endpoint_dir = pdf_dir / cache_name
    endpoint_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = endpoint_dir / f"{national_id}.pdf"
    pdf_path.write_bytes(content)
    if pdf_path.stat().st_size == 0:
        raise ValueError("PDF 寫入後大小為 0")
    return pdf_path


def upsert_person_results(
    conn: sqlite3.Connection, national_id: str, person_results: dict[str, dict[str, Any]]
) -> bool:
    all_ok = True
    for table in ("domestic", "debt", "bankrupt"):
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


async def call_one_endpoint(
    client: httpx.AsyncClient,
    cache_name: str,
    base_url: str,
    endpoint: str,
    payload: dict[str, str],
    national_id: str,
    pdf_dir: Path,
    timeout: float,
    retry: int,
) -> dict[str, Any]:
    url = base_url.rstrip("/") + endpoint
    attempts = retry + 1
    last_error = ""
    for attempt in range(1, attempts + 1):
        resp: httpx.Response | None = None
        start = time.perf_counter()
        try:
            resp = await client.post(url, json=payload, timeout=timeout)
            elapsed = round(time.perf_counter() - start, 3)
            body: Any
            try:
                body = resp.json()
            except Exception:
                body = resp.text
            if not isinstance(body, dict):
                raise ValueError("API 回傳非 JSON 物件")
            if not (200 <= resp.status_code < 300):
                raise ValueError(f"HTTP {resp.status_code}")
            status = body.get("status")
            pdf_url = body.get("pdf_url")
            total_num = body.get("total_num")
            raw_result = body.get("raw_result")
            if status not in {"Y", "N"}:
                raise ValueError(f"status 非成功狀態: {status!r}")
            if not isinstance(pdf_url, str) or not pdf_url.strip():
                raise ValueError("缺少 pdf_url")
            if not isinstance(total_num, int):
                raise ValueError("total_num 不完整")
            if not isinstance(raw_result, dict):
                raise ValueError("raw_result 不完整")

            pdf_path = await download_pdf(client, pdf_url, pdf_dir, national_id, cache_name, timeout)
            return {
                "ok": True,
                "status_code": resp.status_code,
                "elapsed_sec": elapsed,
                "attempt": attempt,
                "response": body,
                "pdf_url": pdf_url,
                "pdf_path": str(pdf_path),
            }
        except Exception as exc:
            elapsed = round(time.perf_counter() - start, 3)
            last_error = f"{type(exc).__name__}: {exc}".strip()
            if attempt == attempts:
                return {
                    "ok": False,
                    "status_code": resp.status_code if resp is not None else None,
                    "elapsed_sec": elapsed,
                    "attempt": attempt,
                    "error": last_error,
                }
            await asyncio.sleep(min(0.5 * attempt, 2.0))

    return {"ok": False, "status_code": None, "error": last_error}


async def process_person(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    person: dict[str, str],
    base_url: str,
    pdf_dir: Path,
    timeout: float,
    retry: int,
) -> tuple[str, dict[str, dict[str, Any]]]:
    national_id = person["national_id"]
    name = person["name"]
    payload = {"ino": national_id, "name": name}

    async with sem:
        tasks = {
            cache_name: asyncio.create_task(
                call_one_endpoint(
                    client=client,
                    cache_name=cache_name,
                    base_url=base_url,
                    endpoint=endpoint,
                    payload=payload,
                    national_id=national_id,
                    pdf_dir=pdf_dir,
                    timeout=timeout,
                    retry=retry,
                )
            )
            for cache_name, endpoint in ENDPOINTS.items()
        }
        endpoint_results = {cache_name: await task for cache_name, task in tasks.items()}

    merged: dict[str, dict[str, Any]] = {}
    now = int(time.time())
    for cache_name, result in endpoint_results.items():
        merged[cache_name] = {
            "name": name,
            "response": result,
            "updated_at": now,
        }
    return national_id, merged


async def run_batch_and_persist(
    people: list[dict[str, str]],
    base_url: str,
    pdf_dir: Path,
    concurrency: int,
    timeout: float,
    retry: int,
    conn: sqlite3.Connection,
) -> tuple[int, int]:
    sem = asyncio.Semaphore(concurrency)
    processed_count = 0
    success_count = 0

    async with httpx.AsyncClient() as client:
        tasks = [
            process_person(
                client, sem, person, base_url=base_url, pdf_dir=pdf_dir, timeout=timeout, retry=retry
            )
            for person in people
        ]
        for fut in asyncio.as_completed(tasks):
            national_id, per_person = await fut
            person_ok = upsert_person_results(conn, national_id, per_person)
            processed_count += 1
            if person_ok:
                success_count += 1
    return processed_count, success_count


def main() -> None:
    args = parse_args()
    if args.concurrency <= 0:
        raise ValueError("--concurrency 必須 > 0")
    if args.limit is not None and args.limit <= 0:
        raise ValueError("--limit 必須 > 0")
    if args.retry < 0:
        raise ValueError("--retry 不能小於 0")

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
    completed = get_completed_ids(conn)
    pending_rows = [r for r in all_rows if r["national_id"] not in completed]
    if args.limit:
        pending_rows = pending_rows[: args.limit]

    print(
        f"總筆數={len(all_rows)}, 已完成={len(completed)}, "
        f"本次處理={len(pending_rows)}, concurrency={args.concurrency}"
    )
    if not pending_rows:
        print("沒有需要處理的新資料。")
        conn.close()
        return

    started_at = time.perf_counter()
    success_count = 0
    processed_count = 0
    try:
        processed_count, success_count = asyncio.run(
            run_batch_and_persist(
                people=pending_rows,
                base_url=args.base_url,
                pdf_dir=pdf_dir,
                concurrency=args.concurrency,
                timeout=args.timeout,
                retry=args.retry,
                conn=conn,
            )
        )
    except KeyboardInterrupt:
        print("偵測到中斷，已保存目前完成的資料，可直接續跑。")
    finally:
        conn.close()

    elapsed = time.perf_counter() - started_at

    print(f"完成，耗時 {elapsed:.1f}s。完整成功 {success_count}/{processed_count} 筆。")
    print(f"結果已寫入 SQLite: {db_path}")


if __name__ == "__main__":
    main()
