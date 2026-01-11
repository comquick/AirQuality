"""
manual_fix.py

用途：
- 指定「台灣時間的某一個整點小時」，對資料庫中該小時資料進行「刪掉再補」或「只補不刪」
- 刪除 API 依你的 Swagger 圖：DELETE /api/AirQuality?id=<id>（id 在 query string）

依賴：
- get.py：提供 fetch_hour_data(target_dt_local)
- post.py：提供 login(), upload_with_relogin_and_dedup(), _normalize_detected_at_utc(), _get_credentials()

使用範例：
1) 乾跑（不刪不補，只看會做什麼）
   METEO_ACCOUNT="yolko" METEO_PASSWORD="yolko123" python manual_fix.py --hour "2026-01-11 05" --dry-run

2) 刪掉再補（預設模式）
   METEO_ACCOUNT="yolko" METEO_PASSWORD="yolko123" python manual_fix.py --hour "2026-01-11 05"

3) 只補不刪（依賴 dedup，若 DB 已有該筆通常會 SKIP_DUPLICATE）
   METEO_ACCOUNT="yolko" METEO_PASSWORD="yolko123" python manual_fix.py --hour "2026-01-11 05" --mode reupload-only

可選參數：
- --list-page-size：LIST 拉回的最新筆數，預設 200（找不到時可調大）
"""

import sys
import json
import argparse
import requests
from datetime import datetime, timedelta, timezone

from get import fetch_hour_data
from post import (
    login,
    upload_with_relogin_and_dedup,
    _normalize_detected_at_utc,
    _get_credentials,
)

BASE_URL = "https://meteo.local2.tempestdigi.com"
LIST_URL = f"{BASE_URL}/api/AirQuality/list"
DELETE_URL = f"{BASE_URL}/api/AirQuality"  # ✅ Swagger：DELETE /api/AirQuality?id=xxx

TZ_TW = timezone(timedelta(hours=8))


# ------------------------
# log helpers
# ------------------------
def log_info(msg: str) -> None:
    print(f"[INFO] {msg}")


def log_ok(msg: str) -> None:
    print(f"[OK] {msg}")


def log_warn(msg: str) -> None:
    print(f"[WARN] {msg}")


def log_err(msg: str) -> None:
    print(f"[ERR] {msg}", file=sys.stderr)


# ------------------------
# 時間解析（台灣時間整點）
# ------------------------
def parse_tw_hour(s: str) -> datetime:
    """
    解析台灣時間整點小時字串，回傳 tz-aware datetime(+08:00)，並對齊整點。

    支援格式：
    - "YYYY-MM-DD HH"     例如 "2026-01-11 05"
    - "YYYY-MM-DD HH:MM"  例如 "2026-01-11 05:00"
    """
    s = (s or "").strip()
    fmts = ("%Y-%m-%d %H", "%Y-%m-%d %H:%M")
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)  # naive
            dt = dt.replace(tzinfo=TZ_TW)
            # 對齊整點
            return dt.replace(minute=0, second=0, microsecond=0)
        except ValueError:
            continue
    raise ValueError(f"Invalid --hour format: {s!r}. Use 'YYYY-MM-DD HH' e.g. '2026-01-11 05'.")


# ------------------------
# DB side helpers
# ------------------------
def list_latest_rows(session: requests.Session, page_size: int = 200) -> list[dict]:
    """
    用 LIST API 拉回最新 page_size 筆資料（desc by DetectedAtUtc）
    """
    body = {
        "page": 0,
        "pageSize": page_size,
        "sortModel": {"items": [{"field": "DetectedAtUtc", "sort": "desc"}]},
        "filterModel": {"items": []},
    }
    resp = session.post(LIST_URL, json=body, timeout=20)

    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"LIST failed: {resp.status_code} | body: {resp.text[:300]}")

    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"LIST response is not valid JSON | body: {resp.text[:300]}")

    rows = data.get("rows", [])
    if not isinstance(rows, list):
        raise RuntimeError("LIST response schema unexpected (rows is not a list)")
    return rows


def find_row_by_detected_at(rows: list[dict], target_detected_at_norm: str) -> dict | None:
    """
    rows：LIST 拉回來的 rows
    target_detected_at_norm：規範化後的 detectedAtUtc（YYYY-MM-DDTHH:MM:SSZ）
    """
    for r in rows:
        s = r.get("detectedAtUtc")
        if not s:
            continue
        try:
            if _normalize_detected_at_utc(s) == target_detected_at_norm:
                return r
        except Exception:
            continue
    return None


def extract_row_id(row: dict) -> str | None:
    """
    後端可能用 id 或 Id（大小寫不一）
    """
    if not isinstance(row, dict):
        return None
    v = row.get("id")
    if v:
        return str(v)
    v = row.get("Id")
    if v:
        return str(v)
    return None


def delete_row_by_id(session: requests.Session, row_id: str) -> None:
    """
    依 Swagger：DELETE /api/AirQuality?id=<id>
    """
    resp = session.delete(
        DELETE_URL,
        params={"id": row_id},  # ✅ query string
        timeout=20,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"DELETE failed: {resp.status_code} | body: {resp.text[:300]}")


# ------------------------
# main
# ------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="Manual fix: delete and/or re-upload one specific hour.")
    ap.add_argument("--hour", required=True, help='TW hour, e.g. "2026-01-11 05"')
    ap.add_argument(
        "--mode",
        choices=["delete-reupload", "reupload-only"],
        default="delete-reupload",
        help="delete-reupload: delete existing row then re-upload; reupload-only: only upload (dedup).",
    )
    ap.add_argument("--dry-run", action="store_true", help="Print actions but do not delete/upload.")
    ap.add_argument(
        "--list-page-size",
        type=int,
        default=200,
        help="How many latest rows to pull from LIST for searching the target hour.",
    )
    args = ap.parse_args()

    # 0) 解析目標小時（TW）
    try:
        target_hour_tw = parse_tw_hour(args.hour)
    except Exception as exc:
        log_err(str(exc))
        return 2

    log_info(f"Target hour (TW): {target_hour_tw.isoformat()} | mode={args.mode} | dry_run={args.dry_run}")

    # 1) 從來源抓該小時資料
    try:
        records = fetch_hour_data(target_hour_tw)
    except Exception as exc:
        log_err(f"fetch_hour_data failed: {exc}")
        return 1

    if not records:
        log_err("No source data found for this hour. Abort.")
        return 1

    # 你目前設計是每小時期望一筆，所以取第一筆
    payload = records[0]

    if "detectedAtUtc" not in payload:
        log_err("Source payload missing detectedAtUtc. Abort.")
        return 1

    try:
        target_norm = _normalize_detected_at_utc(payload["detectedAtUtc"])
    except Exception as exc:
        log_err(f"normalize detectedAtUtc failed: {exc}")
        return 1

    log_info(f"Target detectedAtUtc (normalized): {target_norm}")

    # 2) 登入 DB（為了 LIST + DELETE）
    try:
        account, password = _get_credentials()
    except Exception as exc:
        log_err(str(exc))
        return 1

    with requests.Session() as session:
        try:
            login(session, account, password)
        except Exception as exc:
            log_err(f"Login failed: {exc}")
            return 1

        # 3) 查 DB 是否已有該小時資料（為了刪除）
        try:
            rows = list_latest_rows(session, page_size=args.list_page_size)
        except Exception as exc:
            log_err(f"LIST failed: {exc}")
            return 1

        existing = find_row_by_detected_at(rows, target_norm)

        if existing:
            row_id = extract_row_id(existing)
            log_info("DB existing row found for target hour.")
            log_info(f"Existing row id={row_id}")
            if row_id is None:
                log_warn("Existing row has no id/Id field; delete step cannot proceed.")
        else:
            row_id = None
            log_info("DB existing row NOT found for target hour (within latest LIST page).")

        # 4) delete（若 mode=delete-reupload 且找得到 row）
        if args.mode == "delete-reupload":
            if existing and row_id:
                log_warn(f"Will DELETE row id={row_id} for detectedAtUtc={target_norm}")
                if not args.dry_run:
                    try:
                        delete_row_by_id(session, row_id)
                        log_ok(f"Deleted row id={row_id}")
                    except Exception as exc:
                        log_err(f"DELETE failed: {exc}")
                        return 1
            else:
                log_info("Delete step skipped (no existing row found or missing id).")
        else:
            log_info("Mode is reupload-only, skip delete step.")

    # 5) 上傳（用 post 的安全流程：去重/重登/成功回查）
    if args.dry_run:
        log_ok("Dry-run: skip upload.")
        log_info("Payload that would be uploaded:")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    try:
        result = upload_with_relogin_and_dedup(payload)
    except Exception as exc:
        log_err(f"Upload failed: {exc}")
        return 1

    status = result.get("status")
    if status == "SKIP_DUPLICATE":
        log_warn(f"Upload skipped due to duplicate: {result.get('normalizedDetectedAtUtc')}")
        return 0

    if status == "SUCCESS":
        latest_row = result.get("latestRow") or {}
        new_id = extract_row_id(latest_row)
        log_ok(f"Upload success | detectedAtUtc={result.get('normalizedDetectedAtUtc')} | id={new_id}")

        # 顯示最新 row（若找得到）
        if latest_row:
            log_info("Uploaded row (from LIST latest):")
            print(json.dumps(latest_row, ensure_ascii=False, indent=2))
        else:
            log_info("Uploaded row not found in latest list; showing payload instead:")
            print(json.dumps(result.get("payload") or payload, ensure_ascii=False, indent=2))
        return 0

    log_err(f"Unexpected result: {json.dumps(result, ensure_ascii=False)}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
