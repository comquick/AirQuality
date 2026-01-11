import os
import sys
import json
import argparse
import requests
from datetime import datetime, timezone, timedelta

from post import login  
from backfill import run_backfill  

TZ_TW = timezone(timedelta(hours=8))
UTC = timezone.utc

BASE_URL = "https://meteo.local2.tempestdigi.com"
LIST_URL = f"{BASE_URL}/api/AirQuality/list"

DEFAULT_PAGE_SIZE = 200


def log_info(msg: str) -> None:
    print(f"[INFO] {msg}")

def log_ok(msg: str) -> None:
    print(f"[OK] {msg}")

def log_warn(msg: str) -> None:
    print(f"[WARN] {msg}")

def log_err(msg: str) -> None:
    print(f"[ERR] {msg}", file=sys.stderr)


def get_credentials() -> tuple[str, str]:
    account = os.getenv("METEO_ACCOUNT", "").strip()
    password = os.getenv("METEO_PASSWORD", "").strip()
    #account = "yolko"
    #password = "yolko123"
    if not account or not password:
        raise ValueError("Missing METEO_ACCOUNT or METEO_PASSWORD in environment variables.")
    return account, password


def parse_iso_utc(s: str) -> datetime:
    s = (s or "").strip()
    if not s:
        raise ValueError("Empty detectedAtUtc")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def hour_floor_utc(dt: datetime) -> datetime:
    dt = dt.astimezone(UTC)
    return dt.replace(minute=0, second=0, microsecond=0)


def iter_hours_utc(start_utc: datetime, end_utc: datetime):
    cur = hour_floor_utc(start_utc)
    end_h = hour_floor_utc(end_utc)
    while cur < end_h:
        yield cur
        cur = cur + timedelta(hours=1)


def list_latest_rows(session: requests.Session, page: int, page_size: int) -> dict:
    body = {
        "page": page,
        "pageSize": page_size,
        "sortModel": {"items": [{"field": "DetectedAtUtc", "sort": "desc"}]},
        "filterModel": {"items": []},
    }
    resp = session.post(LIST_URL, json=body, timeout=20)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"LIST failed: {resp.status_code} | body: {resp.text[:300]}")
    data = resp.json()
    if not isinstance(data, dict) or "rows" not in data or not isinstance(data["rows"], list):
        raise RuntimeError("LIST response schema unexpected (missing 'rows' list)")
    return data


def fetch_rows_for_audit(session: requests.Session, hours: int, page_size: int = DEFAULT_PAGE_SIZE) -> list[dict]:
    needed = hours + 10  # buffer
    rows: list[dict] = []
    page = 0
    max_pages = 10

    while len(rows) < needed and page < max_pages:
        data = list_latest_rows(session, page=page, page_size=page_size)
        batch = data.get("rows", [])
        if not batch:
            break
        rows.extend(batch)
        page += 1
        if len(batch) < page_size:
            break

    return rows


def build_existing_hours_set(rows: list[dict]) -> set[datetime]:
    existing = set()
    for r in rows:
        s = r.get("detectedAtUtc")
        if not s:
            continue
        try:
            dt = parse_iso_utc(s)
            existing.add(hour_floor_utc(dt))
        except Exception:
            continue
    return existing

#檢查連續性
def audit_continuity(hours: int, now_utc: datetime | None = None) -> dict:
    if now_utc is None:
        now_utc = datetime.now(UTC)
    else:
        now_utc = now_utc.astimezone(UTC)

    latest_expected = hour_floor_utc(now_utc - timedelta(hours=1))
    start_expected = latest_expected - timedelta(hours=hours)
    end_expected = latest_expected + timedelta(hours=1)  # [start, end)

    account, password = get_credentials()
    with requests.Session() as session:
        login(session, account, password)
        rows = fetch_rows_for_audit(session, hours=hours)
        existing_hours = build_existing_hours_set(rows)

    expected_hours = list(iter_hours_utc(start_expected, end_expected))
    missing = [h for h in expected_hours if h not in existing_hours]

    return {
        "expected_hours": expected_hours,
        "missing_hours": missing,
        "existing_count": len(existing_hours),
        "meta": {
            "hours": hours,
            "now_utc": now_utc.isoformat(),
            "latest_expected_utc": latest_expected.isoformat(),
            "start_expected_utc": start_expected.isoformat(),
            "end_expected_utc": end_expected.isoformat(),
            "rows_fetched": len(rows),
            "expected_count": len(expected_hours),
            "missing_count": len(missing),
        }
    }


def to_tw(dt_utc: datetime) -> datetime:
    return dt_utc.astimezone(TZ_TW)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Audit AirQuality continuity in DB via LIST API.")
    p.add_argument("--hours", type=int, default=24, help="Audit last N hours (default 24).")
    p.add_argument("--json", action="store_true", help="Print full JSON result (expected/missing lists).")

    #直接觸發backfill
    p.add_argument("--auto-backfill", action="store_true", help="If missing hours exist, run backfill automatically.")
    p.add_argument("--backfill-dry-run", action="store_true", help="If auto-backfill, do not upload; only print.")
    p.add_argument(
        "--backfill-hours",
        type=int,
        default=None,
        help="If auto-backfill, backfill last N hours (default: same as --hours)."
    )

    return p


def main() -> int:
    args = build_arg_parser().parse_args()

    if args.hours <= 0:
        log_err("--hours must be > 0")
        return 2

    try:
        r = audit_continuity(hours=args.hours)
    except Exception as exc:
        log_err(str(exc))
        return 1

    meta = r["meta"]
    missing = r["missing_hours"]

    log_info(f"Audit meta: {json.dumps(meta, ensure_ascii=False)}")

    if not missing:
        log_ok(f"Continuity OK: last {args.hours} hours are complete.")
        return 0

    log_warn(f"Continuity FAIL: missing_count={len(missing)} (UTC hours)")
    for h in missing:
        log_warn(f"Missing UTC hour: {h.isoformat()} | TW: {to_tw(h).isoformat()}")

    if args.json:
        out = {
            **r,
            "expected_hours": [x.isoformat() for x in r["expected_hours"]],
            "missing_hours": [x.isoformat() for x in missing],
        }
        print(json.dumps(out, ensure_ascii=False, indent=2))

    #自動 backfill
    if args.auto_backfill:
        backfill_hours = args.backfill_hours if args.backfill_hours is not None else args.hours
        if backfill_hours <= 0:
            log_err("--backfill-hours must be > 0")
            return 2

        now_tw = datetime.now(TZ_TW)
        end_tw = now_tw.replace(minute=0, second=0, microsecond=0)  #對齊整點
        start_tw = end_tw - timedelta(hours=backfill_hours)

        log_warn(
            f"Auto-backfill enabled. Range (TW): start={start_tw.isoformat()} end={end_tw.isoformat()} "
            f"dry_run={args.backfill_dry_run}"
        )


        rc = run_backfill(start_tw, end_tw, dry_run=args.backfill_dry_run)

        if rc != 0:
            log_err(f"Auto-backfill finished with rc={rc}")
            return rc

        log_ok("Auto-backfill finished successfully.")

        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
