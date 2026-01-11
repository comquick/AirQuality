import sys
import json
import argparse
from datetime import datetime, timedelta, timezone

from get2 import fetch_range 
from post import upload_with_relogin_and_dedup 

TZ_TW = timezone(timedelta(hours=8))

def log_info(msg: str) -> None:
    print(f"[INFO] {msg}")

def log_ok(msg: str) -> None:
    print(f"[OK] {msg}")

def log_warn(msg: str) -> None:
    print(f"[WARN] {msg}")

def log_err(msg: str) -> None:
    print(f"[ERR] {msg}", file=sys.stderr)


#ISO-like "YYYY-MM-DDTHH:MM"
def parse_tw_datetime(s: str) -> datetime:
    s = s.strip()
    fmts = ["%Y-%m-%dT%H:%M:%S"]
    for fmt in fmts:
        try:
            dt_naive = datetime.strptime(s, fmt)
            return dt_naive.replace(tzinfo=TZ_TW)
        except ValueError:
            continue
    raise ValueError(f"Unsupported datetime format: {s}")


def hour_floor(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ_TW)
    else:
        dt = dt.astimezone(TZ_TW)
    return dt.replace(minute=0, second=0, microsecond=0)

def run_backfill(start_local: datetime, end_local: datetime, dry_run: bool = False) -> int:

    start_local = hour_floor(start_local)
    end_local = hour_floor(end_local)

    if end_local <= start_local:
        log_err("end must be later than start (after hour_floor).")
        return 2

    log_info(f"Backfill range (TW): start={start_local.isoformat()} end={end_local.isoformat()}")

    r = fetch_range(start_local, end_local)

    records = r.get("records", [])
    missing_hours = r.get("missing_hours", [])
    meta = r.get("meta", {})

    log_info(f"get.fetch_range meta: {json.dumps(meta, ensure_ascii=False)}")

    if missing_hours:
        log_warn(f"Missing hours count={len(missing_hours)}")
        for h in missing_hours:
            log_warn(f"Missing hour: {h.isoformat()}")

    if not records:
        log_warn("No records returned from get.fetch_range. Nothing to upload.")
        return 0

    #逐筆上傳
    success = 0
    skipped_dup = 0
    failed = 0

    for i, payload in enumerate(records, start=1):
        detected = payload.get("detectedAtUtc")
        log_info(f"[{i}/{len(records)}] Upload payload detectedAtUtc={detected}")

        if dry_run:
            log_ok("Dry-run: skip upload")
            continue

        try:
            result = upload_with_relogin_and_dedup(payload)
            status = result.get("status")

            if status == "SKIP_DUPLICATE":
                skipped_dup += 1
                log_warn(f"Duplicate -> skip (detectedAtUtc={result.get('normalizedDetectedAtUtc')})")
                continue

            if status == "SUCCESS":
                success += 1
                latest_row = result.get("latestRow") or {}
                row_id = latest_row.get("id") or latest_row.get("Id")
                log_ok(f"Upload success (id={row_id}, detectedAtUtc={result.get('normalizedDetectedAtUtc')})")
                continue

            failed += 1
            log_err(f"Unexpected status: {json.dumps(result, ensure_ascii=False)}")

        except Exception as exc:
            failed += 1
            log_err(f"Upload failed for detectedAtUtc={detected}: {exc}")

    #統計
    log_info("Backfill summary:")
    log_info(f"  records_total = {len(records)}")
    log_info(f"  success       = {success}")
    log_info(f"  skipped_dup   = {skipped_dup}")
    log_info(f"  failed        = {failed}")
    log_info(f"  missing_hours = {len(missing_hours)}")

    #若有失敗則0
    return 1 if failed > 0 else 0

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Backfill AirQuality hourly data: get.fetch_range -> post.upload_with_relogin_and_dedup"
    )

    # 兩種用法：
    #1.指定 start/end 2.用 --hours N 表示回補最近 N 小時（以現在 TW 為基準）
    p.add_argument("--start", type=str, help='TW datetime, e.g. "2026-01-11 00:00"')
    p.add_argument("--end", type=str, help='TW datetime, e.g. "2026-01-11 12:00"')
    p.add_argument("--hours", type=int, help="Backfill last N hours (TW), e.g. 24")

    p.add_argument("--dry-run", action="store_true", help="Only print what would be uploaded.")
    return p


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    now_local = datetime.now(TZ_TW)

    if args.hours is not None:
        if args.hours <= 0:
            log_err("--hours must be > 0")
            return 2
        end_local = hour_floor(now_local)
        start_local = end_local - timedelta(hours=args.hours)
        return run_backfill(start_local, end_local, dry_run=args.dry_run)

    if args.start and args.end:
        try:
            start_local = parse_tw_datetime(args.start)
            end_local = parse_tw_datetime(args.end)
        except Exception as exc:
            log_err(str(exc))
            return 2
        return run_backfill(start_local, end_local, dry_run=args.dry_run)

    log_err('Please provide either "--hours N" or both "--start" and "--end".')
    return 2


if __name__ == "__main__":
    sys.exit(main())
