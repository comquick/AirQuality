import requests
import json
import html
from datetime import datetime, timezone, timedelta

# 基本設定與常數

TZ  = timezone(timedelta(hours=8))
BLANK = None
UTC = timezone.utc  
#空品站最新只有前一小時的資料
PUBLICATION_LAG_HOURS = 1

#資料如果是負的 變null
NEGATIVE_POLICY = "nullify" 

SOURCE_DT_KEY = "日期時間"

FIELD_MAP = { 
    "PM25": "pm_25",
    "NMHC": "nmhc",
    "THC": "thc",
    "CH4": "ch4",
    "SO2": "so2",
    "O3": "o3",
    "NOX": "nox",
    "NO": "no",
    "CO": "co",
    "CO2": "co2",
}

VALUE_FIELDS = list(FIELD_MAP.values())  

def build_month_url(year_month: str) -> str:  
    return f"https://tortoise-fluent-rationally.ngrok-free.app/api/60min/json/{year_month}"

def html_to_json(page_html: str):  
    start_tag = '<pre>'
    end_tag = '</pre>'
    start = page_html.find(start_tag)
    end = page_html.find(end_tag)
    if start == -1 or end == -1 or end <= start:
        raise ValueError("not found json data in html")
    pre_content = page_html[start + len(start_tag):end]
    pre_content = html.unescape(pre_content).strip()
    return json.loads(pre_content)

def load_month_data(year_month: str) -> list[dict]: 
    url = build_month_url(year_month)
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    data = html_to_json(resp.text)
    if not isinstance(data, list):
        raise RuntimeError("來源資料不是 JSON array（list），格式不符預期。")
    return data

def add_tw_datetime(s: str) -> datetime:
    dt_naive = datetime.strptime(s, "%Y/%m/%d %H:%M:%S")
    return dt_naive.replace(tzinfo=TZ)

def time_to_utc(dt_local: datetime) -> str:
    dt_utc = dt_local.astimezone(UTC)
    return dt_utc.isoformat(timespec="milliseconds").replace("+00:00", "Z")

def hour_floor(dt_local: datetime) -> datetime: 
    if dt_local.tzinfo is None:
        dt_local = dt_local.replace(tzinfo=TZ)
    else:
        dt_local = dt_local.astimezone(TZ)
    return dt_local.replace(minute=0, second=0, microsecond=0)

def iter_hours(start_local: datetime, end_local: datetime): 
    start_h = hour_floor(start_local)
    end_h = hour_floor(end_local)
    cur = start_h
    while cur < end_h:
        yield cur
        cur = cur + timedelta(hours=1)

def expected_latest_hour(now_local: datetime) -> datetime:  
    if now_local.tzinfo is None:
        now_local = now_local.replace(tzinfo=TZ)
    else:
        now_local = now_local.astimezone(TZ)
    return hour_floor(now_local - timedelta(hours=PUBLICATION_LAG_HOURS))


def to_number_or_blank(x):
    if x is None:
        return BLANK
    s = str(x).strip()
    if s == "" or s.lower() in {"na", "nan", "null", "none"}:
        return BLANK
    try:
        return float(s)
    except ValueError:
        return BLANK


def turn_data(rec: dict, dt_local: datetime) -> dict:
    out = {
        "detectedAtUtc": time_to_utc(dt_local),
        "pm_25": BLANK,
        "nmhc": BLANK,
        "thc": BLANK,
        "ch4": BLANK,
        "so2": BLANK,
        "o3": BLANK,
        "nox": BLANK,
        "no": BLANK,
        "co": BLANK,
        "co2": BLANK,
    }

    for src_key, dst_key in FIELD_MAP.items():
        out[dst_key] = to_number_or_blank(rec.get(src_key))
    return out

#時間處理和QC 1.時間不可以大於現在時間 2.數值不能是負的
def qc_publication_timing(dt_local_hour: datetime, now_local: datetime) -> bool:  
    latest = expected_latest_hour(now_local)
    return dt_local_hour <= latest

def qc_negative_values(record: dict) -> tuple[dict, list[str]]: 
    flags = []
    for k in VALUE_FIELDS:
        v = record.get(k)
        if isinstance(v, (int, float)) and v < 0:
            if NEGATIVE_POLICY == "nullify":
                record[k] = BLANK
                flags.append(f"NEGATIVE_NULLIFIED:{k}")
            elif NEGATIVE_POLICY == "reject_record":
                flags.append(f"NEGATIVE_REJECT:{k}")
    return record, flags

def apply_basic_qc(record: dict, dt_local_hour: datetime, now_local: datetime) -> tuple[dict | None, list[str]]:  # [NEW]

    flags = []

    if not qc_publication_timing(dt_local_hour, now_local):
        flags.append("REJECT_PUBLICATION_TIMING")
        return None, flags

    record, neg_flags = qc_negative_values(record)
    flags.extend(neg_flags)

    if NEGATIVE_POLICY == "reject_record" and any(f.startswith("NEGATIVE_REJECT") for f in flags):
        flags.append("REJECT_NEGATIVE_POLICY")
        return None, flags

    return record, flags


def fetch_today_data() -> list[dict]:
    now_local = datetime.now(TZ)
    year_month = now_local.strftime("%Y%m")

    data = load_month_data(year_month)

    filtered_day = []
    for rec in data:
        dt_str = rec.get(SOURCE_DT_KEY)
        if not dt_str:
            continue
        try:
            dt_local = add_tw_datetime(dt_str)
        except Exception:
            continue

        if dt_local.day == now_local.day:
            dt_hour = hour_floor(dt_local)

            record = turn_data(rec, dt_hour)
            record_qc, _flags = apply_basic_qc(record, dt_hour, now_local)
            if record_qc is not None:
                filtered_day.append(record_qc)

    return filtered_day


def fetch_hour_data(target_dt_local: datetime) -> list[dict]:
    if target_dt_local.tzinfo is None:
        target_dt_local = target_dt_local.replace(tzinfo=TZ)
    else:
        target_dt_local = target_dt_local.astimezone(TZ)

    target_hour = hour_floor(target_dt_local)
    year_month = target_hour.strftime("%Y%m")

    data = load_month_data(year_month)

    out = []
    for rec in data:
        dt_str = rec.get(SOURCE_DT_KEY)
        if not dt_str:
            continue
        try:
            dt_local = add_tw_datetime(dt_str)
        except Exception:
            continue

        dt_hour = hour_floor(dt_local)

        #同一小時
        if dt_hour == target_hour:
            record = turn_data(rec, dt_hour)

            #基本 QC
            now_local = datetime.now(TZ)
            record_qc, _flags = apply_basic_qc(record, dt_hour, now_local)
            if record_qc is not None:
                out.append(record_qc)

    return out


def fetch_previous_hour_data() -> list[dict]:
    now_local = datetime.now(TZ)
    prev_hour = now_local - timedelta(hours=1)
    return fetch_hour_data(prev_hour)

#資料缺失檢查
def fetch_range(start_dt_local: datetime, end_dt_local: datetime, now_local: datetime | None = None) -> dict: 

    if start_dt_local.tzinfo is None:
        start_dt_local = start_dt_local.replace(tzinfo=TZ)
    else:
        start_dt_local = start_dt_local.astimezone(TZ)

    if end_dt_local.tzinfo is None:
        end_dt_local = end_dt_local.replace(tzinfo=TZ)
    else:
        end_dt_local = end_dt_local.astimezone(TZ)

    if now_local is None:
        now_local = datetime.now(TZ)
    else:
        now_local = now_local.astimezone(TZ) if now_local.tzinfo else now_local.replace(tzinfo=TZ)

    publish_upto = expected_latest_hour(now_local)

    effective_end = min(end_dt_local, publish_upto + timedelta(hours=1))

    expected_hours = list(iter_hours(start_dt_local, effective_end))

    month_cache: dict[str, list[dict]] = {}

    def get_month_data_cached(ym: str) -> list[dict]:
        if ym not in month_cache:
            month_cache[ym] = load_month_data(ym)
        return month_cache[ym]

    months = set()
    for h in expected_hours:
        months.add(h.strftime("%Y%m"))

    hour_index: dict[datetime, dict] = {}
    for ym in sorted(months):
        data = get_month_data_cached(ym)
        for rec in data:
            dt_str = rec.get(SOURCE_DT_KEY)
            if not dt_str:
                continue
            try:
                dt_local = add_tw_datetime(dt_str)
            except Exception:
                continue
            dt_hour = hour_floor(dt_local)

            if dt_hour in expected_hours:
                hour_index[dt_hour] = rec

    records: list[dict] = []
    missing_hours: list[datetime] = []

    for h in expected_hours:
        rec = hour_index.get(h)
        if rec is None:
            missing_hours.append(h)
            continue

        record = turn_data(rec, h)
        record_qc, _flags = apply_basic_qc(record, h, now_local)

        if record_qc is None:
            continue

        records.append(record_qc)
        # qc_flags_by_hour[h.isoformat()] = _flags

    return {
        "records": records,
        "missing_hours": missing_hours,
        "meta": { 
            "start_local": start_dt_local.isoformat(),
            "end_local": end_dt_local.isoformat(),
            "effective_end_local": effective_end.isoformat(),
            "publish_upto_local": publish_upto.isoformat(),
            "expected_hours": len(expected_hours),
            "records": len(records),
            "missing": len(missing_hours),
        }
    }


def main():  
    result = fetch_previous_hour_data()
    print("Matched records (previous hour):", len(result))
    print(json.dumps(result, ensure_ascii=False, indent=2))

    now_local = datetime.now(TZ)
    start = now_local - timedelta(hours=6)
    end = now_local
    r = fetch_range(start, end, now_local=now_local)
    print("Range meta:", json.dumps(r["meta"], ensure_ascii=False, indent=2))
    print("Missing hours:", [h.isoformat() for h in r["missing_hours"]])

if __name__ == "__main__":
    main()
