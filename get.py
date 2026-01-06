import requests
import json
import html
from datetime import datetime, timezone, timedelta

now = datetime.now()

year_month = datetime.now().strftime("%Y%m")
day = now.day
hour = now.hour

url = f"https://tortoise-fluent-rationally.ngrok-free.app/api/60min/json/{year_month}"
#print('day' , day )

TZ  = timezone(timedelta(hours=8))
UTCtime = 'detectedAtUtc'
BLANK = None

def html_to_josn(page_html : str):
    start_tag = '<pre>'
    end_tag = '</pre>'
    start = page_html.find(start_tag)
    end = page_html.find(end_tag)
    if start == -1 or end == -1 or end <= start:
        raise ValueError("not found josn data in html")
    pre_content = page_html[start + len(start_tag):end]
    pre_content = html.unescape(pre_content).strip()
    return json.loads(pre_content)

def add_tw_datetime(s:str) -> datetime:
    dt_naive = datetime.strptime(s, "%Y/%m/%d %H:%M:%S")
    return dt_naive.replace(tzinfo=TZ)

def time_to_utc(dt_local: datetime) -> str:
    dt_utc = dt_local.astimezone(timezone.utc)
    return dt_utc.isoformat(timespec="milliseconds").replace("+00:00", "Z")

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
    
    field_map = {
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
    
    
    for src_key, dst_key in field_map.items():
        out[dst_key] = to_number_or_blank(rec.get(src_key))
    return out


def fetch_today_data() -> list[dict]:
    resp = requests.get(url, timeout=20)
    data = html_to_josn(resp.text)

    filtered_day = []
    for rec in data:
        dt_str = rec.get('日期時間')
        if not dt_str:
            continue
        try:
            dt_local = add_tw_datetime(dt_str)
        except Exception:
            continue
        if dt_local.day == now.day: 
            filtered_day.append(turn_data(rec, dt_local))
    return filtered_day

def main():
    result = fetch_today_data()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    
if __name__ == "__main__": main()


def fetch_hour_data(target_dt_local: datetime) -> list[dict]:

    if target_dt_local.tzinfo is None:
        target_dt_local = target_dt_local.replace(tzinfo=TZ)
    else:
        target_dt_local = target_dt_local.astimezone(TZ)

    year_month = target_dt_local.strftime("%Y%m")
    url = f"https://tortoise-fluent-rationally.ngrok-free.app/api/60min/json/{year_month}"

    resp = requests.get(url, timeout=20)
    data = html_to_josn(resp.text)
    if not isinstance(data, list):
        raise RuntimeError("資料不是 JSON array（list），格式不符預期。")

    target_y = target_dt_local.year
    target_m = target_dt_local.month
    target_d = target_dt_local.day
    target_h = target_dt_local.hour

    out = []
    for rec in data:
        dt_str = rec.get("日期時間")
        if not dt_str:
            continue
        try:
            dt_local = add_tw_datetime(dt_str)  # 已經會帶 TZ(+8)
        except Exception:
            continue

        # 只要同一天、同一小時
        if (dt_local.year == target_y and
            dt_local.month == target_m and
            dt_local.day == target_d and
            dt_local.hour == target_h):
            out.append(turn_data(rec, dt_local))

    return out

def fetch_previous_hour_data() -> list[dict]:
    now_local = datetime.now(TZ)
    prev_hour = now_local - timedelta(hours=1)
    return fetch_hour_data(prev_hour)

def main():
    result = fetch_previous_hour_data()
    print("Matched records (previous hour):", len(result))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    
if __name__ == "__main__": main()