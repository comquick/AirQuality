import os
import sys
import json
import requests
from datetime import datetime, timezone
from get2 import fetch_previous_hour_data

BASE_URL = "https://meteo.local2.tempestdigi.com"
LOGIN_URL = f"{BASE_URL}/api/Account/login"
LIST_URL = f"{BASE_URL}/api/AirQuality/list"
UPLOAD_URL = f"{BASE_URL}/api/AirQuality"
#檢查是不是重複的(前24筆資料)
PAGE_SIZE = 24 
#需要的欄位
REQUIRED_FIELDS = [
    "detectedAtUtc",
    "pm_25",
    "nmhc",
    "thc",
    "ch4",
    "so2",
    "o3",
    "nox",
    "no",
    "co",
    "co2",
]

#方便檢查狀態
def log_info(msg: str) -> None:
    print(f"[INFO] {msg}")
def log_ok(msg: str) -> None:
    print(f"[OK] {msg}")
def log_skip(msg: str) -> None:
    print(f"[SKIP] {msg}")
def log_err(msg: str) -> None:
    print(f"[ERR] {msg}", file=sys.stderr)
    
#不能是空白
def _is_blank_string(v) -> bool:
    return isinstance(v, str) and v.strip() == ""
#key一定要 可以是null 但不能是空白
def _validate_allow_null_no_blank(record: dict) -> dict:
    missing_keys = [k for k in REQUIRED_FIELDS if k not in record]
    if missing_keys:
        raise ValueError(f"Missing keys: {', '.join(missing_keys)}")

    blank_fields = [k for k in REQUIRED_FIELDS if _is_blank_string(record.get(k))]
    if blank_fields:
        raise ValueError(f"Blank string not allowed in fields: {', '.join(blank_fields)}")

    return {k: record.get(k) for k in REQUIRED_FIELDS}

#要輸入帳密拿cookie
def _get_credentials() -> tuple[str, str]:
    account = os.getenv("METEO_ACCOUNT", "").strip()
    password = os.getenv("METEO_PASSWORD", "").strip()
    #account = "yolko"
    #password = "yolko123"
    if not account or not password:
        raise ValueError("Missing account or password")
    return account, password

def _parse_iso_utc(s: str) -> datetime:
    if not isinstance(s, str) or not s.strip():
        raise ValueError("detectedAtUtc is not a valid string")
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_detected_at_utc(s: str) -> str:
    dt = _parse_iso_utc(s)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

#登入拿cookie
def login(session: requests.Session, account: str, password: str) -> None:
    resp = session.post(
        LOGIN_URL,
        json={"account": account, "password": password},
        timeout=20,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"Login failed: {resp.status_code} | body: {resp.text[:300]}")

    if ".AspNetCore.Cookies" not in session.cookies:
        raise RuntimeError("Login succeeded but '.AspNetCore.Cookies' not found in session cookies")

#查最新的24筆
def list_latest(session: requests.Session) -> dict:
    body = {
        "page": 0,
        "pageSize": PAGE_SIZE,
        "sortModel": {"items": [{"field": "DetectedAtUtc", "sort": "desc"}]},
        "filterModel": {"items": []},
    }
    resp = session.post(LIST_URL, json=body, timeout=20)
    return resp

#遇 401/403，自動重登一次後再試一次(cookies失效)
def _list_with_relogin_on_401(session: requests.Session, account: str, password: str) -> dict:
    resp = list_latest(session)

    if resp.status_code in (401, 403):
        log_err(f"LIST auth failed ({resp.status_code}). Re-login and retry once...")
        session.cookies.clear()
        login(session, account, password)
        resp = list_latest(session)

    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"LIST failed: {resp.status_code} | body: {resp.text[:300]}")

    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"LIST response is not valid JSON | body: {resp.text[:300]}")

    if not isinstance(data, dict) or "rows" not in data or not isinstance(data["rows"], list):
        raise RuntimeError("LIST response schema unexpected (missing 'rows' list)")

    return data

#建立已存在 detectedAtUtc 的集合
def _build_existing_set_from_rows(rows: list[dict]) -> set[str]:
    existing = set()
    for row in rows:
        s = row.get("detectedAtUtc")
        if not s:
            continue
        try:
            existing.add(_normalize_detected_at_utc(s))
        except Exception:
            continue
    return existing

#成功上傳後抓最新的資料
def fetch_latest_rows(session: requests.Session, page_size: int = 5) -> list[dict]:
    body = {
        "page": 0,
        "pageSize": page_size,
        "sortModel": {"items": [{"field": "DetectedAtUtc", "sort": "desc"}]},
        "filterModel": {"items": []},
    }
    resp = session.post(LIST_URL, json=body, timeout=20)

    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"LIST(latest) failed: {resp.status_code} | body: {resp.text[:300]}")

    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"LIST(latest) response is not valid JSON | body: {resp.text[:300]}")

    rows = data.get("rows", [])
    if not isinstance(rows, list):
        raise RuntimeError("LIST(latest) response schema unexpected (rows is not a list)")
    return rows


def find_uploaded_row_by_detected_at(rows: list[dict], target_detected_at_utc: str) -> dict | None:
    """
    target_detected_at_utc: normalized 'YYYY-MM-DDTHH:MM:SSZ'
    """
    for r in rows:
        s = r.get("detectedAtUtc")
        if not s:
            continue
        try:
            if _normalize_detected_at_utc(s) == target_detected_at_utc:
                return r
        except Exception:
            continue
    return None

#指上傳一次(不能一次上傳多個)
def upload_once(session: requests.Session, payload: dict) -> requests.Response:
    return session.post(UPLOAD_URL, json=payload, timeout=20)

#登入>去掉重複>上傳
def upload_with_relogin_and_dedup(payload: dict) -> dict:
    account, password = _get_credentials()
    target_norm = _normalize_detected_at_utc(payload["detectedAtUtc"])

    with requests.Session() as session:
        #登入
        login(session, account, password)
        #去掉重複
        list_data = _list_with_relogin_on_401(session, account, password)
        existing = _build_existing_set_from_rows(list_data["rows"])
        #已經存在不上傳
        if target_norm in existing:
            return {
                "status": "SKIP_DUPLICATE",
                "reason": "detectedAtUtc already exists",
                "detectedAtUtc": payload["detectedAtUtc"],
                "normalizedDetectedAtUtc": target_norm,
                "checkedLatest": PAGE_SIZE,
            }

        #上傳一次
        resp = upload_once(session, payload)

        #若401/403：重登一次 再次去重複後再決定是否重送
        if resp.status_code in (401, 403):
            log_err(f"UPLOAD auth failed ({resp.status_code}). Re-login and retry once...")
            session.cookies.clear()
            login(session, account, password)

            #再查一次
            list_data = _list_with_relogin_on_401(session, account, password)
            existing = _build_existing_set_from_rows(list_data["rows"])

            if target_norm in existing:
                return {
                    "status": "SKIP_DUPLICATE",
                    "reason": "detectedAtUtc exists after relogin",
                    "detectedAtUtc": payload["detectedAtUtc"],
                    "normalizedDetectedAtUtc": target_norm,
                    "checkedLatest": PAGE_SIZE,
                }

            resp = upload_once(session, payload)

        #錯誤結果
        if not (200 <= resp.status_code < 300):
            raise RuntimeError(f"POST failed: {resp.status_code} {resp.reason} | body: {resp.text[:300]}")

        # 成功：再用 LIST 抓最新幾筆，找出剛上傳那筆（含 id）
        latest_rows = fetch_latest_rows(session, page_size=10)
        uploaded_row = find_uploaded_row_by_detected_at(latest_rows, target_norm)

        return {
            "status": "SUCCESS",
            "normalizedDetectedAtUtc": target_norm,
            "payload": payload,          # 你送出去的資料
            "latestRow": uploaded_row,   # LIST 找到的那筆（通常含 id）
        }


def main()-> int:
    try:
        #抓前一小時資料
        records = fetch_previous_hour_data()

        # 沒抓到>報錯（空品站未上傳）
        if not records:
                log_err("GET 沒有抓到任何資料。判定空品站未順利上傳該小時資料。")
                return 1
        #只上傳一筆
        payload = _validate_allow_null_no_blank(records[0])
        log_info(f"Prepared payload detectedAtUtc={payload.get('detectedAtUtc')}")
        #登入>去掉重複>上傳
        result = upload_with_relogin_and_dedup(payload)
        
        status = result.get("status")
        #skip 重複 但還是算成功
        if status == "SKIP_DUPLICATE":
            log_skip(
                f"Duplicate detectedAtUtc={result.get('normalizedDetectedAtUtc')} "
                f"(checkedLatest={result.get('checkedLatest')}) → skip upload"
            )
            return 0
        #成功上傳資料
        if status == "SUCCESS":
            #log_ok("Upload success")
            # log_info(f"Response={json.dumps(result, ensure_ascii=False)}")
        
            uploaded_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            latest_row = result.get("latestRow") or {}
            row_id = latest_row.get("id") or latest_row.get("Id")  # 有些後端可能用 Id

            log_ok(f"Upload success | uploadedAtUtc={uploaded_at_utc} | id={row_id}")

            #顯示上傳的那筆資料
            if latest_row:
                log_info("Uploaded row (from LIST latest):")
                print(json.dumps(latest_row, ensure_ascii=False, indent=2))
            else:
                log_info("Uploaded row not found in latest list; showing payload instead:")
                print(json.dumps(result.get("payload"), ensure_ascii=False, indent=2))

            if result.get("postResponse") is not None:
                log_info("POST response JSON:")
                print(json.dumps(result["postResponse"], ensure_ascii=False, indent=2))
 
            return 0
        #其他錯誤
        log_err(f"Unexpected result status: {json.dumps(result, ensure_ascii=False)}")
        return 1
    #例外的錯誤?
    except Exception as exc:
        log_err(str(exc))
        return 1

if __name__ == "__main__":
    sys.exit(main())