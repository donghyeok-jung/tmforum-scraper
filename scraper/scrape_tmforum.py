import csv
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

import requests
import pandas as pd

URL = "https://www.tmforum.org/topics/an-resources/"
OUT = Path("data/tmforum_an_validations_latest.csv")

REQUIRED_COLS = [
    "Company",
    "AN Scenario",
    "AN Level",
    "ANLET Version",
    "Network Location",
    "Validation Date",
]

def pick_target_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    for df in tables:
        cols = [str(c).strip() for c in df.columns.tolist()]
        if all(c in cols for c in REQUIRED_COLS):
            return df[REQUIRED_COLS]
    raise RuntimeError("대상 테이블을 찾지 못했습니다(표/컬럼명 변경 가능).")

def main():
    headers = {"User-Agent": "Mozilla/5.0 (tmforum-scraper/1.0)"}
    r = requests.get(URL, headers=headers, timeout=30)
    r.raise_for_status()

    tables = pd.read_html(r.text)  # HTML 내 <table> 파싱
    df = pick_target_table(tables)

    if len(df) < 2:
        raise RuntimeError(f"데이터 row가 2개 미만입니다: {len(df)}")

    # 요구사항: 2번째 '데이터' row의 2~6열 (= AN Scenario..Validation Date)
    vals = df.iloc[1, 1:6].astype(str).tolist()

    ts_kst = datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")

    # 핵심: 항상 1행만 유지 => 매번 파일을 'w'로 새로 씀(덮어쓰기)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["timestamp_kst", "col2", "col3", "col4", "col5", "col6"])
        w.writerow([ts_kst, *vals])

    print("OK:", vals)

if __name__ == "__main__":
    main()
