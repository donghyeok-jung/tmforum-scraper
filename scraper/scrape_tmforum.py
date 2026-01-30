import csv
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from io import StringIO

import pandas as pd
from playwright.sync_api import sync_playwright

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
    # ✅ 403 회피: 브라우저로 렌더링 후 HTML 확보
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            locale="en-US",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        )
        page = context.new_page()
        page.goto(URL, wait_until="networkidle", timeout=60000)
        html = page.content()
        browser.close()

    tables = pd.read_html(StringIO(html))
    df = pick_target_table(tables)

    if len(df) < 2:
        raise RuntimeError(f"데이터 row가 2개 미만입니다: {len(df)}")

    # ✅ 변경 포인트: col1~col6 모두 추출
    vals = df.iloc[1, 0:6].astype(str).tolist()

    ts_kst = datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["timestamp_kst", "col1", "col2", "col3", "col4", "col5", "col6"])
        w.writerow([ts_kst, *vals])

    print("OK:", vals)

if __name__ == "__main__":
    main()
