import os
import csv
from io import StringIO
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

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

def read_previous_vals() -> list[str] | None:
    """기존 CSV가 있으면 col1~col6만 읽어서 반환. 없으면 None."""
    if not OUT.exists():
        return None

    with OUT.open("r", encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        _header = next(r, None)
        row = next(r, None)

    if not row or len(row) < 7:
        return None

    return row[1:7]  # col1~col6

def write_github_outputs(**kvs):
    out_path = os.environ.get("GITHUB_OUTPUT")
    if not out_path:
        return
    with open(out_path, "a", encoding="utf-8") as f:
        for k, v in kvs.items():
            f.write(f"{k}={v}\n")

def main():
    prev_vals = read_previous_vals()

    # 1) Playwright 렌더링(403 회피)
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

    # 2) 표 파싱
    tables = pd.read_html(StringIO(html))
    df = pick_target_table(tables)

    if len(df) < 2:
        raise RuntimeError(f"데이터 row가 2개 미만입니다: {len(df)}")

    # 3) 오늘 값(col1~col6)
    vals = df.iloc[1, 0:6].astype(str).tolist()

    # 4) 변경 판단: timestamp는 제외하고 col1~col6만 비교
    changed = (prev_vals is None) or (prev_vals != vals)

    # 5) CSV는 항상 최신 1행으로 덮어쓰기
    ts_kst = datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["timestamp_kst", "col1", "col2", "col3", "col4", "col5", "col6"])
        w.writerow([ts_kst, *vals])

    # 6) Actions output
    write_github_outputs(
        changed=("true" if changed else "false"),
        ts_kst=ts_kst,
        summary=" | ".join(vals),
    )

    print("OK:", vals, "changed=", changed)

if __name__ == "__main__":
    main()
