#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
生成存续可转债快照 cb_list.json（页面「快照优先」用的兜底清单）

用法：
    python tools/gen_cb_list.py            # 生成并覆盖 cb_monitor/cb_list.json
    python tools/gen_cb_list.py --dry-run  # 只统计，不写文件

口径（必须与 app.html / server.py 保持一致）：
  · 来源：东财 datacenter RPT_BOND_CB_LIST，只取 4 列（代码/名称/正股/退市日）
    —— 别用 columns=ALL：单页 3.1MB，这里只用得到 4 个字段（60KB/页）。
  · 剔除 DELIST_DATE 非空的（已退市）。
  · 剔除 404xxx（三板/退市债）：腾讯行情与 5 分钟 K 线都不返回，留着只会让
    「清单条数」与「行情条数」永远对不上。
  · 输出格式：{"date": "YYYY-MM-DD", "note": "...", "list": [[code, name, stock], ...]}
    页面 fetchCbListSnapshot() 就是按这个格式读的，改格式必须同步改页面。

⚠️ 生成后要 cp app.html index.html？—— 不需要，本文件不影响页面；但它会被一起发布，
   所以改完记得重新发布（否则线上仍是旧快照；不过页面每 20 分钟会自己用远程清单校准）。
"""
import argparse
import datetime
import json
import os
import sys
import time
import urllib.parse
import urllib.request

COLS = "SECURITY_CODE,SECURITY_NAME_ABBR,CONVERT_STOCK_CODE,DELIST_DATE"
BASE = ("https://datacenter-web.eastmoney.com/api/data/v1/get"
        "?reportName=RPT_BOND_CB_LIST&columns=" + urllib.parse.quote(COLS, safe="") +
        "&pageSize=500&sortColumns=SECURITY_CODE&sortTypes=1&source=WEB&client=WEB")
PAGE_LIMIT = 6          # 兜底：别无限翻页
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cb_list.json")


def fetch_all():
    """翻页取全清单原始行。返回 (rows, count)。用 ProxyHandler({}) 绕开系统代理。"""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    rows, count, page = [], None, 1
    while page <= PAGE_LIMIT:
        req = urllib.request.Request(
            BASE + "&pageNumber=%d" % page,
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://data.eastmoney.com/"})
        payload = json.load(opener.open(req, timeout=30))
        result = payload.get("result") or {}
        batch = result.get("data") or []
        if count is None:
            count = result.get("count")
        if not batch:
            break
        rows.extend(batch)
        print("  第 %d 页 +%d 条（累计 %d / 共 %s）" % (page, len(batch), len(rows), count))
        if count and len(rows) >= count:
            break
        page += 1
        time.sleep(0.3)
    return rows, count


def build(rows):
    """过滤 → 排序 → 输出结构。"""
    alive = [r for r in rows
             if not r.get("DELIST_DATE") and not str(r.get("SECURITY_CODE", "")).startswith("404")]
    lst = sorted(([r["SECURITY_CODE"], r.get("SECURITY_NAME_ABBR") or "",
                   r.get("CONVERT_STOCK_CODE") or ""] for r in alive), key=lambda x: x[0])
    return {"date": datetime.date.today().isoformat(),
            "note": "存续可转债快照（东财 RPT_BOND_CB_LIST 裁剪列，已剔退市与 404xxx 三板债）。"
                    "静态直连模式下的清单兜底：页面先读它，再后台用远程补全。",
            "list": lst}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="只统计，不写文件")
    ap.add_argument("--out", default=OUT, help="输出路径（默认项目根的 cb_list.json）")
    args = ap.parse_args()

    print("拉取东财清单…")
    rows, count = fetch_all()
    data = build(rows)
    print("原始 %d 条（count=%s）→ 存续 %d 只" % (len(rows), count, len(data["list"])))

    if args.dry_run:
        print("--dry-run：不写文件")
        return 0
    text = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(text)
    print("已写入 %s（%d 字节，日期 %s）" % (args.out, len(text.encode("utf-8")), data["date"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
