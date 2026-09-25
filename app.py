# -*- coding: utf-8 -*-
"""
可转债成交额监控 · 增强启动器
================================
首页使用美化版 app.html；后端 API 逻辑完全复用 server.py（不重复实现）。

性能优化（server.py 目前被句柄占用无法直接编辑，故在运行时打补丁）：
  ① /vendor/* 静态资源：24h 强缓存 + gzip  —— echarts.min.js(~1MB) 不再每次重下
  ② 静态页与 JSON 响应 gzip               —— 行情约 315 行，体积降约 85%
  ③ 首页指向美化版 app.html
  ④ K 线缓存（交易时段）30s → 10s：配合前端每 10 秒复核自选池
  ⑤ 新增 /api/klines60：60 分钟 K 线批量接口（供「60下跌」池）
  ⑥ 新增 /api/premium：全市场转股溢价率（供全市场明细表的「溢价率」列）
  ⑦ 新增 /api/redeem：已公布强赎名单（两个下跌池都不收已强赎的债）

运行：python app.py [端口]     默认 8011
访问：http://127.0.0.1:8011
"""
import gzip
import json
import os
import re
import sys
import time
import threading
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import server  # noqa: E402  复用其数据源/连接池/缓存/批量接口

BASE = os.path.dirname(os.path.abspath(server.__file__))
PAGE = os.path.join(BASE, "app.html")
_GZ_MIN = 1024                     # 小于 1KB 不压缩（省得白忙）

# ==================== ① 首页 → 美化版 app.html ====================
_orig_do_GET = server.Handler.do_GET


def _do_GET(self):
    path_only = urllib.parse.urlparse(self.path).path
    if path_only in ("/", "/index.html"):
        fp = PAGE if os.path.isfile(PAGE) else os.path.join(BASE, "index.html")
        self._serve_file(fp, "text/html; charset=utf-8")
        return
    if path_only.startswith("/api/klines60"):
        self._api_klines60()
        return
    if path_only.startswith("/api/premium"):
        self._api_premium()
        return
    if path_only.startswith("/api/redeem"):
        self._api_redeem()
        return
    _orig_do_GET(self)


server.Handler.do_GET = _do_GET

# ==================== ⑤ 60 分钟 K 线（/api/klines60）====================
_K60_URL = "https://ifzq.gtimg.cn/appstock/app/kline/mkline?param={symbol},m60,,320"
_K60_TTL_TRADING = 60          # 60 分钟 K 线一小时才变一次，缓存 60s 足够
_K60_TTL_IDLE = 300            # 非交易时段（含午休）数据不变，缓存 5 分钟
_K60_ERR_TTL = 10              # 失败负缓存 10s：偶发失败别被冻住 5 分钟
_K60_RETRY = 2                 # 单只失败重试次数（腾讯偶发 Remote end closed）
_k60_cache = {}
_k60_lock = threading.Lock()
_k60_exec = ThreadPoolExecutor(max_workers=8, thread_name_prefix="k60")


def _k60_ttl():
    return _K60_TTL_TRADING if server.is_trading() else _K60_TTL_IDLE


def _k60_get(url):
    """取 m60 原文，带重试：腾讯偶发「Remote end closed connection without response」。"""
    last = None
    for attempt in range(_K60_RETRY + 1):
        try:
            return server.http_get(url, referer="https://gu.qq.com/").decode("utf-8", "ignore")
        except Exception as e:
            last = e
            if attempt < _K60_RETRY:
                time.sleep(0.3 * (attempt + 1))
    raise last


def _fetch_kline60_uncached(code):
    sym = server.symbol_of(code)
    try:
        raw = _k60_get(_K60_URL.format(symbol=sym))
        node = (json.loads(raw).get("data") or {}).get(sym) or {}
        arr = []
        for r in node.get("m60") or []:
            if not isinstance(r, list) or len(r) < 6:
                continue
            arr.append({"day": server._fmt_tc_time(r[0]), "open": r[1], "close": r[2],
                        "high": r[3], "low": r[4], "volume": r[5]})
        built = server._kline_from_arr(arr) if arr else None
        if not built:
            raise RuntimeError("empty m60")
    except Exception as e:
        return {"code": code, "error": "60分钟K线获取失败: %s" % e}
    with server._lock:
        name = server._cb_name.get(code, "")
    return {"code": code, "name": name, **built, "ts": time.strftime("%Y-%m-%d %H:%M:%S")}


def _fetch_kline60(code):
    now = time.time()
    with _k60_lock:
        it = _k60_cache.get(code)
    if it:
        # 成功结果用正常 TTL；失败结果只用 10s 负缓存（否则一次偶发抖动会被冻住 5 分钟）
        ttl = _K60_ERR_TTL if it["data"].get("error") else _k60_ttl()
        if now - it["ts"] < ttl:
            return it["data"]

    data = _fetch_kline60_uncached(code)

    if data.get("error"):
        # 拉失败时：手里若还有上一次的【成功】数据，就继续用它，别让图表变成报错；
        # 且不刷新 ts，下一次请求会立刻重试。
        if it and not it["data"].get("error"):
            return it["data"]
    with _k60_lock:
        _k60_cache[code] = {"ts": time.time(), "data": data}
    return data


def fetch_klines60(codes, tail=0):
    uniq, seen = [], set()
    for c in codes:
        if c and c not in seen:
            seen.add(c)
            uniq.append(c)
    out = {}
    futs = {c: _k60_exec.submit(_fetch_kline60, c) for c in uniq}
    for c, fut in futs.items():
        try:
            out[c] = server._tail_slice(fut.result(timeout=server.HTTP_TIMEOUT + 10), tail)
        except Exception as e:
            out[c] = {"code": c, "error": str(e)}
    return out


def _send_json(self, code, obj):
    self._send(code, server._json(obj), "application/json; charset=utf-8")


def _query_args(self, limit=60):
    """解析 /api/klines*?codes=a,b&tail=N → (codes, tail)"""
    q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
    raw = (q.get("codes") or [""])[0]
    try:
        tail = max(0, min(320, int((q.get("tail") or ["0"])[0] or 0)))
    except ValueError:
        tail = 0
    codes = [c for c in re.split(r"[,\s]+", raw) if server._CODE_RE.match(c)][:limit]
    return codes, tail


def _api_klines60(self):
    codes, tail = _query_args(self)
    if not codes:
        self._send_json(400, {"error": "no valid codes"})
        return
    self._send_json(200, fetch_klines60(codes, tail))


server.Handler._send_json = _send_json
server.Handler._api_klines60 = _api_klines60

# ==================== ⑥ 转股溢价率（东财按代码批量）====================
# 东财 push2 的 ulist.np 接口支持按代码批量取，返回：
#   f12=债券代码  f235=转股价  f236=转股价值  f237=转股溢价率(%)
# 用「板块列表」接口( clist/fs=b:MK0354 )单页上限只有 100 条，凑不齐 315 只，故改用按代码批量。
# 溢价率随正股实时变动，交易时段缓存 60s（上游对连续调用敏感，别调太快）。
_PREM_URL = ("https://push2.eastmoney.com/api/qt/ulist.np/get"
             "?fltt=2&invt=2&fields=f12,f235,f236,f237&secids={secids}")
_PREM_BATCH = 120                 # 单次请求的代码数（URL 长度可控）
_PREM_TTL_TRADING = 60
_PREM_TTL_IDLE = 300
_prem_cache = {"ts": 0.0, "data": {}}
_prem_lock = threading.Lock()
_prem_exec = ThreadPoolExecutor(max_workers=4, thread_name_prefix="prem")


def _em_secid(sym):
    """server 的 'sh118058' / 'sz123091' → 东财的 '1.118058' / '0.123091'"""
    return ("1." if sym.startswith("sh") else "0.") + sym[2:]


def _premium_batch(secids):
    raw = server.http_get(_PREM_URL.format(secids=",".join(secids)),
                          referer="https://quote.eastmoney.com/")
    diff = ((json.loads(raw.decode("utf-8", "ignore")).get("data") or {}).get("diff") or [])
    got = {}
    for r in diff:
        code = str(r.get("f12") or "")
        pr = r.get("f237")
        if server._CODE_RE.match(code) and pr is not None:
            got[code] = {"pr": pr, "cv": r.get("f236"), "cp": r.get("f235")}
    return got


def _fetch_premium_uncached():
    with server._lock:
        lst = list(server._cb_list)
    if not lst:                            # 服务刚启动、行情还没刷新过时兜底
        lst = server.load_cb_list()
    secids = [_em_secid(b["secid"]) for b in lst if b.get("secid")]
    if not secids:
        raise RuntimeError("存续可转债清单为空")
    batches = [secids[i:i + _PREM_BATCH] for i in range(0, len(secids), _PREM_BATCH)]
    out = {}
    if len(batches) == 1:
        out.update(_premium_batch(batches[0]))
    else:
        for fut in [_prem_exec.submit(_premium_batch, b) for b in batches]:
            out.update(fut.result(timeout=server.HTTP_TIMEOUT + 10))
    if not out:
        raise RuntimeError("empty premium list")
    return out


def fetch_premium():
    """全市场转股溢价率 {code: {pr, cv, cp}}；带缓存，上游异常时回落到上次结果。"""
    now = time.time()
    with _prem_lock:
        ttl = _PREM_TTL_TRADING if server.is_trading() else _PREM_TTL_IDLE
        if _prem_cache["data"] and now - _prem_cache["ts"] < ttl:
            return _prem_cache["data"]
    try:
        data = _fetch_premium_uncached()
    except Exception:
        with _prem_lock:
            if _prem_cache["data"]:          # 降级：用上一次的结果，别让前端整列变 --
                return _prem_cache["data"]
        raise
    with _prem_lock:
        _prem_cache["ts"] = time.time()
        _prem_cache["data"] = data
    return data


def _api_premium(self):
    try:
        self._send_json(200, {"ts": time.strftime("%H:%M:%S"), "data": fetch_premium()})
    except Exception as e:
        self._send_json(200, {"ts": "", "data": {}, "error": str(e)})


server.Handler._api_premium = _api_premium

# ==================== ⑧ 已公告强赎名单（/api/redeem）====================
# 判据：未退市 且 出现「赎回」系列字段（SH）任一项。
#   东财字段里 SH=赎回、HS=回售，两者千万别混（回售不算强赎）。
# 实测对照：胜蓝转02(123258) 回售登记日 2026-10-09/回售价 100.05/回售执行日 2026-10-12 -> 命中；
#           长海转债(123091) 同名字段全空、只有 HS（回售）有值 -> 不命中。
_REDEEM_URL = ("https://datacenter-web.eastmoney.com/api/data/v1/get"
               "?reportName=RPT_BOND_CB_LIST"
               "&columns=SECURITY_CODE,SECURITY_NAME_ABBR,DELIST_DATE,NOTICE_DATE_SH,"
               "RECORD_DATE_SH,EXECUTE_START_DATESH,EXECUTE_PRICE_SH,EXECUTE_REASON_SH"
               "&pageNumber={page}&pageSize=500&sortColumns=SECURITY_CODE&sortTypes=1"
               "&source=WEB&client=WEB")
_REDEEM_TTL = 1800                 # 强赎公告不频繁，服务端缓存 30 分钟
_redeem_cache = {"ts": 0.0, "data": {}}
_redeem_lock = threading.Lock()


def _nz(v):
    return v not in (None, "", "None")


def _is_redeeming(r):
    """已公告强赎且尚未退市"""
    if _nz(r.get("DELIST_DATE")):
        return False
    if str(r.get("EXECUTE_REASON_SH") or "") in ("4", "5"):   # 4/5=强制赎回（刚公告时只有这个）
        return True
    return (_nz(r.get("RECORD_DATE_SH"))          # 赎回登记日
            or _nz(r.get("EXECUTE_PRICE_SH"))     # 赎回价格
            or _nz(r.get("EXECUTE_START_DATESH")))  # 赎回执行日


def _fet_redeem_uncached():
    out, page = {}, 1
    while True:
        raw = server.http_get(_REDEEM_URL.format(page=page), referer="https://data.eastmoney.com/")
        res = json.loads(raw.decode("utf-8", "ignore")).get("result") or {}
        for r in res.get("data") or []:
            code = str(r.get("SECURITY_CODE") or "")
            if not server._CODE_RE.match(code) or not _is_redeeming(r):
                continue
            out[code] = {
                "name": r.get("SECURITY_NAME_ABBR") or "",
                "notice": str(r.get("NOTICE_DATE_SH") or "")[:10],
                "record": str(r.get("RECORD_DATE_SH") or "")[:10],
                "price": r.get("EXECUTE_PRICE_SH"),
                "exec": str(r.get("EXECUTE_START_DATESH") or "")[:10],
            }
        if page >= (res.get("pages") or 1) or page >= 8:
            break
        page += 1
    if not out:
        raise RuntimeError("empty redeem list")
    return out


def fetch_redeem():
    """{code: {name,notice,record,price,exec}}；上游异常时回落到上次结果。"""
    now = time.time()
    with _redeem_lock:
        if _redeem_cache["data"] and now - _redeem_cache["ts"] < _REDEEM_TTL:
            return _redeem_cache["data"]
    try:
        data = _fet_redeem_uncached()
    except Exception:
        with _redeem_lock:
            if _redeem_cache["data"]:
                return _redeem_cache["data"]
        raise
    with _redeem_lock:
        _redeem_cache["ts"] = time.time()
        _redeem_cache["data"] = data
    return data


def _api_redeem(self):
    try:
        data = fetch_redeem()
        self._send_json(200, {"ts": time.strftime("%H:%M:%S"), "count": len(data), "data": data})
    except Exception as e:
        self._send_json(200, {"ts": "", "count": 0, "data": {}, "error": str(e)})


server.Handler._api_redeem = _api_redeem

# ==================== ② 响应 gzip（文本类，含 HTML/JSON）====================
_orig_send = server.Handler._send


def _accepts_gzip(headers):
    try:
        return "gzip" in (headers.get("Accept-Encoding") or "").lower()
    except Exception:
        return False


def _gzip_if_accepted(data, extra, headers, level=6):
    """体积 >=1KB 且客户端接受 gzip 时才压缩；返回 (data, extra)"""
    if len(data) < _GZ_MIN or not _accepts_gzip(headers):
        return data, extra
    extra = dict(extra or {})
    extra["Content-Encoding"] = "gzip"
    extra["Vary"] = "Accept-Encoding"
    return gzip.compress(data, level), extra


def _send(self, code, body, ctype, extra=None):
    if isinstance(body, (bytes, bytearray)) and "javascript" not in ctype and "image" not in ctype:
        body, extra = _gzip_if_accepted(bytes(body), extra, self.headers, level=5)
    return _orig_send(self, code, body, ctype, extra)


server.Handler._send = _send

# ==================== ③ vendor 静态资源：长缓存 + gzip ====================
def _serve_vendor(self, base, path_only):
    name = os.path.basename(path_only)
    fp = os.path.join(base, "vendor", name)
    if not os.path.isfile(fp):
        self._send(404, b"not found", "text/plain; charset=utf-8")
        return
    with open(fp, "rb") as f:
        data = f.read()
    data, extra = _gzip_if_accepted(data, {"Cache-Control": "public, max-age=86400"}, self.headers)
    self.send_response(200)
    self.send_header("Content-Type", "application/javascript; charset=utf-8")
    self.send_header("Content-Length", str(len(data)))
    for k, v in extra.items():
        self.send_header(k, v)
    self.end_headers()
    self.wfile.write(data)


server.Handler._serve_vendor = _serve_vendor

# ==================== ④ 交易时段 K 线缓存收紧到 10 秒 ====================
# 自选池每 10 秒复核一轮，缓存同步压到 10 秒，让刚收盘的那根 K 线尽快进入判定。
# （server._kline_ttl() 每次都读这个模块级全局量，改它即可，无需改被占用的 server.py）
server.KLINE_TTL_TRADING = 10


if __name__ == "__main__":
    server.main()
