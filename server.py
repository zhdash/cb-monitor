# -*- coding: utf-8 -*-
"""
可转债成交额监控服务（Top20 + 5分钟K线/布林线）
================================================
数据源（均为免费公开接口）：
  1) 东方财富 datacenter RPT_BOND_CB_LIST —— 存续可转债清单（分页拉全，过滤 DELIST_DATE 为空）
  2) 腾讯行情 qt.gtimg.cn —— 实时行情（批量查询，稳/不限流，涨跌幅口径与东财/同花顺一致）
  3) 腾讯K线 ifzq.gtimg.cn/appstock/app/kline/mkline —— 5分钟K线（主力，与腾讯自选股一致）
  4) 东方财富 push2his —— 5分钟K线备用（腾讯失败时兜底），后端计算标准布林线(MA20±2σ)
  注：新浪数据源已弃用（与腾讯/东财/同花顺口径不一致，存在偏差）

性能要点：
  - 自带 HTTP(S) 连接池：复用 TCP/TLS 连接，省掉每次请求的握手开销（纯标准库，无需 requests）
  - 行情批量请求并发化：4~5 个批次并行拉取，刷新耗时明显下降
  - /api/klines 批量K线：自动盯盘扫描从 20+ 次请求压缩到 1 次
  - quotes 载荷精简：只下发前端真正用到的字段

运行：python server.py [端口]   （默认 8011）
访问：http://127.0.0.1:8011
"""
import datetime
import gzip
import http.client
import json
import os
import re
import ssl
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ==================== 配置 ====================
HOST = "127.0.0.1"
PORT = 8011
REFRESH_SEC = 15          # 兜底循环刷新周期（秒）
STALE_SEC = 4             # 交易时段内快照过期阈值：超过即触发按需刷新
BATCH_SIZE = 80           # 腾讯每批查询只数（腾讯对单次批量长度较敏感，保守取值）
CB_LIST_CACHE_HOURS = 6   # 可转债清单缓存时长（小时）
HTTP_TIMEOUT = 12         # 单次外网请求超时（秒）
MAX_WORKERS = 8           # 外网请求并发上限
POOL_PER_HOST = 4         # 每主机保留的空闲连接数

KLINE_TTL_TRADING = 30    # 交易时段K线缓存（秒）
KLINE_TTL_IDLE = 300      # 非交易时段K线缓存（秒）
KLINE_ERR_TTL = 10        # K线失败负缓存（秒），避免异常时反复打上游
BOLL_PERIOD = 20          # 标准布林线周期
BOLL_MULT = 2.0           # 标准布林线标准差倍数

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")

EAST_URL = ("https://datacenter-web.eastmoney.com/api/data/v1/get"
            "?reportName=RPT_BOND_CB_LIST&columns=ALL"
            "&pageNumber={page}&pageSize=500"
            "&sortColumns=SECURITY_CODE&sortTypes=1&source=WEB&client=WEB")
TENCENT_URL = "https://qt.gtimg.cn/q={codes}"
TENCENT_KLINE_URL = ("https://ifzq.gtimg.cn/appstock/app/kline/mkline"
                     "?param={symbol},m5,,320")
EAST_KLINE_URL = ("https://push2his.eastmoney.com/api/qt/stock/kline/get"
                  "?secid={secid}&klt=5&fqt=0&beg=0&end=20500101&lmt=320"
                  "&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57")


# ==================== HTTP 层：连接池 + 重试 + 自动解压 ====================
class ConnPool:
    """极简 HTTP(S) 连接池：按 (scheme, netloc) 复用连接，避免重复握手。"""

    def __init__(self, per_host=POOL_PER_HOST):
        self._lock = threading.Lock()
        self._free = {}          # key -> [conn, ...]
        self._per_host = per_host

    @staticmethod
    def _connect(scheme, netloc, timeout):
        host, _, port_s = netloc.partition(":")
        if scheme == "https":
            port = int(port_s) if port_s else 443
            return http.client.HTTPSConnection(
                host, port, timeout=timeout, context=ssl.create_default_context())
        port = int(port_s) if port_s else 80
        return http.client.HTTPConnection(host, port, timeout=timeout)

    def get(self, scheme, netloc, timeout):
        key = (scheme, netloc)
        with self._lock:
            bucket = self._free.get(key)
            if bucket:
                return bucket.pop()
        return self._connect(scheme, netloc, timeout)

    def put(self, scheme, netloc, conn):
        key = (scheme, netloc)
        with self._lock:
            bucket = self._free.setdefault(key, [])
            if len(bucket) < self._per_host:
                bucket.append(conn)
                return
        self._close(conn)

    @staticmethod
    def _close(conn):
        try:
            conn.close()
        except Exception:
            pass

    def drop(self, scheme, netloc, conn):
        self._close(conn)

    def clear(self):
        with self._lock:
            for bucket in self._free.values():
                for c in bucket:
                    self._close(c)
            self._free.clear()


_POOL = ConnPool()


def _http_get_once(url, referer=None, timeout=HTTP_TIMEOUT, _depth=0):
    """单次 GET：走连接池，自动解压 gzip，跟随重定向。返回 bytes。"""
    parts = urllib.parse.urlsplit(url)
    scheme = parts.scheme or "https"
    netloc = parts.netloc
    path = parts.path or "/"
    if parts.query:
        path += "?" + parts.query

    headers = {
        "User-Agent": UA,
        "Accept": "*/*",
        "Accept-Encoding": "gzip",
        "Connection": "keep-alive",
    }
    if referer:
        headers["Referer"] = referer

    conn = _POOL.get(scheme, netloc, timeout)
    try:
        conn.request("GET", path, headers=headers)
        resp = conn.getresponse()
        body = resp.read()
        status = resp.status
        location = resp.getheader("Location")
        encoding = (resp.getheader("Content-Encoding") or "").lower()
        keep = not resp.will_close
    except Exception:
        _POOL.drop(scheme, netloc, conn)
        raise

    if status in (301, 302, 303, 307, 308) and location and _depth < 3:
        if keep:
            _POOL.put(scheme, netloc, conn)
        else:
            _POOL.drop(scheme, netloc, conn)
        return _http_get_once(urllib.parse.urljoin(url, location), referer,
                              timeout, _depth + 1)

    if status != 200:
        _POOL.drop(scheme, netloc, conn)
        raise RuntimeError(f"HTTP {status} {netloc}")

    if "gzip" in encoding:
        try:
            body = gzip.decompress(body)
        except Exception:
            pass

    if keep:
        _POOL.put(scheme, netloc, conn)
    else:
        _POOL.drop(scheme, netloc, conn)
    return body


def http_get(url, referer=None, timeout=HTTP_TIMEOUT, retries=2):
    """带重试的 GET。失败会丢弃该连接，避免坏连接被复用。"""
    last = None
    for attempt in range(retries + 1):
        try:
            return _http_get_once(url, referer, timeout)
        except Exception as e:
            last = e
            if attempt < retries:
                time.sleep(0.25 * (attempt + 1))
    raise RuntimeError(f"请求失败 {url}: {last}")


# ==================== 全局状态 ====================
_lock = threading.Lock()
_refreshing = threading.Lock()          # 同一时刻只允许一次行情刷新
_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="io")

_cb_list = []            # [{code, name, stock, secid}]
_cb_list_ts = 0.0
_cb_name = {}            # code -> name（K线接口取名字用，避免每次线性查找）
_quotes = []             # 全市场行情快照（按成交额降序）
_quotes_ts = 0.0
_last_error = ""
_kline_cache = {}        # code -> {"ts": float, "data": dict}


# ==================== 交易时段 ====================
def market_status():
    """A股可转债交易状态（可转债与A股交易时段一致）"""
    now = datetime.datetime.now()
    if now.weekday() >= 5:
        return "休市"
    t = now.strftime("%H:%M")
    if "09:15" <= t <= "11:30" or "13:00" <= t <= "15:00":
        return "交易中"
    if "11:30" < t < "13:00":
        return "午间休市"
    if "09:00" <= t < "09:15":
        return "盘前"
    return "已收盘"


def is_trading():
    return market_status() == "交易中"


# ==================== 可转债清单 ====================
def load_cb_list():
    """分页拉全东财可转债清单，过滤出存续债（DELIST_DATE 为空）"""
    rows, page, count = [], 1, None
    while True:
        data = json.loads(http_get(EAST_URL.format(page=page),
                                   referer="https://data.eastmoney.com/kzz/"))
        res = data.get("result") or {}
        batch = res.get("data") or []
        if not batch:
            break
        if count is None:
            count = res.get("count")
        rows.extend(batch)
        if (count and len(rows) >= count) or len(batch) < 500:
            break
        page += 1

    alive = []
    for r in rows:
        if r.get("DELIST_DATE"):       # 有退市日期 = 已退市
            continue
        code = r["SECURITY_CODE"]
        alive.append({
            "code": code,
            "name": r["SECURITY_NAME_ABBR"],
            "stock": r.get("CONVERT_STOCK_CODE") or "",
            "secid": ("sh" if code.startswith("11") else "sz") + code,
        })
    return alive


# ==================== 腾讯行情 ====================
def fetch_tencent(secids):
    """批量查询腾讯行情，返回 {secid: 原始字符串}"""
    url = TENCENT_URL.format(codes=urllib.parse.quote(",".join(secids), safe=","))
    raw = http_get(url, referer="https://gu.qq.com/").decode("gbk", "ignore")
    out = {}
    for m in re.finditer(r'v_(\w+)="([^"]*)"', raw):
        out[m.group(1)] = m.group(2)
    return out


def parse_tencent_quote(raw):
    """解析腾讯行情 ~ 分隔字段 → 结构化 dict"""
    f = raw.split("~")
    if len(f) < 40:
        return None

    def num(i):
        try:
            v = f[i].strip()
            return float(v) if v not in ("", "-") else None
        except (ValueError, IndexError):
            return None

    return {
        "name": f[1],
        "price": num(3),
        "prev_close": num(4),
        "open": num(5),
        "high": num(33),
        "low": num(34),
        "volume": num(6),
        "amount_wan": num(37),
        "change": num(31),
        "change_pct": num(32),
        "turnover": num(38),
        "time": f[30],
    }


def _fetch_batch(secids):
    """获取一批腾讯行情（失败重试一次）；返回 {secid: quote_dict}。"""
    try:
        raw_map = fetch_tencent(secids)
    except Exception:
        time.sleep(0.4)
        raw_map = fetch_tencent(secids)

    out = {}
    for secid, raw in raw_map.items():
        q = parse_tencent_quote(raw)
        if q and q.get("amount_wan") is not None:
            out[secid] = q
    return out


def refresh_once():
    """刷新一次行情快照：清单缓存 → 并发批量行情 → 排序"""
    global _cb_list, _cb_list_ts, _cb_name, _quotes, _quotes_ts, _last_error

    # 1) 可转债清单（带缓存）
    if not _cb_list or time.time() - _cb_list_ts > CB_LIST_CACHE_HOURS * 3600:
        lst = load_cb_list()
        if lst:
            with _lock:
                _cb_list = lst
                _cb_name = {b["code"]: b["name"] for b in lst}
                _cb_list_ts = time.time()
    with _lock:
        lst = list(_cb_list)

    # 2) 批量行情（并发拉取，显著缩短刷新耗时）
    secids = [b["secid"] for b in lst]
    secid2meta = {b["secid"]: b for b in lst}
    batches = [secids[i:i + BATCH_SIZE] for i in range(0, len(secids), BATCH_SIZE)]

    results = {}
    if len(batches) == 1:
        results.update(_fetch_batch(batches[0]))
    else:
        futures = [_executor.submit(_fetch_batch, b) for b in batches]
        for fut in futures:
            results.update(fut.result(timeout=HTTP_TIMEOUT + 10))

    # 3) 组装（只保留前端真正用到的字段，压缩载荷）
    rows = []
    for secid, q in results.items():
        meta = secid2meta.get(secid, {})
        rows.append({
            "code": meta.get("code", ""),
            "name": q["name"] or meta.get("name", ""),
            "stock": meta.get("stock", ""),
            "price": q["price"],
            "change": q["change"],
            "change_pct": q["change_pct"],
            "amount_wan": q["amount_wan"],
            "volume": q["volume"],
            "high": q["high"],
            "low": q["low"],
        })

    rows.sort(key=lambda x: x["amount_wan"] or 0, reverse=True)
    with _lock:
        _quotes = rows
        _quotes_ts = time.time()
        _last_error = ""
    print(f"[ok] {datetime.datetime.now():%H:%M:%S} 行情刷新(腾讯): "
          f"{len(rows)} 只有效数据", flush=True)


def _refresh_guarded():
    """带互斥保护的刷新：同一时刻只有一个线程在拉行情"""
    global _last_error
    if not _refreshing.acquire(blocking=False):
        return False
    try:
        refresh_once()
        return True
    except Exception as e:
        _last_error = str(e)
        print(f"[err] {datetime.datetime.now():%H:%M:%S} 刷新失败: {e}", flush=True)
        return False
    finally:
        _refreshing.release()


def refresh_loop():
    """兜底循环：无论有没有页面在看，都定期刷新"""
    while True:
        _refresh_guarded()
        time.sleep(REFRESH_SEC)


def maybe_refresh_async():
    """交易时段内，快照超过 STALE_SEC 未更新 → 异步触发一次刷新。
    让前端设置 3s/5s 刷新间隔时，拿到的是真·准实时数据。"""
    if not is_trading():
        return
    with _lock:
        stale = (time.time() - _quotes_ts) > STALE_SEC
    if not stale:
        return
    _executor.submit(_refresh_guarded)


def build_payload():
    with _lock:
        rows = list(_quotes)          # 返回全市场（约300只），前端负责排序/搜索/取前20
        ts = _quotes_ts
        total = len(_quotes)
        err = _last_error
    return {
        "ts": datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else "",
        "ts_raw": int(ts),
        "market": market_status(),
        "total": total,
        "source": "腾讯行情" if total else "加载中",
        "error": err,
        "rows": rows,
    }


# ==================== 5分钟K线 + 布林线 ====================
def symbol_of(code):
    """可转债代码 → 腾讯 symbol（深市12x→sz，沪市11x→sh）"""
    return ("sz" if code.startswith("12") else "sh") + code


def secid_of(code):
    """可转债代码 → 东财secid（深市12x→market 0，沪市11x→market 1）"""
    return ("0." if code.startswith("12") else "1.") + code


def calc_boll(closes, period=BOLL_PERIOD, mult=BOLL_MULT):
    """标准布林线：MID = SMA(period)，UPPER/LOWER = MID ± mult×STD(period)
    STD 取总体标准差（ddof=0）；前 period-1 根无值返回 None"""
    n = len(closes)
    mid, upper, lower = [None] * n, [None] * n, [None] * n
    for i in range(period - 1, n):
        win = closes[i - period + 1:i + 1]
        m = sum(win) / period
        sd = (sum((x - m) ** 2 for x in win) / period) ** 0.5
        mid[i] = round(m, 4)
        upper[i] = round(m + mult * sd, 4)
        lower[i] = round(m - mult * sd, 4)
    return mid, upper, lower


def _fmt_tc_time(t):
    """腾讯K线时间 202608261455 → 2026-08-26 14:55"""
    t = str(t)
    if len(t) >= 12:
        return f"{t[0:4]}-{t[4:6]}-{t[6:8]} {t[8:10]}:{t[10:12]}"
    return t


def _parse_tencent_kline(raw, symbol):
    """解析腾讯 mkline → [{day, open, high, low, close, volume}, ...]
    原始每项：[时间YYYYMMDDHHMM, 开, 收, 高, 低, 成交量(手), ...]"""
    d = json.loads(raw)
    node = (d.get("data") or {}).get(symbol) or {}
    out = []
    for r in node.get("m5") or []:
        if not isinstance(r, list) or len(r) < 6:
            continue
        out.append({"day": _fmt_tc_time(r[0]), "open": r[1], "close": r[2],
                    "high": r[3], "low": r[4], "volume": r[5]})
    return out


def _kline_from_arr(arr):
    """OHLCV 数组 → ECharts 渲染数据 + 布林线"""
    times, closes, klines, vols = [], [], [], []
    for k in arr:
        try:
            o, c, h, l = (float(k["open"]), float(k["close"]),
                          float(k["high"]), float(k["low"]))
        except (KeyError, ValueError, TypeError):
            continue
        times.append(k["day"])
        klines.append([o, c, l, h])        # ECharts candlestick: [open, close, low, high]
        closes.append(c)
        vols.append(float(k.get("volume") or 0))
    if not times:
        return None
    mid, upper, lower = calc_boll(closes)
    return {
        "times": times,
        "kline": klines,
        "vol": vols,
        "boll": {"mid": mid, "upper": upper, "lower": lower},
    }


def _tail_slice(d, n):
    """只保留最后 n 根（自动盯盘只需尾部少量K线，用于压缩批量接口载荷）"""
    if not n or not d or not d.get("kline"):
        return d
    length = len(d["kline"])
    if length <= n:
        return d
    s = length - n
    out = dict(d)
    out["times"] = d["times"][s:]
    out["kline"] = d["kline"][s:]
    out["vol"] = d["vol"][s:]
    boll = d.get("boll") or {}
    out["boll"] = {k: (v[s:] if v else v) for k, v in boll.items()}
    return out


def _kline_ttl():
    return KLINE_TTL_TRADING if is_trading() else KLINE_TTL_IDLE


def _kline_cached(code):
    with _lock:
        item = _kline_cache.get(code)
        if not item:
            return None
        ttl = KLINE_ERR_TTL if item["data"].get("error") else _kline_ttl()
        if time.time() - item["ts"] < ttl:
            return item["data"]
    return None


def _kline_store(code, payload):
    with _lock:
        _kline_cache[code] = {"ts": time.time(), "data": payload}


def _fetch_kline_uncached(code):
    """真正去上游拉K线（主腾讯，失败兜底东财）"""
    built = None
    try:
        symbol = symbol_of(code)
        raw = http_get(TENCENT_KLINE_URL.format(symbol=symbol),
                       referer="https://gu.qq.com/").decode("utf-8", "ignore")
        arr = _parse_tencent_kline(raw, symbol)
        built = _kline_from_arr(arr) if arr else None
        if not built:
            raise RuntimeError("tencent kline empty")
    except Exception as e:
        try:
            url = EAST_KLINE_URL.format(secid=secid_of(code))
            d = json.loads(http_get(url, referer="https://quote.eastmoney.com/"))
            arr = []
            for line in (d.get("data") or {}).get("klines") or []:
                f = line.split(",")
                if len(f) < 7:
                    continue
                arr.append({"day": f[0], "open": f[1], "high": f[3],
                            "low": f[4], "close": f[2], "volume": f[5]})
            built = _kline_from_arr(arr) if arr else None
            if not built:
                raise RuntimeError("east kline empty")
        except Exception as e2:
            return {"code": code, "error": f"K线获取失败: 腾讯({e}); 东财({e2})"}

    with _lock:
        name = _cb_name.get(code, "")
    return {
        "code": code,
        "name": name,
        **built,
        "ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def fetch_kline(code):
    """拉取可转债5分钟K线 + 布林线（带缓存 + 失败负缓存）"""
    cached = _kline_cached(code)
    if cached is not None:
        return cached
    payload = _fetch_kline_uncached(code)
    _kline_store(code, payload)
    return payload


def fetch_klines(codes, tail=0):
    """批量拉K线：去重 + 共享缓存 + 并发 + 尾部裁剪。返回 {code: data}"""
    uniq, seen = [], set()
    for c in codes:
        if c and c not in seen:
            seen.add(c)
            uniq.append(c)

    out = {}
    pending = []
    for c in uniq:
        cached = _kline_cached(c)
        if cached is not None:
            out[c] = _tail_slice(cached, tail)
        else:
            pending.append(c)

    if pending:
        if len(pending) == 1:
            out[pending[0]] = _tail_slice(fetch_kline(pending[0]), tail)
        else:
            futures = {c: _executor.submit(fetch_kline, c) for c in pending}
            for c, fut in futures.items():
                try:
                    out[c] = _tail_slice(fut.result(timeout=HTTP_TIMEOUT + 10), tail)
                except Exception as e:
                    out[c] = {"code": c, "error": str(e)}
    return out


# ==================== HTTP 服务 ====================
_CODE_RE = re.compile(r"^\d{6}$")


def _json(body):
    return json.dumps(body, ensure_ascii=False).encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"     # 配合 Content-Length 支持 keep-alive

    def log_message(self, fmt, *args):  # 静默访问日志
        pass

    def _send(self, code, body, ctype, extra=None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        if extra:
            for k, v in extra.items():
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    # ---------- 路由 ----------
    def do_GET(self):
        base = os.path.dirname(os.path.abspath(__file__))
        # 只取路径部分做路由，避免 /?code=xxx 这类带 query 的请求落到 404
        path_only = urllib.parse.urlparse(self.path).path

        try:
            if path_only in ("/", "/index.html"):
                self._serve_file(os.path.join(base, "index.html"),
                                 "text/html; charset=utf-8")
            elif path_only.startswith("/vendor/"):
                self._serve_vendor(base, path_only)
            elif path_only.startswith("/api/quotes"):
                self._api_quotes()
            elif path_only.startswith("/api/klines"):
                self._api_klines()
            elif path_only.startswith("/api/kline"):
                self._api_kline()
            elif path_only == "/api/health":
                self._send(200, _json({"ok": True, "market": market_status(),
                                       "total": len(_quotes)}),
                           "application/json; charset=utf-8")
            else:
                self._send(404, b"not found", "text/plain; charset=utf-8")
        except Exception as e:
            try:
                self._send(500, _json({"error": str(e)}),
                           "application/json; charset=utf-8")
            except Exception:
                pass

    # ---------- 静态资源 ----------
    def _serve_file(self, fp, ctype):
        if not os.path.isfile(fp):
            self._send(404, b"not found", "text/plain; charset=utf-8")
            return
        with open(fp, "rb") as f:
            self._send(200, f.read(), ctype)

    def _serve_vendor(self, base, path_only):
        # 本地静态资源（ECharts），只允许 vendor 目录内文件
        name = os.path.basename(path_only)
        fp = os.path.join(base, "vendor", name)
        if os.path.isfile(fp):
            with open(fp, "rb") as f:
                self._send(200, f.read(), "application/javascript; charset=utf-8")
        else:
            self._send(404, b"not found", "text/plain; charset=utf-8")

    # ---------- 接口 ----------
    def _api_quotes(self):
        maybe_refresh_async()   # 交易时段内按需加速刷新
        self._send(200, _json(build_payload()), "application/json; charset=utf-8")

    def _api_kline(self):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        code = (q.get("code") or [""])[0].strip()
        if not _CODE_RE.match(code):
            self._send(400, _json({"error": "invalid code"}),
                       "application/json; charset=utf-8")
            return
        self._send(200, _json(fetch_kline(code)), "application/json; charset=utf-8")

    def _api_klines(self):
        """批量K线：/api/klines?codes=113050,123112&tail=16
        自动盯盘扫描靠它把 20+ 次请求压缩成 1 次。"""
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        raw = (q.get("codes") or [""])[0]
        try:
            tail = max(0, min(320, int((q.get("tail") or ["0"])[0] or 0)))
        except ValueError:
            tail = 0
        codes = [c for c in re.split(r"[,\s]+", raw) if _CODE_RE.match(c)]
        if not codes:
            self._send(400, _json({"error": "no valid codes"}),
                       "application/json; charset=utf-8")
            return
        codes = codes[:60]      # 上限保护
        self._send(200, _json(fetch_klines(codes, tail)),
                   "application/json; charset=utf-8")


def main():
    port = PORT
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            pass
    threading.Thread(target=refresh_loop, daemon=True).start()
    srv = ThreadingHTTPServer((HOST, port), Handler)
    srv.daemon_threads = True
    print(f"可转债成交额监控已启动: http://{HOST}:{port}  "
          f"(兜底刷新 {REFRESH_SEC}s，交易时段按需加速)", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止", flush=True)
    finally:
        _POOL.clear()
        _executor.shutdown(wait=False)


if __name__ == "__main__":
    main()
