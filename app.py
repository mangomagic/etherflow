"""EtherFlow — lightweight Ethereum wallet-flow tracer.

A single-file Flask app that crawls wallet-to-wallet transaction flow from a
seed address using the Etherscan free tier, builds a frequency-weighted graph
with NetworkX, and serves it as JSON for in-browser visualization.

Design goals: infrequent use, no database, no build tooling. See README.md.
"""

from __future__ import annotations

import csv
import glob
import json
import os
import re
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Callable

import networkx as nx
import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

load_dotenv()

ETHERSCAN_API_KEY = os.environ.get("ETHERSCAN_API_KEY", "")
# Etherscan API V2 (the V1 /api endpoint is deprecated). One key works across
# all chains; `chainid` selects the network (1 = Ethereum mainnet).
ETHERSCAN_BASE_URL = os.environ.get("ETHERSCAN_BASE_URL", "https://api.etherscan.io/v2/api")
ETHERSCAN_CHAIN_ID = os.environ.get("ETHERSCAN_CHAIN_ID", "1")

# Defaults chosen to keep the crawl bounded and the renderer responsive.
DEFAULT_DEPTH = 2
DEFAULT_LIMIT = 25          # max counterparties kept per node (top-N by tx count)
MAX_DEPTH = 4               # hard ceiling regardless of request
MAX_LIMIT = 100
# Records fetched per address. Kept well under the free-tier per-request cap,
# which Etherscan is cutting from 10,000 to 1,000 on 2026-07-01.
TX_FETCH_LIMIT = 200
WEI = 10 ** 18

# Directory scanned for Etherscan "Export Transactions" CSV files (csv source).
DATA_DIR = os.environ.get("ETHERFLOW_DATA_DIR", "data")

ANNOTATIONS_FILE = os.environ.get("ETHERFLOW_ANNOTATIONS", "annotations.json")

# Addresses that would blow up any graph (exchange hot wallets, routers, etc.).
# Skipped as crawl frontiers but still shown as terminal nodes.
KNOWN_HIGH_VOLUME = {
    "0x0000000000000000000000000000000000000000",  # null / burn
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",  # Uniswap V2 router
    "0xe592427a0aece92de3edee1f18e0157c05861564",  # Uniswap V3 router
}

app = Flask(__name__)


class EtherscanError(Exception):
    """Raised when Etherscan returns an error we can't recover from."""


class RateLimiter:
    """Token-bucket-ish throttle. Free tier allows ~3 calls/sec; we stay under."""

    def __init__(self, min_interval: float = 0.34) -> None:
        self._min_interval = min_interval
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self) -> None:
        with self._lock:
            elapsed = time.monotonic() - self._last
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last = time.monotonic()


_limiter = RateLimiter()


def fetch_txlist(address: str, max_txs: int = TX_FETCH_LIMIT) -> list[dict]:
    """Return the most recent normal transactions for an address.

    Uses Etherscan API V2 account/txlist, newest first, capped at max_txs.
    """
    if not ETHERSCAN_API_KEY:
        raise EtherscanError("ETHERSCAN_API_KEY is not set (see .env.example).")

    _limiter.wait()
    params = {
        "chainid": ETHERSCAN_CHAIN_ID,
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "page": 1,
        "offset": min(max_txs, 1000),  # respect the free-tier per-request cap
        "sort": "desc",
        "apikey": ETHERSCAN_API_KEY,
    }
    try:
        resp = requests.get(ETHERSCAN_BASE_URL, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as exc:
        raise EtherscanError(f"Etherscan request failed: {exc}") from exc

    status, result = data.get("status"), data.get("result")
    if status == "1" and isinstance(result, list):
        return result
    # status "0": an empty *list* result means "no transactions" (benign).
    # A *string* result is a real notice/error — surface it instead of
    # silently returning an empty graph.
    if status == "0":
        if isinstance(result, list):
            return []
        msg = str(result)
        if "rate limit" in msg.lower():
            raise EtherscanError("Etherscan rate limit hit — slow down and retry.")
        raise EtherscanError(f"Etherscan: {msg}")
    raise EtherscanError(f"Unexpected Etherscan response: {result!r}")


def _find_column(fieldnames: list[str], *candidates: str) -> str | None:
    """Return the first header matching any candidate substring (case-insensitive)."""
    lowered = {name.lower(): name for name in fieldnames}
    for cand in candidates:
        for low, original in lowered.items():
            if cand in low:
                return original
    return None


def load_csv_index(
    paths: list[str],
) -> tuple[dict[str, list[dict]], dict[str, str]]:
    """Index Etherscan CSV exports by participating address.

    Handles both "Export Transactions" and "Export Internal Transactions" files
    in one pass. Returns (index, nametags) where:
      index    — {address: [tx_dict, ...]} for every address that appears
      nametags — {address: label} collected from From_Nametag/To_Nametag columns

    Column variants handled:
      Value      — Value_IN(ETH)/Value_OUT(ETH), Value(ETH), or Amount ("3.02 ETH")
      Timestamp  — UnixTimestamp (int) or DateTime (UTC) ("YYYY-MM-DD HH:MM:SS")
      Status     — blank/Success → included; anything else → skipped
    """
    index: dict[str, list[dict]] = defaultdict(list)
    nametags: dict[str, str] = {}

    if not paths:
        raise EtherscanError(f"No CSV files found in '{DATA_DIR}/'. Add Etherscan exports there.")

    for path in paths:
        with open(path, newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            if not reader.fieldnames:
                continue
            fn = reader.fieldnames
            f_from     = _find_column(fn, "from")
            f_to       = _find_column(fn, "to")
            f_in       = _find_column(fn, "value_in", "valuein")
            f_out      = _find_column(fn, "value_out", "valueout")
            # Explicit ETH columns only — avoid matching "Value (USD)"
            f_val      = _find_column(fn, "value(eth", "value (eth")
            f_amount   = _find_column(fn, "amount")   # internal txs: "3.02 ETH"
            f_ts       = _find_column(fn, "unixtimestamp")
            f_dt       = _find_column(fn, "datetime")
            f_status   = _find_column(fn, "status")
            f_from_tag = _find_column(fn, "from_nametag", "fromnametag")
            f_to_tag   = _find_column(fn, "to_nametag",   "tonametag")
            f_hash     = _find_column(fn, "transaction hash", "parent transaction")
            f_method   = _find_column(fn, "method", "type")

            if not (f_from and f_to):
                raise EtherscanError(f"{os.path.basename(path)}: missing From/To columns.")

            for row in reader:
                # Skip failed transactions.
                if f_status:
                    sv = (row.get(f_status) or "").strip().lower()
                    if sv and sv not in ("success", "0", ""):
                        continue

                src = (row.get(f_from) or "").strip().lower()
                dst = (row.get(f_to)   or "").strip().lower()
                if not (src.startswith("0x") and dst.startswith("0x")):
                    continue

                # Collect address labels from nametag columns.
                if f_from_tag:
                    tag = (row.get(f_from_tag) or "").strip()
                    if tag:
                        nametags.setdefault(src, tag)
                if f_to_tag:
                    tag = (row.get(f_to_tag) or "").strip()
                    if tag:
                        nametags.setdefault(dst, tag)

                # ETH value: prefer explicit IN/OUT, then ETH value col, then Amount.
                eth = _parse_float(row.get(f_in)) + _parse_float(row.get(f_out))
                if eth == 0.0 and f_val:
                    eth = _parse_float(row.get(f_val))
                if eth == 0.0 and f_amount:
                    eth = _parse_eth_amount(row.get(f_amount))

                # Timestamp: prefer unix int, fall back to datetime string.
                if f_ts:
                    ts = int(_parse_float(row.get(f_ts, "0")))
                elif f_dt:
                    ts = _parse_datetime_utc(row.get(f_dt))
                else:
                    ts = 0

                tx = {
                    "from": src,
                    "to": dst,
                    "value": str(int(eth * WEI)),
                    "timeStamp": str(ts),
                    "hash":   (row.get(f_hash)   or "").strip() if f_hash   else "",
                    "method": (row.get(f_method) or "").strip() if f_method else "",
                }
                index[src].append(tx)
                index[dst].append(tx)

    return index, nametags


def _parse_float(raw: str | None) -> float:
    if not raw:
        return 0.0
    try:
        return float(str(raw).replace(",", ""))
    except ValueError:
        return 0.0


def _parse_eth_amount(raw: str | None) -> float:
    """Parse Etherscan's internal-tx Amount format: '3.02 ETH' or '3.02'."""
    if not raw:
        return 0.0
    try:
        return float(str(raw).replace("ETH", "").replace(",", "").strip())
    except ValueError:
        return 0.0


def _parse_datetime_utc(raw: str | None) -> int:
    """Parse 'YYYY-MM-DD HH:MM:SS' UTC string to a Unix timestamp."""
    if not raw:
        return 0
    try:
        return int(
            datetime.strptime(raw.strip(), "%Y-%m-%d %H:%M:%S")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )
    except ValueError:
        return 0


def aggregate_directed(address: str, txs: list[dict]) -> dict[tuple[str, str], dict]:
    """Aggregate transactions into directed (src, dst) pairs involving `address`.

    Returns {(src, dst): {count, value_eth, last_ts}} preserving true transfer
    direction. Only pairs where `address` is either src or dst are included,
    so A→B and B→A are kept as separate entries rather than collapsed.
    """
    address = address.lower()
    agg: dict[tuple[str, str], dict] = defaultdict(
        lambda: {"count": 0, "value_eth": 0.0, "last_ts": 0}
    )
    for tx in txs:
        src = (tx.get("from") or "").lower()
        dst = (tx.get("to") or "").lower()
        if not dst or src == dst:
            continue
        if src != address and dst != address:
            continue
        try:
            value_eth = int(tx.get("value", "0")) / WEI
            ts = int(tx.get("timeStamp", "0"))
        except (TypeError, ValueError):
            value_eth, ts = 0.0, 0
        entry = agg[(src, dst)]
        entry["count"] += 1
        entry["value_eth"] += value_eth
        entry["last_ts"] = max(entry["last_ts"], ts)
    return agg


def crawl(
    seed: str,
    depth: int,
    limit: int,
    fetcher: Callable[[str], list[dict]] = fetch_txlist,
) -> nx.DiGraph:
    """Breadth-first crawl of wallet flow out to `depth` hops from `seed`.

    `fetcher(address)` returns that address's transactions as dicts shaped like
    the Etherscan API (`from`/`to`/`value` in wei/`timeStamp`). Swapping it lets
    the crawl run off the live API or a set of exported CSV files unchanged.

    Each node keeps at most `limit` counterparties (the most frequent ones),
    which is the primary guard against fan-out explosion.
    """
    seed = seed.lower()
    graph = nx.DiGraph()
    graph.add_node(seed, depth=0, seed=True)

    visited: set[str] = set()
    frontier: deque[tuple[str, int]] = deque([(seed, 0)])

    while frontier:
        address, level = frontier.popleft()
        if address in visited or level >= depth:
            continue
        visited.add(address)
        if address in KNOWN_HIGH_VOLUME and level > 0:
            continue  # show it, but don't expand through it

        txs = fetcher(address)
        directed = aggregate_directed(address, txs)

        # Rank counterparties by combined tx count across both directions for limit.
        counterparty_totals: dict[str, int] = defaultdict(int)
        for (src, dst), stats in directed.items():
            neighbor = dst if src == address else src
            counterparty_totals[neighbor] += stats["count"]

        top_neighbors = {
            n for n, _ in sorted(
                counterparty_totals.items(), key=lambda kv: kv[1], reverse=True
            )[:limit]
        }

        queued: set[str] = set()
        for (src, dst), stats in directed.items():
            neighbor = dst if src == address else src
            if neighbor not in top_neighbors:
                continue
            if neighbor not in graph:
                graph.add_node(neighbor, depth=level + 1, seed=False)
            graph.add_edge(
                src, dst,
                count=stats["count"],
                value_eth=round(stats["value_eth"], 6),
                last_ts=stats["last_ts"],
            )
            if neighbor not in visited and neighbor not in queued and level + 1 < depth:
                frontier.append((neighbor, level + 1))
                queued.add(neighbor)

    return graph


def load_annotations() -> dict[str, dict]:
    """Return {address: {note, color}} from annotations.json.

    Handles the legacy format where values were plain strings (note only).
    """
    try:
        with open(ANNOTATIONS_FILE, encoding="utf-8") as fh:
            raw = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    result = {}
    for addr, val in raw.items():
        if isinstance(val, str):
            result[addr] = {"note": val, "color": None}
        elif isinstance(val, dict):
            result[addr] = {"note": val.get("note", ""), "color": val.get("color")}
    return result


def save_annotation(address: str, note: str, color: str | None) -> None:
    data = load_annotations()
    if note or color:
        data[address] = {"note": note, "color": color}
    else:
        data.pop(address, None)
    with open(ANNOTATIONS_FILE, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, sort_keys=True)


def _node_label(addr: str, nametag: str) -> str:
    if nametag:
        return nametag if len(nametag) <= 22 else nametag[:21] + "…"
    return addr[:6] + "…" + addr[-4:]


def graph_to_payload(
    graph: nx.DiGraph,
    seed: str,
    nametags: dict[str, str] | None = None,
    annotations: dict[str, str] | None = None,
) -> dict:
    """Serialize the graph into vis-network-friendly nodes/edges JSON."""
    seed = seed.lower()
    nametags = nametags or {}
    annotations = annotations or {}
    nodes = []
    for node, data in graph.nodes(data=True):
        tag = nametags.get(node, "")
        ann = annotations.get(node, {})
        note = ann.get("note", "") if isinstance(ann, dict) else ann
        color = ann.get("color") if isinstance(ann, dict) else None
        title_parts = [p for p in [tag, note, node] if p]
        nodes.append({
            "id": node,
            "label": _node_label(node, tag),
            "title": "\n".join(title_parts),
            "depth": data.get("depth", 0),
            "seed": node == seed,
            "highVolume": node in KNOWN_HIGH_VOLUME,
            "tagged": bool(tag),
            "annotation": note,
            "customColor": color,
        })
    edges = []
    for src, dst, data in graph.edges(data=True):
        edges.append({
            "from": src,
            "to": dst,
            "count": data.get("count", 1),
            "value_eth": data.get("value_eth", 0.0),
        })
    return {"nodes": nodes, "edges": edges, "seed": seed,
            "stats": {"nodes": len(nodes), "edges": len(edges)}}


def csv_files() -> list[str]:
    """All CSV files currently in DATA_DIR."""
    return sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))


_csv_cache: dict = {"index": None, "nametags": None, "mtimes": {}}


def get_csv_index() -> tuple[dict[str, list[dict]], dict[str, str]]:
    """Return the cached CSV index, rebuilding only when files have changed."""
    paths = csv_files()
    if not paths:
        return defaultdict(list), {}
    mtimes = {p: os.path.getmtime(p) for p in paths}
    if _csv_cache["index"] is None or _csv_cache["mtimes"] != mtimes:
        _csv_cache["index"], _csv_cache["nametags"] = load_csv_index(paths)
        _csv_cache["mtimes"] = mtimes
    return _csv_cache["index"], _csv_cache["nametags"]


_ADDRESS_RE = re.compile(r"(0x[0-9a-fA-F]{40})", re.IGNORECASE)


def seed_addresses_from_csvs(paths: list[str]) -> list[str]:
    """Extract wallet addresses embedded in Etherscan export filenames.

    Etherscan names regular exports 'export-0x<address>.csv', so the seed
    address is recoverable without loading the file.
    """
    seen: list[str] = []
    for path in paths:
        m = _ADDRESS_RE.search(os.path.basename(path))
        if m:
            addr = m.group(1).lower()
            if addr not in seen:
                seen.append(addr)
    return seen


@app.route("/")
def index():
    files = csv_files()
    return render_template(
        "index.html",
        configured=bool(ETHERSCAN_API_KEY),
        csv_count=len(files),
        csv_seeds=seed_addresses_from_csvs(files),
    )


@app.route("/api/trace")
def trace():
    address = (request.args.get("address") or "").strip()
    if not (address.startswith("0x") and len(address) == 42):
        return jsonify({"error": "Provide a valid 0x… 42-char Ethereum address."}), 400

    depth = _clamp_int(request.args.get("depth"), DEFAULT_DEPTH, 1, MAX_DEPTH)
    limit = _clamp_int(request.args.get("limit"), DEFAULT_LIMIT, 1, MAX_LIMIT)
    source = (request.args.get("source") or "api").lower()

    try:
        if source == "csv":
            index, nametags = get_csv_index()
            graph = crawl(address, depth, limit, fetcher=lambda a: index.get(a, []))
        else:
            nametags = {}
            graph = crawl(address, depth, limit)
    except EtherscanError as exc:
        return jsonify({"error": str(exc)}), 502

    return jsonify(graph_to_payload(graph, address, nametags, load_annotations()))


@app.route("/api/transactions")
def transactions():
    address = (request.args.get("address") or "").strip().lower()
    source  = (request.args.get("source")  or "csv").lower()
    page     = _clamp_int(request.args.get("page"),     1,  1, 9999)
    per_page = _clamp_int(request.args.get("per_page"), 15, 5,   50)

    if not (address.startswith("0x") and len(address) == 42):
        return jsonify({"error": "Invalid address"}), 400

    try:
        if source == "csv":
            index, _ = get_csv_index()
            raw = list(index.get(address, []))
        else:
            raw = fetch_txlist(address, max_txs=1000)
    except EtherscanError as exc:
        return jsonify({"error": str(exc)}), 502

    # Deduplicate repeated index entries (e.g. self-transfers indexed under
    # both from and to). The key must include direction and value, not just
    # hash: internal transactions carry their *parent* transaction's hash, so
    # keying on hash alone would wrongly drop an internal transfer whenever
    # its parent tx is also present in a regular export.
    seen: set[str] = set()
    unique: list[dict] = []
    for tx in raw:
        key = (f"{tx.get('hash', '')}|{tx['from']}|{tx['to']}"
               f"|{tx.get('value', '')}|{tx.get('timeStamp', '')}")
        if key not in seen:
            seen.add(key)
            unique.append(tx)

    unique.sort(key=lambda t: int(t.get("timeStamp", 0)), reverse=True)

    summary = _tx_summary(address, unique)

    total    = len(unique)
    pages    = max(1, (total + per_page - 1) // per_page)
    page     = min(page, pages)
    start    = (page - 1) * per_page

    result = []
    for tx in unique[start : start + per_page]:
        ts = int(tx.get("timeStamp", 0))
        dt = (datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
              if ts else "—")
        result.append({
            "hash":      tx.get("hash", ""),
            "date":      dt,
            "from":      tx.get("from", ""),
            "to":        tx.get("to",   ""),
            "value_eth": round(int(tx.get("value", 0)) / WEI, 6),
            "method":    tx.get("method", ""),
        })

    return jsonify({"txs": result, "total": total, "page": page, "pages": pages,
                    "summary": summary})


def _tx_summary(address: str, txs: list[dict]) -> dict:
    """Aggregate flow stats for one address over its (deduplicated) tx list."""
    in_eth = out_eth = 0.0
    in_count = out_count = 0
    first_ts = last_ts = 0
    parties: set[str] = set()
    for tx in txs:
        try:
            eth = int(tx.get("value", 0)) / WEI
            ts = int(tx.get("timeStamp", 0))
        except (TypeError, ValueError):
            eth, ts = 0.0, 0
        if tx.get("to") == address:
            in_eth += eth
            in_count += 1
            parties.add(tx.get("from", ""))
        elif tx.get("from") == address:
            out_eth += eth
            out_count += 1
            parties.add(tx.get("to", ""))
        if ts:
            first_ts = min(first_ts or ts, ts)
            last_ts = max(last_ts, ts)
    return {
        "in_eth": round(in_eth, 6),
        "out_eth": round(out_eth, 6),
        "net_eth": round(in_eth - out_eth, 6),
        "in_count": in_count,
        "out_count": out_count,
        "first_ts": first_ts,
        "last_ts": last_ts,
        "counterparties": len(parties - {address, ""}),
    }


_COLOR_RE = re.compile(r"^#[0-9a-fA-F]{6}$")


@app.route("/api/annotate", methods=["POST"])
def annotate():
    body = request.get_json(silent=True) or {}
    address = (body.get("address") or "").strip().lower()
    note = (body.get("note") or "").strip()
    color = body.get("color") or None
    if not (address.startswith("0x") and len(address) == 42):
        return jsonify({"error": "Invalid address"}), 400
    if color and not _COLOR_RE.match(color):
        return jsonify({"error": "color must be #rrggbb or null"}), 400
    save_annotation(address, note, color)
    return jsonify({"address": address, "note": note, "color": color})


def _clamp_int(raw: str | None, default: int, lo: int, hi: int) -> int:
    try:
        return max(lo, min(hi, int(raw)))
    except (TypeError, ValueError):
        return default


if __name__ == "__main__":
    # Default binds loopback only; Docker sets FLASK_RUN_HOST=0.0.0.0.
    app.run(
        debug=bool(os.environ.get("FLASK_DEBUG")),
        host=os.environ.get("FLASK_RUN_HOST", "127.0.0.1"),
        port=int(os.environ.get("FLASK_RUN_PORT", "5000")),
    )
