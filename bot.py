import asyncio
import hashlib
import html
import json
import logging
import os
import re
import threading
from datetime import datetime, time as dt_time, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras
import requests
from flask import Flask
from requests.adapters import HTTPAdapter
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)
from urllib3.util.retry import Retry


# ============================================================
# CONFIGURAÇÕES
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("polymarket-spy-bot")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
ALERT_CHAT_ID = os.environ.get("ALERT_CHAT_ID", "").strip()

POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "60"))
MIN_ALERT_USD = float(os.environ.get("MIN_ALERT_USD", "1000"))
LARGE_ALERT_USD = float(os.environ.get("LARGE_ALERT_USD", "5000"))
WHALE_ALERT_USD = float(os.environ.get("WHALE_ALERT_USD", "20000"))
SETTLE_SECONDS = int(os.environ.get("SETTLE_SECONDS", "60"))
SMALL_BATCH_MAX_AGE_SECONDS = int(
    os.environ.get("SMALL_BATCH_MAX_AGE_SECONDS", "900")
)
FETCH_LIMIT = int(os.environ.get("FETCH_LIMIT", "100"))

BOT_TIMEZONE_NAME = os.environ.get("BOT_TIMEZONE", "America/Sao_Paulo")
DAILY_SUMMARY_HOUR = int(os.environ.get("DAILY_SUMMARY_HOUR", "21"))
DAILY_SUMMARY_MINUTE = int(os.environ.get("DAILY_SUMMARY_MINUTE", "0"))

DATA_API_BASE = "https://data-api.polymarket.com"
GAMMA_API_BASE = "https://gamma-api.polymarket.com"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN não configurado.")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não configurado.")
if not (0 <= DAILY_SUMMARY_HOUR <= 23):
    raise RuntimeError("DAILY_SUMMARY_HOUR deve estar entre 0 e 23.")
if not (0 <= DAILY_SUMMARY_MINUTE <= 59):
    raise RuntimeError("DAILY_SUMMARY_MINUTE deve estar entre 0 e 59.")
if not (0 < MIN_ALERT_USD <= LARGE_ALERT_USD <= WHALE_ALERT_USD):
    raise RuntimeError(
        "Use MIN_ALERT_USD <= LARGE_ALERT_USD <= WHALE_ALERT_USD."
    )

BOT_TZ = ZoneInfo(BOT_TIMEZONE_NAME)


# ============================================================
# HTTP COM RETENTATIVAS
# ============================================================

HTTP = requests.Session()
_retry = Retry(
    total=3,
    connect=3,
    read=3,
    backoff_factor=1,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET"]),
)
HTTP.mount("https://", HTTPAdapter(max_retries=_retry))
HTTP.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Origin": "https://polymarket.com",
        "Referer": "https://polymarket.com/",
    }
)

_MARKET_CACHE: Dict[str, Dict[str, Any]] = {}


# ============================================================
# FLASK — PORTA HTTP DO RENDER
# ============================================================

web_app = Flask(__name__)


@web_app.route("/")
def home():
    try:
        wallets = get_active_wallets()
        pending = count_pending_events()
    except Exception:
        wallets = []
        pending = -1

    return {
        "status": "online",
        "bot": "polymarket-spy-bot",
        "wallets_monitoradas": len(wallets),
        "eventos_pendentes": pending,
        "poll_interval": POLL_INTERVAL,
        "alerta_minimo_usd": MIN_ALERT_USD,
    }, 200


def run_flask() -> None:
    port = int(os.environ.get("PORT", "10000"))
    web_app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False,
    )


# ============================================================
# POSTGRESQL
# ============================================================


def get_conn():
    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=psycopg2.extras.RealDictCursor,
        connect_timeout=15,
    )


def criar_banco() -> None:
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS subscribers (
                    chat_id BIGINT PRIMARY KEY,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS tracked_wallets (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    wallet_address TEXT UNIQUE NOT NULL,
                    active BOOLEAN NOT NULL DEFAULT TRUE,
                    bootstrapped BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_events (
                    raw_id TEXT PRIMARY KEY,
                    wallet_address TEXT NOT NULL,
                    trader_name TEXT NOT NULL,
                    transaction_hash TEXT,
                    condition_id TEXT,
                    asset TEXT,
                    side TEXT,
                    outcome TEXT,
                    market_title TEXT,
                    price NUMERIC(18, 8),
                    size NUMERIC(28, 8),
                    usdc_value NUMERIC(28, 8),
                    market_slug TEXT,
                    event_slug TEXT,
                    end_date TEXT,
                    event_timestamp BIGINT NOT NULL DEFAULT 0,
                    raw_json JSONB,
                    processed BOOLEAN NOT NULL DEFAULT FALSE,
                    alerted BOOLEAN NOT NULL DEFAULT FALSE,
                    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    processed_at TIMESTAMPTZ
                );
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS alert_batches (
                    id BIGSERIAL PRIMARY KEY,
                    batch_key TEXT UNIQUE NOT NULL,
                    wallet_address TEXT NOT NULL,
                    trader_name TEXT NOT NULL,
                    condition_id TEXT,
                    asset TEXT,
                    side TEXT,
                    outcome TEXT,
                    market_title TEXT,
                    avg_price NUMERIC(18, 8),
                    total_size NUMERIC(28, 8),
                    total_usdc NUMERIC(28, 8) NOT NULL,
                    classification TEXT NOT NULL,
                    source_count INTEGER NOT NULL,
                    market_slug TEXT,
                    event_slug TEXT,
                    end_date TEXT,
                    first_event_ts BIGINT,
                    last_event_ts BIGINT,
                    sent BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    sent_at TIMESTAMPTZ
                );
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS daily_summary_log (
                    summary_date DATE PRIMARY KEY,
                    sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_trade_events_pending
                ON trade_events(processed, wallet_address);
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_alert_batches_sent_at
                ON alert_batches(sent_at);
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_wallets_active
                ON tracked_wallets(active);
                """
            )

            # Migração segura da versão anterior: se a nova tabela estiver vazia,
            # força novo bootstrap para não disparar histórico antigo.
            cursor.execute("SELECT COUNT(*) AS total FROM trade_events;")
            total = int(cursor.fetchone()["total"] or 0)
            if total == 0:
                cursor.execute(
                    "UPDATE tracked_wallets SET bootstrapped = FALSE WHERE active = TRUE;"
                )

    logger.info("Banco PostgreSQL pronto.")


def normalize_wallet(wallet: str) -> str:
    return wallet.strip().lower()


def save_subscriber(chat_id: int) -> None:
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO subscribers (chat_id)
                VALUES (%s)
                ON CONFLICT (chat_id) DO NOTHING;
                """,
                (chat_id,),
            )


def get_subscribers() -> List[int]:
    result: List[int] = []

    if ALERT_CHAT_ID:
        for item in ALERT_CHAT_ID.split(","):
            try:
                value = int(item.strip())
                if value not in result:
                    result.append(value)
            except ValueError:
                logger.warning("ALERT_CHAT_ID inválido ignorado: %s", item)

    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT chat_id FROM subscribers ORDER BY created_at;")
            rows = cursor.fetchall()

    for row in rows:
        value = int(row["chat_id"])
        if value not in result:
            result.append(value)

    return result


def get_active_wallets() -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, name, wallet_address, active, bootstrapped
                FROM tracked_wallets
                WHERE active = TRUE
                ORDER BY id;
                """
            )
            return list(cursor.fetchall())


def add_wallet_db(name: str, wallet: str) -> None:
    wallet = normalize_wallet(wallet)
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO tracked_wallets (
                    name, wallet_address, active, bootstrapped
                )
                VALUES (%s, %s, TRUE, FALSE)
                ON CONFLICT (wallet_address)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    active = TRUE,
                    bootstrapped = FALSE,
                    updated_at = NOW();
                """,
                (name, wallet),
            )


def remove_wallet_db(wallet: str) -> bool:
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE tracked_wallets
                SET active = FALSE, updated_at = NOW()
                WHERE wallet_address = %s
                RETURNING wallet_address;
                """,
                (normalize_wallet(wallet),),
            )
            return cursor.fetchone() is not None


def set_wallet_bootstrapped(wallet: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE tracked_wallets
                SET bootstrapped = TRUE, updated_at = NOW()
                WHERE wallet_address = %s;
                """,
                (normalize_wallet(wallet),),
            )


def count_pending_events() -> int:
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS total FROM trade_events WHERE processed = FALSE;"
            )
            return int(cursor.fetchone()["total"] or 0)


def count_alerts_today() -> int:
    start_utc, end_utc = local_day_bounds_utc(datetime.now(BOT_TZ).date())
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) AS total
                FROM alert_batches
                WHERE sent = TRUE
                  AND sent_at >= %s
                  AND sent_at < %s;
                """,
                (start_utc, end_utc),
            )
            return int(cursor.fetchone()["total"] or 0)


# ============================================================
# HELPERS DE DADOS
# ============================================================


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, Decimal):
            return float(value)
        return float(value)
    except (TypeError, ValueError):
        return None


def request_json(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    response = HTTP.get(url, params=params or {}, timeout=25)
    response.raise_for_status()
    return response.json()


def fetch_activity(wallet: str, limit: int = FETCH_LIMIT) -> List[Dict[str, Any]]:
    data = request_json(
        f"{DATA_API_BASE}/activity",
        {
            "user": normalize_wallet(wallet),
            "limit": limit,
            "offset": 0,
            "type": "TRADE",
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
        },
    )
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("data", [])
    return []


def fetch_trades_fallback(
    wallet: str, limit: int = FETCH_LIMIT
) -> List[Dict[str, Any]]:
    data = request_json(
        f"{DATA_API_BASE}/trades",
        {
            "user": normalize_wallet(wallet),
            "limit": limit,
            "offset": 0,
            "takerOnly": "false",
        },
    )
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("data", [])
    return []


def fetch_latest_trades(
    wallet: str, limit: int = FETCH_LIMIT
) -> List[Dict[str, Any]]:
    try:
        rows = fetch_activity(wallet, limit)
        if rows:
            return rows
        logger.warning("/activity vazio para %s; tentando /trades.", wallet)
    except Exception as exc:
        logger.warning("Erro em /activity para %s: %s", wallet, exc)

    return fetch_trades_fallback(wallet, limit)


def get_usdc_value(trade: Dict[str, Any]) -> Optional[float]:
    for key in ("usdcSize", "usdc_size", "value", "amount", "cost"):
        value = safe_float(trade.get(key))
        if value is not None:
            return value

    size = safe_float(trade.get("size"))
    price = safe_float(trade.get("price"))
    if size is not None and price is not None:
        return size * price
    return None


def build_raw_id(trade: Dict[str, Any], wallet: str) -> str:
    canonical = {
        "wallet": normalize_wallet(wallet),
        "transactionHash": trade.get("transactionHash")
        or trade.get("transaction_hash")
        or trade.get("hash"),
        "id": trade.get("id"),
        "conditionId": trade.get("conditionId"),
        "asset": trade.get("asset"),
        "side": trade.get("side"),
        "outcome": trade.get("outcome"),
        "price": trade.get("price"),
        "size": trade.get("size"),
        "timestamp": trade.get("timestamp"),
    }
    payload = json.dumps(canonical, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def extract_end_date_from_trade(trade: Dict[str, Any]) -> Optional[str]:
    for key in (
        "endDate",
        "end_date",
        "endDateIso",
        "endDateISO",
        "marketEndDate",
        "closeTime",
    ):
        value = trade.get(key)
        if value:
            return str(value)[:10]

    title = str(trade.get("title") or "")
    match = re.search(r"(20\d{2}-\d{2}-\d{2})", title)
    return match.group(1) if match else None


def get_market_metadata(slug: Optional[str]) -> Dict[str, Any]:
    if not slug:
        return {}
    if slug in _MARKET_CACHE:
        return _MARKET_CACHE[slug]

    try:
        data = request_json(f"{GAMMA_API_BASE}/markets/slug/{slug}")
        if isinstance(data, dict):
            _MARKET_CACHE[slug] = data
            return data
    except Exception as exc:
        logger.warning("Falha buscando metadados de %s: %s", slug, exc)

    return {}


def enrich_end_date(trade: Dict[str, Any]) -> Optional[str]:
    direct = extract_end_date_from_trade(trade)
    if direct:
        return direct

    metadata = get_market_metadata(trade.get("slug"))
    for key in ("endDate", "endDateIso"):
        value = metadata.get(key)
        if value:
            return str(value)[:10]
    return None


def insert_trade_event(
    trade: Dict[str, Any], wallet_row: Dict[str, Any], processed: bool
) -> bool:
    wallet = normalize_wallet(wallet_row["wallet_address"])
    raw_id = build_raw_id(trade, wallet)
    event_ts = int(safe_float(trade.get("timestamp")) or 0)
    end_date = enrich_end_date(trade)

    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO trade_events (
                    raw_id, wallet_address, trader_name, transaction_hash,
                    condition_id, asset, side, outcome, market_title,
                    price, size, usdc_value, market_slug, event_slug,
                    end_date, event_timestamp, raw_json,
                    processed, alerted, processed_at
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, FALSE,
                    CASE WHEN %s THEN NOW() ELSE NULL END
                )
                ON CONFLICT (raw_id) DO NOTHING;
                """,
                (
                    raw_id,
                    wallet,
                    wallet_row["name"],
                    trade.get("transactionHash")
                    or trade.get("transaction_hash")
                    or trade.get("hash"),
                    trade.get("conditionId"),
                    trade.get("asset"),
                    str(trade.get("side") or "").upper(),
                    trade.get("outcome"),
                    trade.get("title"),
                    safe_float(trade.get("price")),
                    safe_float(trade.get("size")),
                    get_usdc_value(trade),
                    trade.get("slug"),
                    trade.get("eventSlug"),
                    end_date,
                    event_ts,
                    psycopg2.extras.Json(trade),
                    processed,
                    processed,
                ),
            )
            return cursor.rowcount > 0


def bootstrap_wallet_sync(wallet_row: Dict[str, Any]) -> int:
    trades = fetch_latest_trades(wallet_row["wallet_address"], FETCH_LIMIT)
    inserted = 0
    for trade in trades:
        if insert_trade_event(trade, wallet_row, processed=True):
            inserted += 1
    set_wallet_bootstrapped(wallet_row["wallet_address"])
    return inserted


def ingest_wallet_sync(wallet_row: Dict[str, Any]) -> int:
    trades = fetch_latest_trades(wallet_row["wallet_address"], FETCH_LIMIT)
    inserted = 0
    for trade in trades:
        if insert_trade_event(trade, wallet_row, processed=False):
            inserted += 1
    return inserted


# ============================================================
# AGREGAÇÃO, FILTRO E CLASSIFICAÇÃO
# ============================================================


def classify_entry(value: float) -> Tuple[str, str]:
    if value >= WHALE_ALERT_USD:
        return "WHALE", "🐋 Whale"
    if value >= LARGE_ALERT_USD:
        return "GRANDE", "🟧 Grande"
    if value >= MIN_ALERT_USD:
        return "MEDIA", "🟨 Média"
    return "ABAIXO_MINIMO", "⬜ Abaixo do mínimo"


def suggested_unit(value: float) -> str:
    if value >= WHALE_ALERT_USD:
        return "2.0"
    if value >= LARGE_ALERT_USD:
        return "1.0"
    if value >= MIN_ALERT_USD:
        return "0.5"
    return "0.0"


def fetch_pending_groups() -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    wallet_address,
                    MAX(trader_name) AS trader_name,
                    condition_id,
                    asset,
                    side,
                    outcome,
                    MAX(market_title) AS market_title,
                    CASE
                        WHEN SUM(COALESCE(size, 0)) > 0
                        THEN SUM(COALESCE(price, 0) * COALESCE(size, 0))
                             / SUM(COALESCE(size, 0))
                        ELSE AVG(price)
                    END AS avg_price,
                    SUM(COALESCE(size, 0)) AS total_size,
                    SUM(COALESCE(usdc_value, 0)) AS total_usdc,
                    MAX(market_slug) AS market_slug,
                    MAX(event_slug) AS event_slug,
                    MAX(end_date) AS end_date,
                    MIN(event_timestamp) AS first_event_ts,
                    MAX(event_timestamp) AS last_event_ts,
                    COUNT(*) AS source_count
                FROM trade_events
                WHERE processed = FALSE
                GROUP BY
                    wallet_address, condition_id, asset, side, outcome
                ORDER BY MIN(event_timestamp);
                """
            )
            return list(cursor.fetchall())


def _group_where_sql() -> str:
    return """
        wallet_address = %s
        AND condition_id IS NOT DISTINCT FROM %s
        AND asset IS NOT DISTINCT FROM %s
        AND side IS NOT DISTINCT FROM %s
        AND outcome IS NOT DISTINCT FROM %s
        AND processed = FALSE
    """


def mark_group_processed(group: Dict[str, Any], alerted: bool) -> None:
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE trade_events
                SET processed = TRUE,
                    alerted = %s,
                    processed_at = NOW()
                WHERE {_group_where_sql()};
                """,
                (
                    alerted,
                    group["wallet_address"],
                    group.get("condition_id"),
                    group.get("asset"),
                    group.get("side"),
                    group.get("outcome"),
                ),
            )


def create_alert_batch(group: Dict[str, Any]) -> Optional[int]:
    total_usdc = float(group.get("total_usdc") or 0)
    classification, _ = classify_entry(total_usdc)
    canonical = {
        "wallet": group["wallet_address"],
        "condition": group.get("condition_id"),
        "asset": group.get("asset"),
        "side": group.get("side"),
        "outcome": group.get("outcome"),
        "first": int(group.get("first_event_ts") or 0),
        "last": int(group.get("last_event_ts") or 0),
        "count": int(group.get("source_count") or 0),
        "value": round(total_usdc, 6),
    }
    batch_key = hashlib.sha256(
        json.dumps(canonical, sort_keys=True).encode("utf-8")
    ).hexdigest()

    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO alert_batches (
                    batch_key, wallet_address, trader_name,
                    condition_id, asset, side, outcome, market_title,
                    avg_price, total_size, total_usdc, classification,
                    source_count, market_slug, event_slug, end_date,
                    first_event_ts, last_event_ts
                )
                VALUES (
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s
                )
                ON CONFLICT (batch_key) DO NOTHING
                RETURNING id;
                """,
                (
                    batch_key,
                    group["wallet_address"],
                    group["trader_name"],
                    group.get("condition_id"),
                    group.get("asset"),
                    group.get("side"),
                    group.get("outcome"),
                    group.get("market_title"),
                    group.get("avg_price"),
                    group.get("total_size"),
                    total_usdc,
                    classification,
                    group.get("source_count"),
                    group.get("market_slug"),
                    group.get("event_slug"),
                    group.get("end_date"),
                    group.get("first_event_ts"),
                    group.get("last_event_ts"),
                ),
            )
            row = cursor.fetchone()
            return int(row["id"]) if row else None


def prepare_alert_batches_sync() -> Tuple[int, int]:
    now_ts = int(datetime.now(timezone.utc).timestamp())
    created = 0
    ignored = 0

    for group in fetch_pending_groups():
        last_ts = int(group.get("last_event_ts") or 0)
        first_ts = int(group.get("first_event_ts") or 0)
        total_usdc = float(group.get("total_usdc") or 0)

        if now_ts - last_ts < SETTLE_SECONDS:
            continue

        if total_usdc >= MIN_ALERT_USD:
            create_alert_batch(group)
            mark_group_processed(group, alerted=True)
            created += 1
        elif now_ts - first_ts >= SMALL_BATCH_MAX_AGE_SECONDS:
            mark_group_processed(group, alerted=False)
            ignored += 1

    return created, ignored


def get_unsent_batches(limit: int = 50) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM alert_batches
                WHERE sent = FALSE
                ORDER BY id
                LIMIT %s;
                """,
                (limit,),
            )
            return list(cursor.fetchall())


def mark_batch_sent(batch_id: int) -> None:
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE alert_batches
                SET sent = TRUE, sent_at = NOW()
                WHERE id = %s;
                """,
                (batch_id,),
            )


# ============================================================
# FORMATAÇÃO DOS ALERTAS
# ============================================================


def format_money(value: Any) -> str:
    number = safe_float(value)
    return "N/A" if number is None else f"${number:,.0f}"


def format_price_and_odd(price: Any) -> str:
    number = safe_float(price)
    if number is None or number <= 0:
        return "N/A"
    return f"{number * 100:.1f}% (Odd {1 / number:.2f})"


def confidence_label(price: Any) -> str:
    number = safe_float(price)
    if number is None:
        return "N/A"
    percentage = number * 100
    if percentage >= 80:
        return "🟩🟩 Muito Alta"
    if percentage >= 60:
        return "🟩 Alta"
    if percentage >= 40:
        return "🟨🟨 Média"
    if percentage >= 20:
        return "🟧 Baixa"
    return "🟥 Muito Baixa"


def build_market_link(data: Dict[str, Any]) -> str:
    if data.get("event_slug"):
        return f"https://polymarket.com/event/{data['event_slug']}"
    if data.get("market_slug"):
        return f"https://polymarket.com/market/{data['market_slug']}"
    return "https://polymarket.com"


def build_alert_message(batch: Dict[str, Any]) -> str:
    total = float(batch.get("total_usdc") or 0)
    _, class_label = classify_entry(total)
    link = build_market_link(batch)
    end_date = batch.get("end_date") or "N/A"

    return (
        f"🎯 <b>{html.escape(str(batch['trader_name']))}</b>\n\n"
        f"{html.escape(str(batch.get('market_title') or 'Mercado desconhecido'))}\n\n"
        f"Lado: <b>{html.escape(str(batch.get('outcome') or batch.get('side') or 'N/A'))}</b>\n\n"
        f"Cotação: <b>{html.escape(format_price_and_odd(batch.get('avg_price')))}</b>\n\n"
        f"Investido: <b>{html.escape(format_money(total))}</b>\n\n"
        f"Classificação: <b>{html.escape(class_label)}</b>\n\n"
        f"Unidade sugerida: <b>{suggested_unit(total)}</b>\n\n"
        f"Confiança: <b>{html.escape(confidence_label(batch.get('avg_price')))}</b>\n\n"
        f"Execuções agrupadas: <b>{int(batch.get('source_count') or 0)}</b>\n\n"
        f"⏰ Encerra: <b>{html.escape(str(end_date))}</b>\n\n"
        f'<a href="{html.escape(link)}">🔗 Ver mercado</a>'
    )


def preview_data_from_trade(
    trade: Dict[str, Any], trader_name: str
) -> Dict[str, Any]:
    value = get_usdc_value(trade) or 0.0
    return {
        "trader_name": trader_name,
        "market_title": trade.get("title"),
        "outcome": trade.get("outcome"),
        "side": trade.get("side"),
        "avg_price": safe_float(trade.get("price")),
        "total_usdc": value,
        "source_count": 1,
        "market_slug": trade.get("slug"),
        "event_slug": trade.get("eventSlug"),
        "end_date": enrich_end_date(trade),
    }


# ============================================================
# ENVIO DE ALERTAS
# ============================================================


async def send_to_all(bot, text: str) -> bool:
    subscribers = get_subscribers()
    if not subscribers:
        logger.warning("Nenhum chat cadastrado. Use /start.")
        return False

    all_success = True
    for chat_id in subscribers:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False,
            )
        except Exception:
            all_success = False
            logger.exception("Erro enviando mensagem para %s", chat_id)
    return all_success


async def send_unsent_alerts(application: Application) -> int:
    sent = 0
    for batch in get_unsent_batches():
        if await send_to_all(application.bot, build_alert_message(batch)):
            mark_batch_sent(int(batch["id"]))
            sent += 1
    return sent


# ============================================================
# PIPELINE DE MONITORAMENTO
# ============================================================


async def process_monitoring(application: Application) -> Dict[str, int]:
    stats = {
        "wallets": 0,
        "inserted": 0,
        "bootstrapped": 0,
        "batches": 0,
        "ignored": 0,
        "sent": 0,
    }

    for wallet_row in get_active_wallets():
        stats["wallets"] += 1
        try:
            if not wallet_row["bootstrapped"]:
                count = await asyncio.to_thread(bootstrap_wallet_sync, wallet_row)
                stats["bootstrapped"] += 1
                logger.info(
                    "Bootstrap %s: %s eventos.", wallet_row["name"], count
                )
            else:
                stats["inserted"] += await asyncio.to_thread(
                    ingest_wallet_sync, wallet_row
                )
        except Exception:
            logger.exception(
                "Erro monitorando %s (%s)",
                wallet_row["name"],
                wallet_row["wallet_address"],
            )

    created, ignored = await asyncio.to_thread(prepare_alert_batches_sync)
    stats["batches"] = created
    stats["ignored"] = ignored
    stats["sent"] = await send_unsent_alerts(application)
    return stats


async def monitor_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    stats = await process_monitoring(context.application)
    logger.info("Monitor concluído: %s", stats)


# ============================================================
# RANKING ESTILO POMET
# ============================================================


PERIOD_MAP = {
    "day": "DAY",
    "dia": "DAY",
    "24h": "DAY",
    "week": "WEEK",
    "semana": "WEEK",
    "7d": "WEEK",
    "month": "MONTH",
    "mes": "MONTH",
    "mês": "MONTH",
    "30d": "MONTH",
    "all": "ALL",
    "geral": "ALL",
    "alltime": "ALL",
}

METRIC_MAP = {
    "roi": "ROI",
    "lucro": "PNL",
    "pnl": "PNL",
    "volume": "VOL",
    "apostado": "VOL",
}


def fetch_wallet_leaderboard(
    wallet_row: Dict[str, Any], period: str
) -> Dict[str, Any]:
    data = request_json(
        f"{DATA_API_BASE}/v1/leaderboard",
        {
            "category": "OVERALL",
            "timePeriod": period,
            "orderBy": "PNL",
            "limit": 1,
            "offset": 0,
            "user": wallet_row["wallet_address"],
        },
    )
    row = data[0] if isinstance(data, list) and data else {}
    pnl = float(row.get("pnl") or 0)
    volume = float(row.get("vol") or 0)
    roi = (pnl / volume * 100) if volume > 0 else 0.0
    return {
        "name": wallet_row["name"],
        "wallet": wallet_row["wallet_address"],
        "rank_global": row.get("rank") or "—",
        "pnl": pnl,
        "volume": volume,
        "roi": roi,
    }


def get_monitored_ranking(period: str, metric: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for wallet_row in get_active_wallets():
        try:
            rows.append(fetch_wallet_leaderboard(wallet_row, period))
        except Exception:
            logger.exception("Erro no ranking de %s", wallet_row["name"])
            rows.append(
                {
                    "name": wallet_row["name"],
                    "wallet": wallet_row["wallet_address"],
                    "rank_global": "—",
                    "pnl": 0.0,
                    "volume": 0.0,
                    "roi": 0.0,
                }
            )

    key = {"ROI": "roi", "PNL": "pnl", "VOL": "volume"}[metric]
    rows.sort(key=lambda item: item[key], reverse=True)
    return rows


def ranking_title(period: str, metric: str) -> str:
    periods = {"DAY": "24H", "WEEK": "7D", "MONTH": "30D", "ALL": "ALL TIME"}
    metrics = {"ROI": "ROI", "PNL": "LUCRO", "VOL": "VOLUME"}
    return f"🏆 RANKING • {periods[period]} • {metrics[metric]}"


def build_ranking_text(
    rows: List[Dict[str, Any]], period: str, metric: str, limit: int = 10
) -> str:
    lines = [f"<b>{ranking_title(period, metric)}</b>", ""]
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}

    for position, row in enumerate(rows[:limit], start=1):
        lines.extend(
            [
                f"{medals.get(position, f'#{position}')} <b>{html.escape(row['name'])}</b>",
                f"Lucro: <b>{format_money(row['pnl'])}</b>",
                f"ROI: <b>{row['roi']:+.2f}%</b>",
                f"Total apostado: <b>{format_money(row['volume'])}</b>",
                f"Ranking global: <b>#{html.escape(str(row['rank_global']))}</b>",
                "━━━━━━━━━━━━━━",
            ]
        )

    lines.append(
        "<i>ROI calculado como PnL ÷ volume retornados pela API oficial.</i>"
    )
    return "\n".join(lines)


# ============================================================
# RESUMO DIÁRIO
# ============================================================


def local_day_bounds_utc(day) -> Tuple[datetime, datetime]:
    start_local = datetime.combine(day, dt_time.min, tzinfo=BOT_TZ)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def get_daily_summary_data(day) -> Dict[str, Any]:
    start_utc, end_utc = local_day_bounds_utc(day)
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS alerts,
                    COALESCE(SUM(total_usdc), 0) AS total_volume,
                    COUNT(*) FILTER (WHERE classification = 'WHALE') AS whales,
                    COUNT(*) FILTER (WHERE classification = 'GRANDE') AS grandes,
                    COUNT(*) FILTER (WHERE classification = 'MEDIA') AS medias
                FROM alert_batches
                WHERE sent = TRUE
                  AND sent_at >= %s
                  AND sent_at < %s;
                """,
                (start_utc, end_utc),
            )
            totals = dict(cursor.fetchone())

            cursor.execute(
                """
                SELECT
                    trader_name,
                    COUNT(*) AS alerts,
                    COALESCE(SUM(total_usdc), 0) AS volume
                FROM alert_batches
                WHERE sent = TRUE
                  AND sent_at >= %s
                  AND sent_at < %s
                GROUP BY trader_name
                ORDER BY volume DESC;
                """,
                (start_utc, end_utc),
            )
            traders = list(cursor.fetchall())

    return {"totals": totals, "traders": traders}


def build_daily_summary_text(day) -> str:
    data = get_daily_summary_data(day)
    totals = data["totals"]
    lines = [
        f"📅 <b>RESUMO DIÁRIO — {day.strftime('%d/%m/%Y')}</b>",
        "",
        f"Alertas enviados: <b>{int(totals['alerts'] or 0)}</b>",
        f"Volume monitorado: <b>{format_money(totals['total_volume'])}</b>",
        f"🐋 Whales: <b>{int(totals['whales'] or 0)}</b>",
        f"🟧 Grandes: <b>{int(totals['grandes'] or 0)}</b>",
        f"🟨 Médias: <b>{int(totals['medias'] or 0)}</b>",
        "",
        "<b>Por trader:</b>",
    ]

    if not data["traders"]:
        lines.append("Nenhuma entrada acima do mínimo foi alertada hoje.")
    else:
        for index, row in enumerate(data["traders"], start=1):
            lines.append(
                f"{index}. <b>{html.escape(row['trader_name'])}</b> — "
                f"{int(row['alerts'])} alerta(s) • {format_money(row['volume'])}"
            )

    try:
        ranking = get_monitored_ranking("DAY", "ROI")[:3]
        lines.extend(["", "<b>Top 3 do dia por ROI:</b>"])
        for index, row in enumerate(ranking, start=1):
            lines.append(
                f"{index}. <b>{html.escape(row['name'])}</b> — "
                f"ROI {row['roi']:+.2f}% • Lucro {format_money(row['pnl'])}"
            )
    except Exception:
        logger.exception("Não foi possível incluir o ranking no resumo diário.")

    return "\n".join(lines)


def summary_already_sent(day) -> bool:
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM daily_summary_log WHERE summary_date = %s;",
                (day,),
            )
            return cursor.fetchone() is not None


def mark_summary_sent(day) -> None:
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO daily_summary_log (summary_date)
                VALUES (%s)
                ON CONFLICT (summary_date) DO NOTHING;
                """,
                (day,),
            )


async def daily_summary_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    day = datetime.now(BOT_TZ).date()
    if summary_already_sent(day):
        return

    text = await asyncio.to_thread(build_daily_summary_text, day)
    if await send_to_all(context.bot, text):
        mark_summary_sent(day)


async def daily_summary_catchup_job(
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    now_local = datetime.now(BOT_TZ)
    scheduled = now_local.replace(
        hour=DAILY_SUMMARY_HOUR,
        minute=DAILY_SUMMARY_MINUTE,
        second=0,
        microsecond=0,
    )
    if now_local >= scheduled:
        await daily_summary_job(context)


# ============================================================
# COMANDOS DO TELEGRAM
# ============================================================


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    save_subscriber(chat_id)
    wallets = get_active_wallets()

    text = (
        "🤖 <b>POLYMARKET SPY BOT PRO</b>\n\n"
        "Você receberá alertas automáticos acima do valor mínimo.\n\n"
        f"Wallets ativas: <b>{len(wallets)}</b>\n"
        f"Alerta mínimo: <b>{format_money(MIN_ALERT_USD)}</b>\n"
        f"Checagem: <b>{POLL_INTERVAL}s</b>\n"
        f"Resumo diário: <b>{DAILY_SUMMARY_HOUR:02d}:{DAILY_SUMMARY_MINUTE:02d} "
        f"({html.escape(BOT_TIMEZONE_NAME)})</b>\n\n"
        "<b>Comandos:</b>\n"
        "/status\n/wallets\n/addwallet Nome | 0xWallet\n"
        "/removewallet 0xWallet\n/preview\n/forcecheck\n"
        "/ranking [all|month|week|day] [roi|lucro|volume]\n"
        "/resumodiario\n/chatid"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "📡 <b>STATUS</b>\n\n"
        "✅ Bot online\n"
        f"👥 Wallets ativas: <b>{len(get_active_wallets())}</b>\n"
        f"⏳ Eventos pendentes: <b>{count_pending_events()}</b>\n"
        f"🔔 Alertas hoje: <b>{count_alerts_today()}</b>\n"
        f"⏱ Intervalo: <b>{POLL_INTERVAL}s</b>\n"
        f"💵 Alerta mínimo: <b>{format_money(MIN_ALERT_USD)}</b>\n"
        f"🟧 Grande a partir de: <b>{format_money(LARGE_ALERT_USD)}</b>\n"
        f"🐋 Whale a partir de: <b>{format_money(WHALE_ALERT_USD)}</b>\n"
        f"📅 Resumo: <b>{DAILY_SUMMARY_HOUR:02d}:{DAILY_SUMMARY_MINUTE:02d}</b>"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def chatid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f"Seu Chat ID:\n{update.effective_chat.id}")


async def wallets_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    wallets = get_active_wallets()
    if not wallets:
        await update.message.reply_text("Nenhuma wallet ativa.")
        return

    lines = ["👥 <b>WALLETS MONITORADAS</b>", ""]
    for item in wallets:
        lines.extend(
            [
                f"🎯 <b>{html.escape(item['name'])}</b>",
                f"<code>{html.escape(item['wallet_address'])}</code>",
                f"Bootstrap: {'✅' if item['bootstrapped'] else '⏳'}",
                "━━━━━━━━━━━━━━",
            ]
        )
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def addwallet(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message.text.replace("/addwallet", "", 1).strip()
    parts = [part.strip() for part in message.split("|")]
    if len(parts) != 2:
        await update.message.reply_text(
            "Use:\n/addwallet Nome | 0xWallet"
        )
        return

    name, wallet = parts[0], normalize_wallet(parts[1])
    if not re.fullmatch(r"0x[a-f0-9]{40}", wallet):
        await update.message.reply_text("Wallet inválida.")
        return

    add_wallet_db(name, wallet)
    await update.message.reply_text(
        f"✅ Wallet adicionada: {name}\n{wallet}\n"
        "O histórico recente será marcado como visto no próximo ciclo."
    )


async def removewallet(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not context.args:
        await update.message.reply_text("Use:\n/removewallet 0xWallet")
        return
    wallet = normalize_wallet(context.args[0])
    if remove_wallet_db(wallet):
        await update.message.reply_text(f"🗑 Wallet removida:\n{wallet}")
    else:
        await update.message.reply_text("Wallet não encontrada.")


async def preview(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🔎 Buscando última entrada de cada wallet...")
    for wallet_row in get_active_wallets():
        try:
            trades = await asyncio.to_thread(
                fetch_latest_trades, wallet_row["wallet_address"], 1
            )
            if not trades:
                await update.message.reply_text(
                    f"Sem atividade recente para {wallet_row['name']}."
                )
                continue
            data = preview_data_from_trade(trades[0], wallet_row["name"])
            await update.message.reply_text(
                build_alert_message(data),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False,
            )
        except Exception as exc:
            logger.exception("Erro em preview")
            await update.message.reply_text(
                f"Erro no preview de {wallet_row['name']}:\n{exc}"
            )


async def forcecheck(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    await update.message.reply_text("🔎 Checando e agrupando novas entradas...")
    try:
        stats = await process_monitoring(context.application)
        await update.message.reply_text(
            "✅ Checagem concluída.\n\n"
            f"Eventos novos: {stats['inserted']}\n"
            f"Lotes criados: {stats['batches']}\n"
            f"Alertas enviados: {stats['sent']}\n"
            f"Abaixo do mínimo descartados: {stats['ignored']}"
        )
    except Exception as exc:
        logger.exception("Erro em forcecheck")
        await update.message.reply_text(f"Erro:\n{exc}")


async def ranking_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    raw_period = context.args[0].lower() if context.args else "all"
    raw_metric = context.args[1].lower() if len(context.args) > 1 else "roi"

    period = PERIOD_MAP.get(raw_period)
    metric = METRIC_MAP.get(raw_metric)
    if not period or not metric:
        await update.message.reply_text(
            "Use:\n"
            "/ranking all roi\n"
            "/ranking month lucro\n"
            "/ranking week volume\n"
            "/ranking day roi"
        )
        return

    await update.message.reply_text("🏆 Calculando ranking...")
    rows = await asyncio.to_thread(get_monitored_ranking, period, metric)
    await update.message.reply_text(
        build_ranking_text(rows, period, metric),
        parse_mode=ParseMode.HTML,
    )


async def daily_summary_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    day = datetime.now(BOT_TZ).date()
    text = await asyncio.to_thread(build_daily_summary_text, day)
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


# ============================================================
# MAIN
# ============================================================


def main() -> None:
    criar_banco()

    threading.Thread(target=run_flask, daemon=True).start()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("chatid", chatid))
    app.add_handler(CommandHandler("wallets", wallets_command))
    app.add_handler(CommandHandler("addwallet", addwallet))
    app.add_handler(CommandHandler("removewallet", removewallet))
    app.add_handler(CommandHandler("preview", preview))
    app.add_handler(CommandHandler("forcecheck", forcecheck))
    app.add_handler(CommandHandler("ranking", ranking_command))
    app.add_handler(CommandHandler("resumodiario", daily_summary_command))

    if app.job_queue is None:
        raise RuntimeError(
            "JobQueue indisponível. Instale python-telegram-bot[job-queue]."
        )

    app.job_queue.run_repeating(
        monitor_job,
        interval=POLL_INTERVAL,
        first=10,
        name="polymarket-monitor",
    )
    app.job_queue.run_daily(
        daily_summary_job,
        time=dt_time(
            hour=DAILY_SUMMARY_HOUR,
            minute=DAILY_SUMMARY_MINUTE,
            tzinfo=BOT_TZ,
        ),
        name="daily-summary",
    )
    app.job_queue.run_once(
        daily_summary_catchup_job,
        when=30,
        name="daily-summary-catchup",
    )

    logger.info("BOT ONLINE")
    app.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
    )


if __name__ == "__main__":
    main()
