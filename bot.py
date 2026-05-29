import os
import re
import html
import logging
import threading
from decimal import Decimal
from typing import Any, Dict, List, Optional

import requests
import psycopg2
import psycopg2.extras

from flask import Flask

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)


# ============================================================
# CONFIGURAÇÕES GERAIS
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger("polymarket-spy-bot")

BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")

POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "180"))

SEND_BOOTSTRAP_ALERTS = (
    os.environ.get("SEND_BOOTSTRAP_ALERTS", "false").lower() == "true"
)

ALERT_CHAT_ID = os.environ.get("ALERT_CHAT_ID", "").strip()

DATA_API_BASE = "https://data-api.polymarket.com"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN não configurado.")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não configurado.")


# ============================================================
# WALLETS INICIAIS
# ============================================================

DEFAULT_WALLETS = [
    {
        "name": "FullPicks1",
        "wallet": "0x9b1e0334569aa1768a07705a859686aad58e82c9",
    },
    {
        "name": "maxgreen",
        "wallet": "0x97448b375f3702bb9e15ed619e226aaf93e0573c",
    },
    {
        "name": "Wallet 2",
        "wallet": "0x5c3a1a602848565bb16165fcd460b00c3d43020b",
    },
    {
        "name": "Wallet 3",
        "wallet": "0xba389f76b0119aed07c53c9029852664bd97e406",
    },
]


# ============================================================
# FLASK — PORTA HTTP PARA O RENDER
# ============================================================

web_app = Flask(__name__)


@web_app.route("/")
def home():
    wallets = get_active_wallets_safe()

    return {
        "status": "online",
        "bot": "polymarket-spy-bot",
        "wallets_monitoradas": len(wallets),
        "poll_interval": POLL_INTERVAL,
    }, 200


def run_flask():
    port = int(os.environ.get("PORT", 10000))

    web_app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False,
    )


# ============================================================
# BANCO POSTGRESQL
# ============================================================

def get_conn():
    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def criar_banco():
    with get_conn() as conn:
        with conn.cursor() as cursor:

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id BIGINT PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS tracked_wallets (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                wallet_address TEXT UNIQUE NOT NULL,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                bootstrapped BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS seen_trades (
                id SERIAL PRIMARY KEY,
                tx_id TEXT UNIQUE NOT NULL,
                wallet_address TEXT,
                trader_name TEXT,
                market_title TEXT,
                side TEXT,
                outcome TEXT,
                price NUMERIC(14, 6),
                size NUMERIC(20, 6),
                usdc_size NUMERIC(20, 6),
                market_slug TEXT,
                event_slug TEXT,
                raw_json JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                alerted_at TIMESTAMPTZ
            );
            """)

            cursor.execute("""
            ALTER TABLE seen_trades
            ADD COLUMN IF NOT EXISTS wallet_address TEXT;
            """)

            cursor.execute("""
            ALTER TABLE seen_trades
            ADD COLUMN IF NOT EXISTS trader_name TEXT;
            """)

            cursor.execute("""
            ALTER TABLE seen_trades
            ADD COLUMN IF NOT EXISTS market_title TEXT;
            """)

            cursor.execute("""
            ALTER TABLE seen_trades
            ADD COLUMN IF NOT EXISTS side TEXT;
            """)

            cursor.execute("""
            ALTER TABLE seen_trades
            ADD COLUMN IF NOT EXISTS outcome TEXT;
            """)

            cursor.execute("""
            ALTER TABLE seen_trades
            ADD COLUMN IF NOT EXISTS price NUMERIC(14, 6);
            """)

            cursor.execute("""
            ALTER TABLE seen_trades
            ADD COLUMN IF NOT EXISTS size NUMERIC(20, 6);
            """)

            cursor.execute("""
            ALTER TABLE seen_trades
            ADD COLUMN IF NOT EXISTS usdc_size NUMERIC(20, 6);
            """)

            cursor.execute("""
            ALTER TABLE seen_trades
            ADD COLUMN IF NOT EXISTS market_slug TEXT;
            """)

            cursor.execute("""
            ALTER TABLE seen_trades
            ADD COLUMN IF NOT EXISTS event_slug TEXT;
            """)

            cursor.execute("""
            ALTER TABLE seen_trades
            ADD COLUMN IF NOT EXISTS raw_json JSONB;
            """)

            cursor.execute("""
            ALTER TABLE seen_trades
            ADD COLUMN IF NOT EXISTS alerted_at TIMESTAMPTZ;
            """)

            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_seen_wallet
            ON seen_trades(wallet_address);
            """)

            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_wallets_active
            ON tracked_wallets(active);
            """)

    seed_default_wallets()

    logger.info("Banco pronto.")


def seed_default_wallets():
    with get_conn() as conn:
        with conn.cursor() as cursor:
            for item in DEFAULT_WALLETS:
                cursor.execute("""
                INSERT INTO tracked_wallets (
                    name,
                    wallet_address,
                    active
                )
                VALUES (%s, %s, TRUE)
                ON CONFLICT (wallet_address)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    active = TRUE,
                    updated_at = NOW();
                """, (
                    item["name"],
                    normalize_wallet(item["wallet"]),
                ))


def normalize_wallet(wallet: str) -> str:
    return wallet.strip().lower()


def get_active_wallets() -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT
                id,
                name,
                wallet_address,
                active,
                bootstrapped
            FROM tracked_wallets
            WHERE active = TRUE
            ORDER BY id ASC;
            """)

            return cursor.fetchall()


def get_active_wallets_safe() -> List[Dict[str, Any]]:
    try:
        return get_active_wallets()
    except Exception:
        return []


def add_wallet_db(name: str, wallet: str):
    wallet = normalize_wallet(wallet)

    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            INSERT INTO tracked_wallets (
                name,
                wallet_address,
                active,
                bootstrapped
            )
            VALUES (%s, %s, TRUE, FALSE)
            ON CONFLICT (wallet_address)
            DO UPDATE SET
                name = EXCLUDED.name,
                active = TRUE,
                updated_at = NOW();
            """, (name, wallet))


def remove_wallet_db(wallet: str) -> bool:
    wallet = normalize_wallet(wallet)

    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            UPDATE tracked_wallets
            SET active = FALSE,
                updated_at = NOW()
            WHERE wallet_address = %s
            RETURNING wallet_address;
            """, (wallet,))

            row = cursor.fetchone()

    return row is not None


def set_wallet_bootstrapped(wallet: str):
    wallet = normalize_wallet(wallet)

    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            UPDATE tracked_wallets
            SET bootstrapped = TRUE,
                updated_at = NOW()
            WHERE wallet_address = %s;
            """, (wallet,))


def save_subscriber(chat_id: int):
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            INSERT INTO subscribers (chat_id)
            VALUES (%s)
            ON CONFLICT (chat_id) DO NOTHING;
            """, (chat_id,))


def get_subscribers() -> List[int]:
    chat_ids = []

    if ALERT_CHAT_ID:
        for item in ALERT_CHAT_ID.split(","):
            item = item.strip()
            if item:
                try:
                    chat_ids.append(int(item))
                except Exception:
                    pass

    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT chat_id
            FROM subscribers;
            """)

            rows = cursor.fetchall()

    for row in rows:
        chat_id = int(row["chat_id"])

        if chat_id not in chat_ids:
            chat_ids.append(chat_id)

    return chat_ids


def is_seen(tx_id: str) -> bool:
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT 1
            FROM seen_trades
            WHERE tx_id = %s;
            """, (tx_id,))

            return cursor.fetchone() is not None


def mark_seen(
    trade: Dict[str, Any],
    wallet: str,
    trader_name: str,
    alerted: bool,
):
    tx_id = build_trade_id(trade, wallet)

    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            INSERT INTO seen_trades (
                tx_id,
                wallet_address,
                trader_name,
                market_title,
                side,
                outcome,
                price,
                size,
                usdc_size,
                market_slug,
                event_slug,
                raw_json,
                alerted_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                CASE WHEN %s THEN NOW() ELSE NULL END
            )
            ON CONFLICT (tx_id)
            DO NOTHING;
            """, (
                tx_id,
                normalize_wallet(wallet),
                trader_name,
                trade.get("title"),
                trade.get("side"),
                trade.get("outcome"),
                safe_float(trade.get("price")),
                safe_float(trade.get("size")),
                safe_float(get_usdc_value(trade)),
                trade.get("slug"),
                trade.get("eventSlug"),
                psycopg2.extras.Json(trade),
                alerted,
            ))


def count_seen_trades() -> int:
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT COUNT(*) AS total
            FROM seen_trades;
            """)

            row = cursor.fetchone()

    return int(row["total"] or 0)


# ============================================================
# POLYMARKET API
# ============================================================

def request_json(url: str, params: Dict[str, Any]):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Origin": "https://polymarket.com",
        "Referer": "https://polymarket.com/",
    }

    response = requests.get(
        url,
        params=params,
        headers=headers,
        timeout=25,
    )

    response.raise_for_status()

    return response.json()


def fetch_activity(wallet: str, limit: int = 20) -> List[Dict[str, Any]]:
    params = {
        "user": normalize_wallet(wallet),
        "limit": limit,
        "offset": 0,
        "type": "TRADE",
        "sortBy": "TIMESTAMP",
        "sortDirection": "DESC",
    }

    data = request_json(
        f"{DATA_API_BASE}/activity",
        params=params,
    )

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        return data.get("data", [])

    return []


def fetch_trades_fallback(wallet: str, limit: int = 20) -> List[Dict[str, Any]]:
    params = {
        "user": normalize_wallet(wallet),
        "limit": limit,
        "offset": 0,
        "takerOnly": "false",
    }

    data = request_json(
        f"{DATA_API_BASE}/trades",
        params=params,
    )

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        return data.get("data", [])

    return []


def fetch_latest_trades(wallet: str, limit: int = 20) -> List[Dict[str, Any]]:
    try:
        trades = fetch_activity(wallet, limit=limit)

        if trades:
            return trades

        logger.warning(
            f"/activity retornou vazio para {wallet}. Tentando /trades."
        )

        return fetch_trades_fallback(wallet, limit=limit)

    except Exception as error:
        logger.warning(
            f"Erro no /activity para {wallet}: {error}. Tentando /trades."
        )

        return fetch_trades_fallback(wallet, limit=limit)


# ============================================================
# FORMATAÇÃO ESTILO POMET
# ============================================================

def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None

    try:
        if isinstance(value, Decimal):
            return float(value)

        return float(value)

    except Exception:
        return None


def format_money(value: Any) -> str:
    value = safe_float(value)

    if value is None:
        return "N/A"

    return f"${value:,.0f}"


def format_price_and_odd(price: Any) -> str:
    price = safe_float(price)

    if price is None or price <= 0:
        return "N/A"

    percentage = price * 100
    odd = 1 / price

    return f"{percentage:.1f}% (Odd {odd:.2f})"


def confidence_label(price: Any) -> str:
    price = safe_float(price)

    if price is None:
        return "N/A"

    percentage = price * 100

    if percentage >= 80:
        return "🟩🟩 Muito Alta"

    if percentage >= 60:
        return "🟩 Alta"

    if percentage >= 40:
        return "🟨🟨 Média"

    if percentage >= 20:
        return "🟧 Baixa"

    return "🟥 Muito Baixa"


def suggested_unit(usdc_value: Any) -> str:
    value = safe_float(usdc_value)

    if value is None:
        return "N/A"

    if value >= 30000:
        return "2.0"

    if value >= 10000:
        return "1.0"

    if value >= 5000:
        return "0.75"

    if value >= 1000:
        return "0.5"

    return "0.25"


def get_usdc_value(trade: Dict[str, Any]) -> Optional[float]:
    possible_keys = [
        "usdcSize",
        "usdc_size",
        "value",
        "amount",
        "cost",
    ]

    for key in possible_keys:
        value = safe_float(trade.get(key))

        if value is not None:
            return value

    size = safe_float(trade.get("size"))
    price = safe_float(trade.get("price"))

    if size is not None and price is not None:
        return size * price

    return None


def extract_close_date(trade: Dict[str, Any]) -> str:
    possible_keys = [
        "endDate",
        "end_date",
        "endDateIso",
        "endDateISO",
        "marketEndDate",
        "closeTime",
    ]

    for key in possible_keys:
        value = trade.get(key)

        if value:
            return str(value)[:10]

    title = str(trade.get("title") or "")

    match = re.search(r"(20\d{2}-\d{2}-\d{2})", title)

    if match:
        return match.group(1)

    return "N/A"


def build_trade_id(trade: Dict[str, Any], wallet: str) -> str:
    tx_hash = (
        trade.get("transactionHash")
        or trade.get("transaction_hash")
        or trade.get("hash")
        or trade.get("id")
    )

    parts = [
        normalize_wallet(wallet),
        str(tx_hash or ""),
        str(trade.get("conditionId", "")),
        str(trade.get("asset", "")),
        str(trade.get("outcome", "")),
        str(trade.get("side", "")),
        str(trade.get("price", "")),
        str(trade.get("size", "")),
        str(trade.get("timestamp", "")),
    ]

    return "|".join(parts)


def build_market_link(trade: Dict[str, Any]) -> str:
    event_slug = trade.get("eventSlug")
    slug = trade.get("slug")

    if event_slug:
        return f"https://polymarket.com/event/{event_slug}"

    if slug:
        return f"https://polymarket.com/market/{slug}"

    condition_id = trade.get("conditionId")

    if condition_id:
        return f"https://polymarket.com/search?query={condition_id}"

    return "https://polymarket.com"


def build_message(trade: Dict[str, Any], trader_name: str) -> str:
    title = trade.get("title", "Mercado desconhecido")
    outcome = trade.get("outcome", "N/A")
    price = trade.get("price", None)
    invested = get_usdc_value(trade)
    close_date = extract_close_date(trade)
    link = build_market_link(trade)

    message = f"""
🎯 <b>{html.escape(trader_name)}</b>

{html.escape(str(title))}

Lado: <b>{html.escape(str(outcome))}</b>

Cotação: <b>{html.escape(format_price_and_odd(price))}</b>

Investido: <b>{html.escape(format_money(invested))}</b>

Unidade sugerida: <b>{html.escape(suggested_unit(invested))}</b>

Confiança: <b>{html.escape(confidence_label(price))}</b>

⏰ Encerra: <b>{html.escape(close_date)}</b>

<a href="{html.escape(link)}">🔗 Ver mercado</a>
"""

    return message.strip()


# ============================================================
# ENVIO E MONITORAMENTO
# ============================================================

async def send_to_all(bot, message: str):
    subscribers = get_subscribers()

    if not subscribers:
        logger.warning("Nenhum subscriber cadastrado. Use /start no Telegram.")
        return

    for chat_id in subscribers:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False,
            )

        except Exception:
            logger.exception(f"Erro ao enviar para chat_id={chat_id}")


def bootstrap_wallet_sync(wallet_row: Dict[str, Any]):
    wallet = wallet_row["wallet_address"]
    trader_name = wallet_row["name"]

    trades = fetch_latest_trades(wallet, limit=20)

    for trade in trades:
        mark_seen(
            trade=trade,
            wallet=wallet,
            trader_name=trader_name,
            alerted=False,
        )

    set_wallet_bootstrapped(wallet)

    logger.info(
        f"Bootstrap feito para {trader_name} ({wallet}). Trades marcados: {len(trades)}"
    )


def get_new_trades_for_wallet_sync(wallet_row: Dict[str, Any]) -> List[Dict[str, Any]]:
    wallet = wallet_row["wallet_address"]

    trades = fetch_latest_trades(wallet, limit=20)

    new_trades = []

    for trade in trades:
        tx_id = build_trade_id(trade, wallet)

        if not is_seen(tx_id):
            new_trades.append(trade)

    new_trades.sort(
        key=lambda item: int(item.get("timestamp") or 0)
    )

    return new_trades


async def process_monitoring(application: Application):
    wallets = get_active_wallets()

    if not wallets:
        logger.warning("Nenhuma wallet ativa cadastrada.")
        return

    for wallet_row in wallets:
        wallet = wallet_row["wallet_address"]
        trader_name = wallet_row["name"]
        bootstrapped = wallet_row["bootstrapped"]

        try:
            if not bootstrapped and not SEND_BOOTSTRAP_ALERTS:
                await application.bot.send_message(
                    chat_id=get_subscribers()[0],
                    text=(
                        f"✅ Monitoramento iniciado para {trader_name}.\n\n"
                        "Entradas antigas foram marcadas como vistas. "
                        "A partir de agora, só novas entradas serão alertadas."
                    ),
                ) if get_subscribers() else None

                await application.create_task(
                    async_noop()
                )

                await run_in_thread(
                    bootstrap_wallet_sync,
                    wallet_row,
                )

                continue

            new_trades = await run_in_thread(
                get_new_trades_for_wallet_sync,
                wallet_row,
            )

            if new_trades:
                logger.info(
                    f"{len(new_trades)} novas entradas para {trader_name}."
                )

            for trade in new_trades:
                message = build_message(
                    trade=trade,
                    trader_name=trader_name,
                )

                await send_to_all(
                    application.bot,
                    message,
                )

                await run_in_thread(
                    mark_seen,
                    trade,
                    wallet,
                    trader_name,
                    True,
                )

        except Exception:
            logger.exception(
                f"Erro monitorando {trader_name} ({wallet})."
            )


async def run_in_thread(func, *args, **kwargs):
    import asyncio
    return await asyncio.to_thread(func, *args, **kwargs)


async def async_noop():
    return None


async def monitor_job(context: ContextTypes.DEFAULT_TYPE):
    await process_monitoring(context.application)


# ============================================================
# COMANDOS TELEGRAM
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    save_subscriber(chat_id)

    wallets = get_active_wallets()

    text = (
        "🤖 <b>POLYMARKET SPY BOT</b>\n\n"
        "Você foi cadastrado para receber alertas automáticos.\n\n"
        f"Wallets monitoradas: <b>{len(wallets)}</b>\n"
        f"Intervalo de checagem: <b>{POLL_INTERVAL}s</b>\n\n"
        "<b>Comandos:</b>\n"
        "/status\n"
        "/wallets\n"
        "/addwallet Nome | 0xWallet\n"
        "/removewallet 0xWallet\n"
        "/preview\n"
        "/forcecheck\n"
        "/chatid"
    )

    await update.message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wallets = get_active_wallets()
    seen = count_seen_trades()
    subscribers = get_subscribers()

    text = (
        "📡 <b>STATUS</b>\n\n"
        "✅ Bot online\n\n"
        f"👥 Wallets ativas: <b>{len(wallets)}</b>\n"
        f"🧾 Trades vistos: <b>{seen}</b>\n"
        f"🔔 Chats cadastrados: <b>{len(subscribers)}</b>\n"
        f"⏱ Intervalo: <b>{POLL_INTERVAL}s</b>\n"
    )

    await update.message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
    )


async def chatid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    await update.message.reply_text(
        f"Seu Chat ID:\n{chat_id}"
    )


async def wallets_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wallets = get_active_wallets()

    if not wallets:
        await update.message.reply_text("Nenhuma wallet ativa.")
        return

    text = "👥 <b>WALLETS MONITORADAS</b>\n\n"

    for item in wallets:
        text += (
            f"🎯 <b>{html.escape(item['name'])}</b>\n"
            f"<code>{html.escape(item['wallet_address'])}</code>\n"
            f"Bootstrap: {'✅' if item['bootstrapped'] else '⏳'}\n"
            "━━━━━━━━━━━━━━\n"
        )

    await update.message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
    )


async def addwallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        message = update.message.text.replace("/addwallet", "", 1).strip()

        parts = [p.strip() for p in message.split("|")]

        if len(parts) != 2:
            await update.message.reply_text(
                "Formato inválido.\n\nUse:\n/addwallet Nome | 0xWallet"
            )
            return

        name = parts[0]
        wallet = normalize_wallet(parts[1])

        if not wallet.startswith("0x") or len(wallet) != 42:
            await update.message.reply_text(
                "Wallet inválida. Ela precisa começar com 0x e ter 42 caracteres."
            )
            return

        add_wallet_db(name, wallet)

        await update.message.reply_text(
            f"✅ Wallet adicionada:\n\n{name}\n{wallet}\n\n"
            "Ela será monitorada automaticamente. As entradas antigas serão marcadas como vistas."
        )

    except Exception as error:
        logger.exception("Erro no /addwallet")
        await update.message.reply_text(f"Erro:\n{error}")


async def removewallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Use:\n/removewallet 0xWallet"
        )
        return

    wallet = normalize_wallet(context.args[0])

    removed = remove_wallet_db(wallet)

    if removed:
        await update.message.reply_text(
            f"🗑 Wallet removida do monitoramento:\n{wallet}"
        )
    else:
        await update.message.reply_text(
            "Wallet não encontrada."
        )


async def preview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wallets = get_active_wallets()

    if not wallets:
        await update.message.reply_text("Nenhuma wallet ativa.")
        return

    await update.message.reply_text(
        "🔎 Buscando última entrada de cada wallet..."
    )

    for wallet_row in wallets:
        wallet = wallet_row["wallet_address"]
        trader_name = wallet_row["name"]

        try:
            trades = await run_in_thread(
                fetch_latest_trades,
                wallet,
                1,
            )

            if not trades:
                await update.message.reply_text(
                    f"Sem atividade recente para {trader_name}."
                )
                continue

            message = build_message(
                trade=trades[0],
                trader_name=trader_name,
            )

            await update.message.reply_text(
                message,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False,
            )

        except Exception as error:
            await update.message.reply_text(
                f"Erro no preview de {trader_name}:\n{error}"
            )


async def forcecheck(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🔎 Fazendo checagem manual de novas entradas..."
    )

    try:
        await process_monitoring(context.application)

        await update.message.reply_text(
            "✅ Checagem manual concluída."
        )

    except Exception as error:
        logger.exception("Erro no /forcecheck")
        await update.message.reply_text(
            f"Erro no forcecheck:\n{error}"
        )


# ============================================================
# MAIN
# ============================================================

def main():
    criar_banco()

    flask_thread = threading.Thread(
        target=run_flask,
        daemon=True,
    )

    flask_thread.start()

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("chatid", chatid))
    app.add_handler(CommandHandler("wallets", wallets_command))
    app.add_handler(CommandHandler("addwallet", addwallet))
    app.add_handler(CommandHandler("removewallet", removewallet))
    app.add_handler(CommandHandler("preview", preview))
    app.add_handler(CommandHandler("forcecheck", forcecheck))

    if app.job_queue is None:
        raise RuntimeError(
            "JobQueue não está disponível. Confira se requirements.txt usa python-telegram-bot[job-queue]."
        )

    app.job_queue.run_repeating(
        monitor_job,
        interval=POLL_INTERVAL,
        first=10,
        name="polymarket-monitor",
    )

    logger.info("BOT ONLINE")

    app.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
    )


if __name__ == "__main__":
    main()
