import os
import asyncio
import logging
import threading
from datetime import datetime

import requests
import psycopg2
import psycopg2.extras

from flask import Flask

from telegram import Update
from telegram.constants import ParseMode

from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

# ============================================================
# CONFIG
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")

DATABASE_URL = os.environ.get("DATABASE_URL")

TARGET_WALLET = os.environ.get(
    "TARGET_WALLET",
    "0x9b1e0334569aa1768a07705a859686aad58e82c9"
)

POLL_INTERVAL = int(
    os.environ.get("POLL_INTERVAL", "180")
)

# ============================================================
# FLASK
# ============================================================

web_app = Flask(__name__)

@web_app.route("/")
def home():

    return {
        "status": "online",
        "bot": "polymarket-spy-bot",
        "target": TARGET_WALLET
    }, 200


def run_flask():

    port = int(
        os.environ.get("PORT", 10000)
    )

    web_app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False
    )

# ============================================================
# POSTGRESQL
# ============================================================

def get_conn():

    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=psycopg2.extras.RealDictCursor
    )


def criar_banco():

    with get_conn() as conn:

        with conn.cursor() as cursor:

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS seen_trades (

                id SERIAL PRIMARY KEY,

                tx_id TEXT UNIQUE NOT NULL,

                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (

                chat_id BIGINT PRIMARY KEY,

                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)

    logger.info("Banco pronto")

# ============================================================
# HELPERS
# ============================================================

def save_subscriber(chat_id):

    with get_conn() as conn:

        with conn.cursor() as cursor:

            cursor.execute("""
            INSERT INTO subscribers (chat_id)
            VALUES (%s)
            ON CONFLICT (chat_id)
            DO NOTHING;
            """, (chat_id,))


def get_subscribers():

    with get_conn() as conn:

        with conn.cursor() as cursor:

            cursor.execute("""
            SELECT chat_id
            FROM subscribers;
            """)

            rows = cursor.fetchall()

    return [row["chat_id"] for row in rows]


def is_seen(tx_id):

    with get_conn() as conn:

        with conn.cursor() as cursor:

            cursor.execute("""
            SELECT 1
            FROM seen_trades
            WHERE tx_id=%s;
            """, (tx_id,))

            return cursor.fetchone() is not None


def mark_seen(tx_id):

    with get_conn() as conn:

        with conn.cursor() as cursor:

            cursor.execute("""
            INSERT INTO seen_trades (tx_id)
            VALUES (%s)
            ON CONFLICT (tx_id)
            DO NOTHING;
            """, (tx_id,))

# ============================================================
# POLYMARKET API
# ============================================================

def fetch_activity():

    url = "https://data-api.polymarket.com/activity"

    params = {
        "limit": 20,
        "offset": 0,
        "user": TARGET_WALLET
    }

    headers = {
        "User-Agent": (
            "Mozilla/5.0"
        )
    }

    response = requests.get(
        url,
        params=params,
        headers=headers,
        timeout=20
    )

    response.raise_for_status()

    data = response.json()

    if isinstance(data, list):
        return data

    if isinstance(data, dict):

        if "data" in data:
            return data["data"]

    return []

# ============================================================
# FORMAT
# ============================================================

def escape_md(text):

    if text is None:
        return "N/A"

    text = str(text)

    chars = r"_*[]()~`>#+-=|{}.!"

    for c in chars:
        text = text.replace(c, f"\\{c}")

    return text


def build_message(trade):

    title = trade.get(
        "title",
        "Mercado desconhecido"
    )

    side = trade.get(
        "side",
        "N/A"
    )

    outcome = trade.get(
        "outcome",
        ""
    )

    price = trade.get(
        "price",
        "N/A"
    )

    size = trade.get(
        "size",
        "N/A"
    )

    slug = trade.get("slug")

    if slug:
        link = (
            f"https://polymarket.com/market/{slug}"
        )
    else:
        link = "https://polymarket.com"

    if side.upper() == "BUY":

        if outcome.upper() == "YES":
            operation = "Comprou SIM"

        elif outcome.upper() == "NO":
            operation = "Comprou NÃO"

        else:
            operation = "Comprou"

    elif side.upper() == "SELL":
        operation = "Vendeu"

    else:
        operation = side

    message = f"""
🚨 *NOVA ENTRADA DETECTADA \\- FULLPICKS1*

*Mercado:* {escape_md(title)}

*Operação:* {escape_md(operation)}

*Preço/Cotação:* ${escape_md(price)}

*Quantidade/Valor:* {escape_md(size)}

*Link Direto:* [Abrir mercado]({link})
"""

    return message

# ============================================================
# MONITOR LOOP
# ============================================================

async def monitor_loop(app):

    logger.info(
        "Monitoramento iniciado"
    )

    while True:

        try:

            trades = await asyncio.to_thread(
                fetch_activity
            )

            trades.reverse()

            for trade in trades:

                tx_id = str(
                    trade.get(
                        "transactionHash"
                    )
                    or
                    trade.get("id")
                )

                if not tx_id:
                    continue

                if is_seen(tx_id):
                    continue

                mark_seen(tx_id)

                message = build_message(
                    trade
                )

                subscribers = (
                    get_subscribers()
                )

                for chat_id in subscribers:

                    try:

                        await app.bot.send_message(
                            chat_id=chat_id,
                            text=message,
                            parse_mode=ParseMode.MARKDOWN_V2,
                            disable_web_page_preview=False
                        )

                    except Exception:

                        logger.exception(
                            "Erro enviando alerta"
                        )

                logger.info(
                    f"Nova entrada detectada: {tx_id}"
                )

        except Exception:

            logger.exception(
                "Erro no monitoramento"
            )

        await asyncio.sleep(
            POLL_INTERVAL
        )

# ============================================================
# TELEGRAM COMMANDS
# ============================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    chat_id = (
        update.effective_chat.id
    )

    save_subscriber(chat_id)

    texto = f"""
🤖 POLYMARKET SPY BOT

Monitorando Wallet:
{TARGET_WALLET}

Você receberá alertas automáticos quando o usuário fizer novas entradas.

Comandos:

/status
/chatid
"""

    await update.message.reply_text(
        texto
    )


async def status(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    texto = f"""
📡 STATUS

✅ Bot online

🎯 Monitorando:
{TARGET_USERNAME}

⏱ Intervalo:
{POLL_INTERVAL} segundos
"""

    await update.message.reply_text(
        texto
    )


async def chatid(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    chat_id = (
        update.effective_chat.id
    )

    await update.message.reply_text(
        f"Seu Chat ID:\n{chat_id}"
    )

# ============================================================
# POST INIT
# ============================================================

async def post_init(app):

    app.create_task(
        monitor_loop(app)
    )

# ============================================================
# MAIN
# ============================================================

def main():

    criar_banco()

    flask_thread = threading.Thread(
        target=run_flask,
        daemon=True
    )

    flask_thread.start()

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(
        CommandHandler("start", start)
    )

    app.add_handler(
        CommandHandler("status", status)
    )

    app.add_handler(
        CommandHandler("chatid", chatid)
    )

    logger.info("BOT ONLINE")

    app.run_polling()

if __name__ == "__main__":
    main()
