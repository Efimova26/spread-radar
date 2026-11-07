"""Telegram-бот: обвязка вокруг service.py (команды, форматирование, расписание сбора)."""
from __future__ import annotations

import html
import logging
import os
import time
from datetime import timedelta

import httpx
from dotenv import load_dotenv
from telegram import Update
from telegram.constants import ParseMode
from telegram.error import Forbidden
from telegram.ext import (Application, ApplicationBuilder, CommandHandler, ContextTypes)

from db import Database
from exchanges import build_exchanges
from service import (QUOTE_CURRENCY, Board, board_from_db, boards_from_quotes, collect,
                     due_alerts, health_report)

load_dotenv()
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s: %(message)s", level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)  # не писать в лог каждый запрос с токеном
logging.getLogger("apscheduler").setLevel(logging.WARNING)  # не шуметь о каждом запуске задачи
log = logging.getLogger("bot")

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
DB_PATH = os.environ.get("DB_PATH", "market.db")
ASSETS = [a.strip().upper() for a in os.environ.get("ASSETS", "BTC,ETH,SOL").split(",") if a.strip()]
COLLECT_SECONDS = float(os.environ.get("COLLECT_SECONDS", "30"))
OUTLIER_PCT = float(os.environ.get("OUTLIER_PCT", "1.0"))
ALERT_COOLDOWN_MIN = float(os.environ.get("ALERT_COOLDOWN_MIN", "10"))
RETENTION_DAYS = float(os.environ.get("RETENTION_DAYS", "7"))
MAX_QUOTE_AGE_S = COLLECT_SECONDS * 3   # котировки старше трёх циклов сбора считаем устаревшими

HELP_TEXT = (
    "Слежу за ценами на биржах Binance, Bybit, OKX и Kraken.\n\n"
    f"/price — котировки по {', '.join(ASSETS)}\n"
    "/price BTC — одна монета\n"
    "/latency — скорость и надёжность бирж за час\n"
    "/alert 5 — сообщить, когда разрыв цен между биржами превысит 5 bps\n"
    "/alert off — отключить уведомления\n"
    "/stats — что накоплено в базе\n\n"
    "1 bps = 0,01%. Разрыв считается без учёта комиссий, это не торговый сигнал."
)


def fmt_price(x: float) -> str:
    return f"{x:.2f}" if x >= 100 else f"{x:.4f}"


def format_board(b: Board) -> str:
    lines = [f"<b>{html.escape(b.symbol)}</b> · данные {b.age_s:.0f} с назад", "<pre>"]
    lines.append(f"{'Биржа':<8}{'Bid':>12}{'Ask':>12}{'мс':>6}")
    for q in b.quotes:
        lines.append(f"{q.exchange:<8}{fmt_price(q.bid):>12}{fmt_price(q.ask):>12}{q.latency_ms:>6.0f}")
    lines.append("</pre>")
    lines.append(f"Лучший bid: {b.best_bid.exchange} {fmt_price(b.best_bid.bid)}")
    lines.append(f"Лучший ask: {b.best_ask.exchange} {fmt_price(b.best_ask.ask)}")
    if b.gap_bps > 0:
        lines.append(f"Межбиржевой разрыв: <b>+{b.gap_bps:.1f} bps</b> "
                     f"(bid на {b.best_bid.exchange} выше ask на {b.best_ask.exchange})")
    else:
        lines.append(f"Разрыва нет, рынки согласованы ({b.gap_bps:.1f} bps)")
    return "\n".join(lines)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Привет! " + HELP_TEXT)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(HELP_TEXT)


async def price_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Database = context.bot_data["db"]
    assets = [a.upper() for a in context.args] or ASSETS
    unknown = [a for a in assets if a not in ASSETS]
    if unknown:
        await update.message.reply_text(
            f"Не слежу за {', '.join(unknown)}. Доступно: {', '.join(ASSETS)}.")
        return
    parts = []
    for asset in assets:
        symbol = f"{asset}/{QUOTE_CURRENCY}"
        board = board_from_db(db, symbol, MAX_QUOTE_AGE_S)
        parts.append(format_board(board) if board else
                     f"<b>{symbol}</b>: свежих котировок меньше чем с двух бирж, подожди сбора.")
    await update.message.reply_text("\n\n".join(parts), parse_mode=ParseMode.HTML)


async def latency_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    report = health_report(context.bot_data["db"])
    if not report:
        await update.message.reply_text("Данных пока нет, подожди первого сбора.")
        return
    lines = ["<b>Биржи за последний час</b>", "<pre>",
             f"{'Биржа':<8}{'доступн.':>9}{'p50 мс':>8}{'p95 мс':>8}"]
    for h in report:
        p50 = f"{h.p50_ms:.0f}" if h.p50_ms is not None else "—"
        p95 = f"{h.p95_ms:.0f}" if h.p95_ms is not None else "—"
        lines.append(f"{h.exchange:<8}{h.uptime_pct:>8.1f}%{p50:>8}{p95:>8}")
    lines.append("</pre>")
    lines.append("p50 — типичная задержка, p95 — задержка в худших 5% запросов.")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def alert_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Database = context.bot_data["db"]
    chat_id = update.effective_chat.id
    arg = context.args[0].lower() if context.args else ""
    if arg == "off":
        removed = db.unsubscribe(chat_id)
        await update.message.reply_text("Уведомления отключены." if removed else "Подписки не было.")
        return
    try:
        threshold = float(arg.replace(",", "."))
    except ValueError:
        threshold = 0
    if not 0 < threshold <= 1000:
        await update.message.reply_text("Укажи порог в bps от 0 до 1000, например /alert 5")
        return
    db.subscribe(chat_id, threshold)
    await update.message.reply_text(
        f"Готово: напишу, когда разрыв превысит {threshold:g} bps. "
        f"Не чаще раза в {ALERT_COOLDOWN_MIN:g} мин по каждой монете.")


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    s = context.bot_data["db"].stats()
    await update.message.reply_text(
        f"Снапшотов в базе: {s['snapshots']}\n"
        f"Корректных: {s['ok']}, аномалий: {s['outliers']}, ошибок: {s['errors']}\n"
        f"Подписчиков на алерты: {s['subscribers']}\n"
        f"Храню данные {RETENTION_DAYS:g} дн., сбор каждые {COLLECT_SECONDS:g} с.")


async def collect_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Database = context.bot_data["db"]
    report = await collect(context.bot_data["exchanges"], db, ASSETS, OUTLIER_PCT)
    alerts = due_alerts(boards_from_quotes(report.ok), db.subscriptions(),
                        context.bot_data["last_sent"], time.time(), ALERT_COOLDOWN_MIN * 60)
    for chat_id, board in alerts:
        text = (f"⚡ {board.symbol}: bid на {board.best_bid.exchange} "
                f"({fmt_price(board.best_bid.bid)}) выше ask на {board.best_ask.exchange} "
                f"({fmt_price(board.best_ask.ask)}) на {board.gap_bps:.1f} bps. Без учёта комиссий.")
        try:
            await context.bot.send_message(chat_id, text)
        except Forbidden:  # пользователь заблокировал бота — убираем подписку
            db.unsubscribe(chat_id)


async def purge_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    removed = context.bot_data["db"].purge_older_than(time.time() - RETENTION_DAYS * 86400)
    log.info("Purged %d old snapshots", removed)


async def post_init(app: Application) -> None:
    app.bot_data["db"] = Database(DB_PATH)
    app.bot_data["http"] = httpx.AsyncClient(headers={"User-Agent": "market-monitor-bot"})
    app.bot_data["exchanges"] = build_exchanges(app.bot_data["http"])
    app.bot_data["last_sent"] = {}
    app.job_queue.run_repeating(collect_job, interval=COLLECT_SECONDS, first=1)
    app.job_queue.run_repeating(purge_job, interval=timedelta(hours=6), first=timedelta(minutes=5))
    log.info("Bot started: assets=%s, every %ss, DB=%s", ASSETS, COLLECT_SECONDS, DB_PATH)


async def post_shutdown(app: Application) -> None:
    await app.bot_data["http"].aclose()
    app.bot_data["db"].close()


def main() -> None:
    if not TOKEN:
        raise SystemExit("Нет TELEGRAM_BOT_TOKEN: создай файл .env по образцу .env.example")
    app = (ApplicationBuilder().token(TOKEN)
           .post_init(post_init).post_shutdown(post_shutdown).build())
    for name, handler in [("start", start), ("help", help_cmd), ("price", price_cmd),
                          ("latency", latency_cmd), ("alert", alert_cmd), ("stats", stats_cmd)]:
        app.add_handler(CommandHandler(name, handler))
    app.run_polling()


if __name__ == "__main__":
    main()
