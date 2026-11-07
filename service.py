"""Логика без Telegram: сбор котировок, отсев аномалий, межбиржевой разрыв, здоровье источников, алерты."""
from __future__ import annotations

import asyncio
import logging
import math
import statistics
import time
from dataclasses import dataclass, field

from db import Database
from exchanges import Exchange, ExchangeError, Quote

log = logging.getLogger(__name__)

QUOTE_CURRENCY = "USDT"


@dataclass
class CollectReport:
    ok: list[Quote] = field(default_factory=list)
    outliers: list[Quote] = field(default_factory=list)
    errors: list[tuple[str, str, str]] = field(default_factory=list)   # (exchange, symbol, error)


@dataclass(frozen=True)
class Board:
    symbol: str
    quotes: list[Quote]
    best_bid: Quote       # где дороже всего покупают
    best_ask: Quote       # где дешевле всего продают
    gap_bps: float        # (best bid - best ask) / mid, в базисных пунктах
    age_s: float = 0.0    # возраст самой старой котировки на доске


@dataclass(frozen=True)
class Health:
    exchange: str
    requests: int
    uptime_pct: float          # доля запросов, на которые пришёл ответ
    p50_ms: float | None
    p95_ms: float | None


# ---------- Сбор ----------

def find_outliers(quotes: list[Quote], max_dev_pct: float) -> list[Quote]:
    """Котировки, середина которых отходит от медианы по биржам больше чем на max_dev_pct %.

    Медиана, а не среднее: одна сломанная биржа не сдвигает ориентир.
    Нужно минимум 3 источника, иначе непонятно, кто из двух ошибается.
    """
    if len(quotes) < 3:
        return []
    median_mid = statistics.median(q.mid for q in quotes)
    return [q for q in quotes if abs(q.mid - median_mid) / median_mid * 100 > max_dev_pct]


async def collect(exchanges: list[Exchange], db: Database, bases: list[str],
                  max_dev_pct: float = 1.0, now: float | None = None) -> CollectReport:
    """Опрашивает все биржи по всем парам параллельно и сохраняет результат."""
    now = time.time() if now is None else now
    jobs = [(ex, base) for base in bases for ex in exchanges]
    # Параллельно: общее время сбора ≈ самая медленная биржа, а не сумма всех
    results = await asyncio.gather(*(ex.fetch(base, QUOTE_CURRENCY) for ex, base in jobs),
                                   return_exceptions=True)

    report = CollectReport()
    by_symbol: dict[str, list[Quote]] = {}
    for (ex, base), result in zip(jobs, results):
        symbol = f"{base}/{QUOTE_CURRENCY}"
        if isinstance(result, Quote):
            by_symbol.setdefault(symbol, []).append(result)
        elif isinstance(result, ExchangeError):
            report.errors.append((ex.name, symbol, str(result)))
        else:  # непредвиденная ошибка в коде — не роняем весь сбор, но громко пишем в лог
            log.exception("Unexpected error from %s %s", ex.name, symbol, exc_info=result)
            report.errors.append((ex.name, symbol, f"{ex.name}: внутренняя ошибка"))

    for quotes in by_symbol.values():
        bad = find_outliers(quotes, max_dev_pct)
        report.outliers.extend(bad)
        report.ok.extend(q for q in quotes if q not in bad)

    rows = [(q.exchange, q.symbol, q.bid, q.ask, q.latency_ms, "ok", None) for q in report.ok]
    rows += [(q.exchange, q.symbol, q.bid, q.ask, q.latency_ms, "outlier",
              "отклонение от медианы") for q in report.outliers]
    rows += [(ex, sym, None, None, None, "error", err) for ex, sym, err in report.errors]
    db.save_snapshots(now, rows)
    log.info("Collect: ok=%d outliers=%d errors=%d",
             len(report.ok), len(report.outliers), len(report.errors))
    return report


# ---------- Аналитика ----------

def build_board(symbol: str, quotes: list[Quote], age_s: float = 0.0) -> Board | None:
    """Сводит котировки бирж по одной паре. Нужно минимум 2 биржи."""
    if len(quotes) < 2:
        return None
    best_bid = max(quotes, key=lambda q: q.bid)
    best_ask = min(quotes, key=lambda q: q.ask)
    mid = (best_bid.bid + best_ask.ask) / 2
    gap_bps = (best_bid.bid - best_ask.ask) / mid * 10_000
    return Board(symbol, sorted(quotes, key=lambda q: q.exchange), best_bid, best_ask, gap_bps, age_s)


def boards_from_quotes(quotes: list[Quote]) -> list[Board]:
    by_symbol: dict[str, list[Quote]] = {}
    for q in quotes:
        by_symbol.setdefault(q.symbol, []).append(q)
    boards = [build_board(sym, qs) for sym, qs in sorted(by_symbol.items())]
    return [b for b in boards if b]


def board_from_db(db: Database, symbol: str, max_age_s: float, now: float | None = None) -> Board | None:
    now = time.time() if now is None else now
    rows = db.latest_ok(symbol, max_age_s, now)
    quotes = [Quote(r["exchange"], r["symbol"], r["bid"], r["ask"], r["latency_ms"]) for r in rows]
    age = max((now - r["ts"] for r in rows), default=0.0)
    return build_board(symbol, quotes, age)


def percentile(values: list[float], p: float) -> float | None:
    """Перцентиль по методу ближайшего ранга: p95 = значение, ниже которого 95% замеров."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(p / 100 * len(ordered)))
    return ordered[rank - 1]


def health_report(db: Database, window_s: float = 3600, now: float | None = None) -> list[Health]:
    now = time.time() if now is None else now
    per_exchange: dict[str, list] = {}
    for row in db.health_rows(now - window_s):
        per_exchange.setdefault(row["exchange"], []).append(row)

    report = []
    for name, rows in sorted(per_exchange.items()):
        # Аномалия — это ответ, пусть и плохой: в доступность её засчитываем, ошибку — нет
        answered = [r for r in rows if r["status"] != "error"]
        latencies = [r["latency_ms"] for r in answered]
        report.append(Health(
            exchange=name,
            requests=len(rows),
            uptime_pct=100 * len(answered) / len(rows),
            p50_ms=percentile(latencies, 50),
            p95_ms=percentile(latencies, 95),
        ))
    return report


# ---------- Алерты ----------

def due_alerts(boards: list[Board], subscriptions: list[tuple[int, float]],
               last_sent: dict[tuple[int, str], float], now: float,
               cooldown_s: float) -> list[tuple[int, Board]]:
    """Кому и о чём сообщить. Пауза cooldown_s на пару, чтобы не заспамить одним и тем же разрывом."""
    alerts = []
    for chat_id, threshold in subscriptions:
        for board in boards:
            if board.gap_bps < threshold:
                continue
            key = (chat_id, board.symbol)
            if now - last_sent.get(key, -math.inf) < cooldown_s:
                continue
            last_sent[key] = now
            alerts.append((chat_id, board))
    return alerts
