"""База SQLite: снапшоты котировок (включая ошибки и аномалии) и подписки на алерты."""
from __future__ import annotations

import sqlite3
import time

SCHEMA = """
-- Одна строка = один ответ одной биржи по одной паре в один момент.
-- Ошибки и аномалии тоже пишем: по ним считается надёжность источника.
CREATE TABLE IF NOT EXISTS snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL NOT NULL,                 -- unix-время сбора
    exchange    TEXT NOT NULL,
    symbol      TEXT NOT NULL,                 -- единый формат "BTC/USDT"
    bid         REAL,
    ask         REAL,
    latency_ms  REAL,
    status      TEXT NOT NULL CHECK (status IN ('ok', 'outlier', 'error')),
    error       TEXT
);
CREATE INDEX IF NOT EXISTS idx_snapshots_symbol_ts ON snapshots(symbol, ts);
CREATE INDEX IF NOT EXISTS idx_snapshots_ts ON snapshots(ts);

CREATE TABLE IF NOT EXISTS subscriptions (
    chat_id        INTEGER PRIMARY KEY,
    threshold_bps  REAL NOT NULL,
    created_at     REAL NOT NULL
);
"""


class Database:
    def __init__(self, path: str = "market.db") -> None:
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)

    def save_snapshots(self, ts: float, rows: list[tuple]) -> None:
        """rows: (exchange, symbol, bid, ask, latency_ms, status, error)."""
        with self.conn:
            self.conn.executemany(
                """INSERT INTO snapshots (ts, exchange, symbol, bid, ask, latency_ms, status, error)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [(ts, *row) for row in rows],
            )

    def latest_ok(self, symbol: str, max_age_s: float, now: float | None = None) -> list[sqlite3.Row]:
        """Последняя корректная котировка каждой биржи, не старше max_age_s секунд."""
        now = time.time() if now is None else now
        return self.conn.execute(
            """SELECT s.* FROM snapshots s
               JOIN (SELECT exchange, MAX(id) AS last_id FROM snapshots
                     WHERE symbol = ? AND status = 'ok' AND ts >= ?
                     GROUP BY exchange) t ON s.id = t.last_id
               ORDER BY s.exchange""",
            (symbol, now - max_age_s),
        ).fetchall()

    def health_rows(self, since: float) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT exchange, status, latency_ms FROM snapshots WHERE ts >= ?", (since,)
        ).fetchall()

    def subscribe(self, chat_id: int, threshold_bps: float) -> None:
        with self.conn:
            self.conn.execute(
                """INSERT INTO subscriptions (chat_id, threshold_bps, created_at) VALUES (?, ?, ?)
                   ON CONFLICT(chat_id) DO UPDATE SET threshold_bps = excluded.threshold_bps""",
                (chat_id, threshold_bps, time.time()),
            )

    def unsubscribe(self, chat_id: int) -> bool:
        with self.conn:
            cur = self.conn.execute("DELETE FROM subscriptions WHERE chat_id = ?", (chat_id,))
        return cur.rowcount > 0

    def subscriptions(self) -> list[tuple[int, float]]:
        return [(r["chat_id"], r["threshold_bps"]) for r in
                self.conn.execute("SELECT chat_id, threshold_bps FROM subscriptions").fetchall()]

    def purge_older_than(self, ts: float) -> int:
        with self.conn:
            cur = self.conn.execute("DELETE FROM snapshots WHERE ts < ?", (ts,))
        return cur.rowcount

    def stats(self) -> dict[str, int]:
        one = lambda sql: self.conn.execute(sql).fetchone()[0]  # noqa: E731
        return {
            "snapshots": one("SELECT COUNT(*) FROM snapshots"),
            "ok": one("SELECT COUNT(*) FROM snapshots WHERE status = 'ok'"),
            "outliers": one("SELECT COUNT(*) FROM snapshots WHERE status = 'outlier'"),
            "errors": one("SELECT COUNT(*) FROM snapshots WHERE status = 'error'"),
            "subscribers": one("SELECT COUNT(*) FROM subscriptions"),
        }

    def close(self) -> None:
        self.conn.close()
