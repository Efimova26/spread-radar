"""Тесты без интернета: все четыре биржи подменяются фейковыми ответами (httpx.MockTransport)."""
import asyncio
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db import Database  # noqa: E402
from exchanges import (OKX, Binance, Bybit, ExchangeError, Kraken, Quote,  # noqa: E402
                       build_exchanges)
from service import (board_from_db, build_board, collect, due_alerts,  # noqa: E402
                     find_outliers, health_report, percentile)

# Цены bid/ask, которые «отдаёт» каждая фейковая биржа по каждой монете
PRICES = {
    "Binance": {"BTC": (67000.0, 67000.5), "ETH": (3500.0, 3500.1)},
    "Bybit":   {"BTC": (67001.0, 67001.5), "ETH": (3500.2, 3500.3)},
    "OKX":     {"BTC": (67010.0, 67010.5), "ETH": (3499.9, 3500.0)},
    "Kraken":  {"BTC": (66999.0, 67000.0), "ETH": (3500.0, 3500.4)},
}


def fake_exchanges(request: httpx.Request, prices=PRICES) -> httpx.Response:
    """Отвечает в родном формате каждой биржи."""
    host, params = request.url.host, request.url.params
    if host == "data-api.binance.vision":
        base = params["symbol"].removesuffix("USDT")
        bid, ask = prices["Binance"][base]
        return httpx.Response(200, json={"symbol": params["symbol"],
                                         "bidPrice": str(bid), "askPrice": str(ask)})
    if host == "api.bybit.com":
        base = params["symbol"].removesuffix("USDT")
        bid, ask = prices["Bybit"][base]
        return httpx.Response(200, json={"retCode": 0, "retMsg": "OK", "result": {
            "list": [{"symbol": params["symbol"], "bid1Price": str(bid), "ask1Price": str(ask)}]}})
    if host == "www.okx.com":
        base = params["instId"].split("-")[0]
        bid, ask = prices["OKX"][base]
        return httpx.Response(200, json={"code": "0", "msg": "", "data": [
            {"instId": params["instId"], "bidPx": str(bid), "askPx": str(ask)}]})
    if host == "api.kraken.com":
        pair = params["pair"]
        base = {"XBT": "BTC"}.get(pair.removesuffix("USDT"), pair.removesuffix("USDT"))
        bid, ask = prices["Kraken"][base]
        # Kraken возвращает ключ, отличный от запрошенного
        return httpx.Response(200, json={"error": [], "result": {
            f"X{pair}Z": {"a": [str(ask), "1", "1.0"], "b": [str(bid), "1", "1.0"]}}})
    return httpx.Response(404)


def client_for(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def q(exchange, bid, ask, symbol="BTC/USDT", latency=10.0):
    return Quote(exchange, symbol, bid, ask, latency)


@pytest.fixture
def db():
    database = Database(":memory:")
    yield database
    database.close()


# --- Адаптеры бирж ---

@pytest.mark.parametrize("cls,bid,ask", [
    (Binance, 67000.0, 67000.5), (Bybit, 67001.0, 67001.5),
    (OKX, 67010.0, 67010.5), (Kraken, 66999.0, 67000.0),
])
def test_each_exchange_parses_native_format(cls, bid, ask):
    quote = asyncio.run(cls(client_for(fake_exchanges)).fetch("BTC", "USDT"))
    assert (quote.symbol, quote.bid, quote.ask) == ("BTC/USDT", bid, ask)
    assert quote.exchange == cls.name and quote.latency_ms >= 0


def test_symbol_normalization():
    c = client_for(fake_exchanges)
    assert Binance(c).native_symbol("BTC", "USDT") == "BTCUSDT"
    assert OKX(c).native_symbol("BTC", "USDT") == "BTC-USDT"
    assert Kraken(c).native_symbol("BTC", "USDT") == "XBTUSDT"
    assert Kraken(c).native_symbol("ETH", "USDT") == "ETHUSDT"


def test_error_inside_http_200_body():
    # Bybit и OKX сообщают об ошибке в теле ответа при HTTP 200
    bybit = lambda r: httpx.Response(200, json={"retCode": 10001, "retMsg": "params error"})  # noqa: E731
    okx = lambda r: httpx.Response(200, json={"code": "51001", "msg": "Instrument ID does not exist"})  # noqa: E731
    kraken = lambda r: httpx.Response(200, json={"error": ["EQuery:Unknown asset pair"], "result": {}})  # noqa: E731
    for cls, handler, text in [(Bybit, bybit, "params error"), (OKX, okx, "Instrument"),
                               (Kraken, kraken, "Unknown asset pair")]:
        with pytest.raises(ExchangeError, match=text):
            asyncio.run(cls(client_for(handler)).fetch("BTC", "USDT"))


def test_crossed_book_rejected():
    bad = {**PRICES, "Binance": {"BTC": (67001.0, 67000.0)}}
    with pytest.raises(ExchangeError, match="выше ask"):
        asyncio.run(Binance(client_for(lambda r: fake_exchanges(r, bad))).fetch("BTC", "USDT"))


def test_http_error_and_broken_json():
    with pytest.raises(ExchangeError, match="HTTP 451"):
        asyncio.run(Binance(client_for(lambda r: httpx.Response(451))).fetch("BTC", "USDT"))
    with pytest.raises(ExchangeError, match="формат"):
        asyncio.run(Binance(client_for(lambda r: httpx.Response(200, json={"oops": 1}))).fetch("BTC", "USDT"))


def test_network_error():
    def down(request):
        raise httpx.ConnectTimeout("timeout")
    with pytest.raises(ExchangeError, match="нет ответа"):
        asyncio.run(OKX(client_for(down)).fetch("BTC", "USDT"))


# --- Сбор ---

def test_collect_all_sources(db):
    report = asyncio.run(collect(build_exchanges(client_for(fake_exchanges)), db, ["BTC", "ETH"], now=1000))
    assert len(report.ok) == 8 and not report.errors and not report.outliers
    assert db.stats()["ok"] == 8


def test_collect_survives_one_exchange_down(db):
    def kraken_down(request):
        if request.url.host == "api.kraken.com":
            return httpx.Response(503)
        return fake_exchanges(request)
    report = asyncio.run(collect(build_exchanges(client_for(kraken_down)), db, ["BTC"], now=1000))
    assert len(report.ok) == 3
    assert report.errors == [("Kraken", "BTC/USDT", "Kraken: HTTP 503")]
    assert db.stats()["errors"] == 1


def test_collect_flags_outlier(db):
    broken = {**PRICES, "OKX": {"BTC": (70000.0, 70000.5), "ETH": (3499.9, 3500.0)}}
    report = asyncio.run(collect(build_exchanges(client_for(lambda r: fake_exchanges(r, broken))),
                                 db, ["BTC"], now=1000))
    assert [x.exchange for x in report.outliers] == ["OKX"]
    assert db.stats()["outliers"] == 1
    # Аномальная котировка не попадает на доску
    board = board_from_db(db, "BTC/USDT", max_age_s=60, now=1010)
    assert "OKX" not in [x.exchange for x in board.quotes]


def test_outliers_need_three_sources():
    assert find_outliers([q("A", 100, 101), q("B", 200, 201)], 1.0) == []


# --- Доска и межбиржевой разрыв ---

def test_board_gap_math():
    board = build_board("BTC/USDT", [q("A", 100.0, 100.2), q("B", 100.4, 100.5)])
    assert board.best_bid.exchange == "B" and board.best_ask.exchange == "A"
    # (100.4 - 100.2) / 100.3 * 10000 ≈ 19.94 bps
    assert board.gap_bps == pytest.approx(0.2 / 100.3 * 10_000)


def test_board_no_gap_is_negative():
    board = build_board("BTC/USDT", [q("A", 100.0, 100.2), q("B", 100.1, 100.3)])
    assert board.gap_bps < 0


def test_board_needs_two_exchanges():
    assert build_board("BTC/USDT", [q("A", 100.0, 100.2)]) is None


def test_stale_quotes_are_ignored(db):
    db.save_snapshots(1000, [("A", "BTC/USDT", 100, 101, 10, "ok", None),
                             ("B", "BTC/USDT", 100, 101, 10, "ok", None)])
    assert board_from_db(db, "BTC/USDT", max_age_s=90, now=1050) is not None
    assert board_from_db(db, "BTC/USDT", max_age_s=90, now=1200) is None


def test_board_uses_latest_quote_per_exchange(db):
    db.save_snapshots(1000, [("A", "BTC/USDT", 100, 101, 10, "ok", None)])
    db.save_snapshots(1030, [("A", "BTC/USDT", 102, 103, 10, "ok", None),
                             ("B", "BTC/USDT", 102, 103, 10, "ok", None)])
    board = board_from_db(db, "BTC/USDT", max_age_s=90, now=1040)
    assert [x.bid for x in board.quotes] == [102, 102]
    assert board.age_s == pytest.approx(10)


# --- Здоровье источников ---

def test_percentile_nearest_rank():
    values = list(range(1, 101))
    assert percentile(values, 50) == 50 and percentile(values, 95) == 95
    assert percentile([42.0], 95) == 42.0 and percentile([], 50) is None


def test_health_report(db):
    rows = [("A", "BTC/USDT", 100, 101, ms, "ok", None) for ms in (10, 20, 30)]
    rows += [("A", "BTC/USDT", None, None, None, "error", "HTTP 503")]
    rows += [("B", "BTC/USDT", 100, 101, 50, "outlier", "отклонение")]
    db.save_snapshots(1000, rows)
    report = {h.exchange: h for h in health_report(db, window_s=3600, now=1100)}
    assert report["A"].uptime_pct == 75 and report["A"].p50_ms == 20 and report["A"].p95_ms == 30
    assert report["B"].uptime_pct == 100   # аномалия — это ответ, а не простой


def test_purge(db):
    db.save_snapshots(100, [("A", "BTC/USDT", 1, 2, 1, "ok", None)])
    db.save_snapshots(900, [("A", "BTC/USDT", 1, 2, 1, "ok", None)])
    assert db.purge_older_than(500) == 1 and db.stats()["snapshots"] == 1


# --- Алерты ---

def test_alerts_threshold_and_cooldown():
    board = build_board("BTC/USDT", [q("A", 100.0, 100.2), q("B", 100.4, 100.5)])   # ≈ 19.9 bps
    subs = [(1, 10.0), (2, 50.0)]
    last_sent = {}
    first = due_alerts([board], subs, last_sent, now=0, cooldown_s=600)
    assert [chat for chat, _ in first] == [1]                      # порог 50 не пробит
    assert due_alerts([board], subs, last_sent, now=300, cooldown_s=600) == []   # пауза
    assert len(due_alerts([board], subs, last_sent, now=601, cooldown_s=600)) == 1


def test_subscriptions(db):
    db.subscribe(1, 5)
    db.subscribe(1, 8)            # повторная подписка обновляет порог, а не дублирует
    assert db.subscriptions() == [(1, 8)]
    assert db.unsubscribe(1) and not db.unsubscribe(1)
