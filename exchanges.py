"""Адаптеры публичных API бирж: у каждой свой формат символа и ответа, наружу — единый Quote."""
from __future__ import annotations

import time
from dataclasses import dataclass

import httpx

REQUEST_TIMEOUT = 5.0


class ExchangeError(Exception):
    """Биржа не ответила, ответила ошибкой или прислала некорректные данные."""


@dataclass(frozen=True)
class Quote:
    exchange: str
    symbol: str        # единый формат: "BTC/USDT"
    bid: float         # лучшая цена покупки
    ask: float         # лучшая цена продажи
    latency_ms: float  # время ответа API

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2


class Exchange:
    """Общая часть: запрос, замер задержки, проверка данных. Различия — в трёх методах ниже."""

    name = "base"

    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client

    def native_symbol(self, base: str, quote: str) -> str:
        raise NotImplementedError

    def request(self, native: str) -> tuple[str, dict]:
        raise NotImplementedError

    def parse(self, payload: dict, native: str) -> tuple[float, float]:
        raise NotImplementedError

    async def fetch(self, base: str, quote: str) -> Quote:
        native = self.native_symbol(base, quote)
        url, params = self.request(native)
        started = time.perf_counter()
        try:
            response = await self.client.get(url, params=params, timeout=REQUEST_TIMEOUT)
        except httpx.HTTPError as exc:
            raise ExchangeError(f"{self.name}: нет ответа ({exc.__class__.__name__})") from exc
        latency_ms = (time.perf_counter() - started) * 1000

        if response.status_code != 200:
            raise ExchangeError(f"{self.name}: HTTP {response.status_code}")
        try:
            bid, ask = self.parse(response.json(), native)
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise ExchangeError(f"{self.name}: неожиданный формат ответа") from exc

        # Проверка здравого смысла: цены положительные, bid не выше ask
        if bid <= 0 or ask <= 0:
            raise ExchangeError(f"{self.name}: неположительная цена (bid={bid}, ask={ask})")
        if bid > ask:
            raise ExchangeError(f"{self.name}: bid {bid} выше ask {ask}, данные некорректны")
        return Quote(self.name, f"{base}/{quote}", bid, ask, latency_ms)


class Binance(Exchange):
    # data-api.binance.vision — официальный адрес Binance только для публичных рыночных данных
    name = "Binance"

    def native_symbol(self, base, quote):
        return f"{base}{quote}"                      # BTCUSDT

    def request(self, native):
        return "https://data-api.binance.vision/api/v3/ticker/bookTicker", {"symbol": native}

    def parse(self, payload, native):
        return float(payload["bidPrice"]), float(payload["askPrice"])


class Bybit(Exchange):
    name = "Bybit"

    def native_symbol(self, base, quote):
        return f"{base}{quote}"                      # BTCUSDT

    def request(self, native):
        return "https://api.bybit.com/v5/market/tickers", {"category": "spot", "symbol": native}

    def parse(self, payload, native):
        # Bybit отвечает HTTP 200 даже при ошибке, сама ошибка — в retCode
        if payload["retCode"] != 0:
            raise ExchangeError(f"Bybit: {payload.get('retMsg', 'ошибка API')}")
        item = payload["result"]["list"][0]
        return float(item["bid1Price"]), float(item["ask1Price"])


class OKX(Exchange):
    name = "OKX"

    def native_symbol(self, base, quote):
        return f"{base}-{quote}"                     # BTC-USDT

    def request(self, native):
        return "https://www.okx.com/api/v5/market/ticker", {"instId": native}

    def parse(self, payload, native):
        # Как и у Bybit: код ошибки внутри тела, "0" — успех
        if payload["code"] != "0":
            raise ExchangeError(f"OKX: {payload.get('msg', 'ошибка API')}")
        item = payload["data"][0]
        return float(item["bidPx"]), float(item["askPx"])


class Kraken(Exchange):
    name = "Kraken"
    ALIASES = {"BTC": "XBT"}                         # у Kraken биткоин называется XBT

    def native_symbol(self, base, quote):
        return f"{self.ALIASES.get(base, base)}{quote}"   # XBTUSDT

    def request(self, native):
        return "https://api.kraken.com/0/public/Ticker", {"pair": native}

    def parse(self, payload, native):
        if payload["error"]:
            raise ExchangeError(f"Kraken: {', '.join(payload['error'])}")
        # Ключ в ответе может отличаться от запрошенного (XBTUSD -> XXBTZUSD), берём единственный
        data = next(iter(payload["result"].values()))
        return float(data["b"][0]), float(data["a"][0])


ALL_EXCHANGES = [Binance, Bybit, OKX, Kraken]


def build_exchanges(client: httpx.AsyncClient) -> list[Exchange]:
    return [cls(client) for cls in ALL_EXCHANGES]
