#!/usr/bin/env python3
"""
Binance Futures Data Fetcher using CCXT

Fetches futures market data including:
- Available trading pairs
- OHLCV candlestick data
- Orderbook depth
- Recent trades
- Funding rates
"""

import ccxt
import typer
from typing import Optional

app = typer.Typer(help="Binance futures data fetcher using CCXT")


def create_exchange() -> ccxt.binanceusdm:
    """Create and configure Binance USDM futures exchange instance."""
    exchange = ccxt.binanceusdm({
        "enableRateLimit": True,
        "options": {
            "defaultType": "future",
        },
    })
    return exchange


def fetch_futures_pairs_data(exchange: ccxt.binanceusdm) -> list[dict]:
    """Fetch all available futures trading pairs (internal function)."""
    markets = exchange.load_markets()
    futures_pairs = []

    for symbol, market in markets.items():
        if market.get("linear") and market.get("active"):
            futures_pairs.append({
                "symbol": symbol,
                "base": market["base"],
                "quote": market["quote"],
                "contract_size": market.get("contractSize", 1),
            })

    return futures_pairs


@app.command()
def pairs(
    limit: int = typer.Option(20, "--limit", "-l", help="Number of pairs to display"),
):
    """List available futures trading pairs."""
    print("\n=== Fetching Futures Trading Pairs ===")

    exchange = create_exchange()
    pairs_data = fetch_futures_pairs_data(exchange)

    print(f"Found {len(pairs_data)} active linear futures pairs\n")

    display_limit = min(limit, len(pairs_data))
    print(f"Displaying first {display_limit} pairs:\n")
    for pair in pairs_data[:display_limit]:
        print(f"  {pair['symbol']:<20} | Base: {pair['base']:<5} | Quote: {pair['quote']:<5}")


@app.command()
def ohlcv(
    symbol: str = typer.Argument("BTC/USDT:USDT", help="Futures symbol (e.g., BTC/USDT:USDT)"),
    timeframe: str = typer.Option("1h", "--timeframe", "-t", help="Candlestick timeframe: 1m, 5m, 15m, 1h, 4h, 1d"),
    limit: int = typer.Option(100, "--limit", "-l", help="Number of candles to fetch (max 1000)"),
):
    """Fetch OHLCV candlestick data."""
    print(f"\n=== Fetching OHLCV Data for {symbol} ===")
    print(f"Timeframe: {timeframe}, Limit: {limit}\n")

    exchange = create_exchange()
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)

    print(f"Fetched {len(ohlcv)} candles\n")

    print("Last 10 candles [timestamp, open, high, low, close, volume]:")
    for candle in ohlcv[-10:]:
        timestamp = exchange.iso8601(candle[0])
        print(f"  {timestamp}: O={candle[1]:.2f} H={candle[2]:.2f} "
              f"L={candle[3]:.2f} C={candle[4]:.2f} V={candle[5]:.2f}")


@app.command()
def orderbook(
    symbol: str = typer.Argument("BTC/USDT:USDT", help="Futures symbol (e.g., BTC/USDT:USDT)"),
    limit: int = typer.Option(20, "--limit", "-l", help="Orderbook depth levels (max 1000)"),
):
    """Fetch current orderbook depth."""
    print(f"\n=== Fetching Orderbook for {symbol} ===")

    exchange = create_exchange()
    orderbook = exchange.fetch_order_book(symbol, limit=limit)

    print(f"Orderbook timestamp: {exchange.iso8601(orderbook['timestamp'])}")
    print(f"Bids: {len(orderbook['bids'])}, Asks: {len(orderbook['asks'])}\n")

    display_limit = min(10, len(orderbook["bids"]), len(orderbook["asks"]))

    print(f"Top {display_limit} Bids (price, amount):")
    for bid in orderbook["bids"][:display_limit]:
        print(f"  {bid[0]:.2f} | {bid[1]:.4f}")

    print(f"\nTop {display_limit} Asks (price, amount):")
    for ask in orderbook["asks"][:display_limit]:
        print(f"  {ask[0]:.2f} | {ask[1]:.4f}")

    # Calculate spread
    if orderbook["bids"] and orderbook["asks"]:
        best_bid = orderbook["bids"][0][0]
        best_ask = orderbook["asks"][0][0]
        spread = best_ask - best_bid
        spread_pct = (spread / best_ask) * 100
        print(f"\nSpread: {spread:.2f} ({spread_pct:.4f}%)")


@app.command()
def trades(
    symbol: str = typer.Argument("BTC/USDT:USDT", help="Futures symbol (e.g., BTC/USDT:USDT)"),
    limit: int = typer.Option(50, "--limit", "-l", help="Number of trades to fetch (max 1000)"),
):
    """Fetch recent trades."""
    print(f"\n=== Fetching Recent Trades for {symbol} ===")

    exchange = create_exchange()
    trades_data = exchange.fetch_trades(symbol, limit=limit)

    print(f"Fetched {len(trades_data)} trades\n")

    print("Last 10 trades:")
    for trade in trades_data[-10:]:
        side = "BUY " if trade["side"] == "buy" else "SELL"
        timestamp = exchange.iso8601(trade["timestamp"])
        print(f"  {timestamp} | {side} | {trade['price']:.2f} | {trade['amount']:.4f}")

    # Calculate buy/sell ratio
    buy_volume = sum(t["amount"] for t in trades_data if t["side"] == "buy")
    sell_volume = sum(t["amount"] for t in trades_data if t["side"] == "sell")
    total_volume = buy_volume + sell_volume

    if total_volume > 0:
        print(f"\nBuy/Sell ratio: {buy_volume/total_volume*100:.1f}% / "
              f"{sell_volume/total_volume*100:.1f}%")


@app.command()
def funding(
    symbol: str = typer.Argument("BTC/USDT:USDT", help="Futures symbol (e.g., BTC/USDT:USDT)"),
):
    """Fetch current funding rate."""
    print(f"\n=== Fetching Funding Rate for {symbol} ===")

    exchange = create_exchange()
    try:
        funding = exchange.fapiPublic_get_premiumindex(symbol=symbol.replace("/", ""))

        print(f"Symbol: {funding['symbol']}")
        print(f"Funding Rate: {funding['lastFundingRate']:.6f}")
        print(f"Mark Price: {funding['markPrice']:.2f}")
        print(f"Index Price: {funding['indexPrice']:.2f}")
        print(f"Next Funding Time: {funding['nextFundingTime']}")

    except Exception as e:
        print(f"Error fetching funding rate: {e}")


@app.command()
def main_legacy():
    """Legacy mode: Fetch all data at once (original script behavior)."""
    print("=" * 50)
    print("Binance Futures Data Fetcher (Legacy Mode)")
    print("=" * 50)

    try:
        exchange = create_exchange()
        pairs_data = fetch_futures_pairs_data(exchange)

        print(f"\n1. Found {len(pairs_data)} active linear futures pairs")
        for pair in pairs_data[:5]:
            print(f"   {pair['symbol']}")

        ohlcv = exchange.fetch_ohlcv("BTC/USDT:USDT", "1h", 100)
        print(f"\n2. Fetched {len(ohlcv)} OHLCV candles")

        orderbook = exchange.fetch_order_book("BTC/USDT:USDT", 20)
        print(f"3. Fetched orderbook: {len(orderbook['bids'])} bids, {len(orderbook['asks'])} asks")

        trades = exchange.fetch_trades("BTC/USDT:USDT", 50)
        print(f"4. Fetched {len(trades)} recent trades")

        print("\n" + "=" * 50)
        print("Data fetch complete!")
        print("=" * 50)

    except ccxt.NetworkError as e:
        print(f"\nNetwork error: {e}")
        raise
    except ccxt.ExchangeError as e:
        print(f"\nExchange error: {e}")
        raise
    except ccxt.BaseError as e:
        print(f"\nCCXT error: {e}")
        raise


if __name__ == "__main__":
    app()
