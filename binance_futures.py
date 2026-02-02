#!/usr/bin/env python3
"""
Binance Futures Data Fetcher using CCXT

Fetches futures market data including:
- Available trading pairs
- OHLCV candlestick data
- Orderbook depth
- Recent trades
"""

import ccxt
from typing import Optional


# Configuration
CONFIG = {
    "symbol": "BTC/USDT:USDT",  # Perpetual futures symbol format
    "timeframe": "1h",          # Candlestick timeframe: 1m, 5m, 15m, 1h, 4h, 1d
    "ohlcv_limit": 100,         # Number of candles to fetch
    "orderbook_limit": 20,      # Orderbook depth levels
    "trades_limit": 50,         # Number of recent trades
}


def create_exchange() -> ccxt.binanceusdm:
    """Create and configure Binance USDM futures exchange instance."""
    exchange = ccxt.binanceusdm({
        "enableRateLimit": True,
        "options": {
            "defaultType": "future",
        },
    })
    return exchange


def fetch_futures_pairs(exchange: ccxt.binanceusdm) -> list[dict]:
    """Fetch all available futures trading pairs."""
    print("\n=== Fetching Futures Trading Pairs ===")

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

    print(f"Found {len(futures_pairs)} active linear futures pairs")

    # Show first 10 as sample
    print("\nSample pairs:")
    for pair in futures_pairs[:10]:
        print(f"  {pair['symbol']}")

    return futures_pairs


def fetch_ohlcv(
    exchange: ccxt.binanceusdm,
    symbol: str,
    timeframe: str = "1h",
    limit: int = 100,
) -> list[list]:
    """Fetch OHLCV candlestick data."""
    print(f"\n=== Fetching OHLCV Data for {symbol} ===")
    print(f"Timeframe: {timeframe}, Limit: {limit}")

    ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)

    print(f"Fetched {len(ohlcv)} candles")

    # Display last 5 candles
    print("\nLast 5 candles [timestamp, open, high, low, close, volume]:")
    for candle in ohlcv[-5:]:
        timestamp = exchange.iso8601(candle[0])
        print(f"  {timestamp}: O={candle[1]:.2f} H={candle[2]:.2f} "
              f"L={candle[3]:.2f} C={candle[4]:.2f} V={candle[5]:.2f}")

    return ohlcv


def fetch_orderbook(
    exchange: ccxt.binanceusdm,
    symbol: str,
    limit: int = 20,
) -> dict:
    """Fetch current orderbook depth."""
    print(f"\n=== Fetching Orderbook for {symbol} ===")

    orderbook = exchange.fetch_order_book(symbol, limit=limit)

    print(f"Orderbook timestamp: {exchange.iso8601(orderbook['timestamp'])}")
    print(f"Bids: {len(orderbook['bids'])}, Asks: {len(orderbook['asks'])}")

    # Display top 5 levels
    print("\nTop 5 Bids (price, amount):")
    for bid in orderbook["bids"][:5]:
        print(f"  {bid[0]:.2f} | {bid[1]:.4f}")

    print("\nTop 5 Asks (price, amount):")
    for ask in orderbook["asks"][:5]:
        print(f"  {ask[0]:.2f} | {ask[1]:.4f}")

    # Calculate spread
    if orderbook["bids"] and orderbook["asks"]:
        best_bid = orderbook["bids"][0][0]
        best_ask = orderbook["asks"][0][0]
        spread = best_ask - best_bid
        spread_pct = (spread / best_ask) * 100
        print(f"\nSpread: {spread:.2f} ({spread_pct:.4f}%)")

    return orderbook


def fetch_recent_trades(
    exchange: ccxt.binanceusdm,
    symbol: str,
    limit: int = 50,
) -> list[dict]:
    """Fetch recent trades."""
    print(f"\n=== Fetching Recent Trades for {symbol} ===")

    trades = exchange.fetch_trades(symbol, limit=limit)

    print(f"Fetched {len(trades)} trades")

    # Display last 10 trades
    print("\nLast 10 trades:")
    for trade in trades[-10:]:
        side = "BUY " if trade["side"] == "buy" else "SELL"
        timestamp = exchange.iso8601(trade["timestamp"])
        print(f"  {timestamp} | {side} | {trade['price']:.2f} | {trade['amount']:.4f}")

    # Calculate buy/sell ratio
    buy_volume = sum(t["amount"] for t in trades if t["side"] == "buy")
    sell_volume = sum(t["amount"] for t in trades if t["side"] == "sell")
    total_volume = buy_volume + sell_volume

    if total_volume > 0:
        print(f"\nBuy/Sell ratio: {buy_volume/total_volume*100:.1f}% / "
              f"{sell_volume/total_volume*100:.1f}%")

    return trades


def main():
    """Main function to fetch all Binance futures data."""
    print("=" * 50)
    print("Binance Futures Data Fetcher")
    print("=" * 50)

    try:
        # Create exchange instance
        exchange = create_exchange()

        # 1. Fetch available futures pairs
        pairs = fetch_futures_pairs(exchange)

        # 2. Fetch OHLCV data
        ohlcv = fetch_ohlcv(
            exchange,
            CONFIG["symbol"],
            CONFIG["timeframe"],
            CONFIG["ohlcv_limit"],
        )

        # 3. Fetch orderbook
        orderbook = fetch_orderbook(
            exchange,
            CONFIG["symbol"],
            CONFIG["orderbook_limit"],
        )

        # 4. Fetch recent trades
        trades = fetch_recent_trades(
            exchange,
            CONFIG["symbol"],
            CONFIG["trades_limit"],
        )

        print("\n" + "=" * 50)
        print("Data fetch complete!")
        print("=" * 50)

        return {
            "pairs": pairs,
            "ohlcv": ohlcv,
            "orderbook": orderbook,
            "trades": trades,
        }

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
    main()
