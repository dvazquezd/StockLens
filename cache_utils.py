"""Utility script for managing StockLens cache and database."""

import argparse
from pathlib import Path
from src.database.data_cache import DataCache
from src.database.market_db import MarketDatabase


def show_cache_stats(db_path: str = "data/stocklens.db"):
    """Display cache statistics."""
    print("\n" + "=" * 60)
    print("📊 STOCKLENS CACHE STATISTICS")
    print("=" * 60)

    with DataCache(db_path) as cache:
        stats = cache.get_cache_stats()

        print(f"\n💾 Database: {stats['database_path']}")
        print(f"   Size: {stats['database_size_mb']:.2f} MB")
        print(f"\n📈 Market Data:")
        print(f"   Total rows: {stats['total_rows']:,}")
        print(f"   Unique symbols: {stats['unique_symbols']}")
        print(f"   Oldest data: {stats['oldest_data']}")
        print(f"   Newest data: {stats['newest_data']}")


def show_agent_runs(db_path: str = "data/stocklens.db", limit: int = 10):
    """Display recent agent runs."""
    print("\n" + "=" * 60)
    print(f"🤖 RECENT AGENT RUNS (Last {limit})")
    print("=" * 60)

    with MarketDatabase(db_path) as db:
        runs = db.get_agent_runs_summary(limit=limit)

        if runs.empty:
            print("\nNo agent runs found.")
            return

        print(f"\n{runs.to_string(index=False)}")


def show_recommendations(db_path: str = "data/stocklens.db", symbol: str = None, limit: int = 20):
    """Display recent recommendations."""
    print("\n" + "=" * 60)
    print(f"💡 RECENT RECOMMENDATIONS")
    if symbol:
        print(f"   Symbol: {symbol}")
    print(f"   Limit: {limit}")
    print("=" * 60)

    with MarketDatabase(db_path) as db:
        recs = db.get_recommendation_history(symbol=symbol, limit=limit)

        if recs.empty:
            print("\nNo recommendations found.")
            return

        # Display formatted recommendations
        for _, row in recs.iterrows():
            print(f"\n{'─' * 60}")
            print(f"📅 {row['created_at']}")
            print(f"🏷️  {row['symbol']}")
            print(f"🎯 Recommendation: {row['recommendation'].upper()}")
            if row['price_at_recommendation']:
                print(f"💰 Price: ${row['price_at_recommendation']:.2f}")
            print(f"🤖 Agent: {row['agent_type']}", end="")
            if row['llm_provider']:
                print(f" ({row['llm_provider']}/{row['llm_model']})")
            else:
                print()
            print(f"📝 Rationale: {row['rationale']}")


def show_symbol_data(symbol: str, source: str, interval: str, db_path: str = "data/stocklens.db", limit: int = 10):
    """Display recent data for a specific symbol."""
    print("\n" + "=" * 60)
    print(f"📊 DATA FOR {symbol} ({source}, {interval})")
    print("=" * 60)

    with MarketDatabase(db_path) as db:
        df = db.get_market_data(
            symbol=symbol,
            source=source,
            interval=interval,
            limit=limit
        )

        if df.empty:
            print(f"\nNo data found for {symbol}")
            return

        print(f"\n{df.to_string(index=False)}")
        print(f"\nTotal rows in database: {len(df)}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="StockLens Cache Management Utilities",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cache_utils.py stats
  python cache_utils.py runs --limit 20
  python cache_utils.py recs --symbol BTCUSDT --limit 10
  python cache_utils.py data --symbol BTCUSDT --source binance --interval 1d
        """
    )

    parser.add_argument(
        'command',
        choices=['stats', 'runs', 'recs', 'data'],
        help='Command to execute'
    )
    parser.add_argument('--db', default='data/stocklens.db', help='Database path')
    parser.add_argument('--symbol', help='Symbol filter')
    parser.add_argument('--source', help='Data source (binance/yahoo)')
    parser.add_argument('--interval', help='Time interval')
    parser.add_argument('--limit', type=int, default=10, help='Number of results')

    args = parser.parse_args()

    if args.command == 'stats':
        show_cache_stats(args.db)

    elif args.command == 'runs':
        show_agent_runs(args.db, args.limit)

    elif args.command == 'recs':
        show_recommendations(args.db, args.symbol, args.limit)

    elif args.command == 'data':
        if not args.symbol or not args.source or not args.interval:
            print("Error: --symbol, --source, and --interval are required for 'data' command")
            return
        show_symbol_data(args.symbol, args.source, args.interval, args.db, args.limit)

    print("\n" + "=" * 60 + "\n")


if __name__ == "__main__":
    main()
