"""Main entry point for StockLens trading analysis system."""

from src.pipeline.trading_pipeline import TradingAnalysisPipeline


def main():
    """
    Main entry point for the StockLens trading analysis system.

    Uses the OOP architecture with TradingAnalysisPipeline for clean,
    maintainable code with intelligent caching and multi-LLM support.
    """
    try:
        # Initialize and run the complete trading analysis pipeline
        pipeline = TradingAnalysisPipeline(
            db_path="data/stocklens.db",
            use_cache=True
        )
        pipeline.run_complete_pipeline()
        print("\n=== Pipeline execution completed successfully ===")

    except Exception as e:
        print("\n=== Pipeline execution failed ===")
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
