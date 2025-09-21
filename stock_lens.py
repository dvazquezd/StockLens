"""Main entry point for StockLens trading analysis system."""

from src.pipeline.trading_pipeline import TradingAnalysisPipeline


def main() -> None:
    """Main entry point for the StockLens trading analysis system."""
    try:
        pipeline = TradingAnalysisPipeline()
        pipeline.run_complete_pipeline()
        print("\n=== Pipeline execution completed successfully ===")
        
    except Exception as e:
        print("\n=== Pipeline execution failed ===")
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()